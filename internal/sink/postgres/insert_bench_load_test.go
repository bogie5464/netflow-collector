//go:build load

package postgres

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

const (
	benchBatch   = 2000
	benchBatches = 20
	benchWarmup  = 2
)

// TestInsertPathBenchmark isolates the per-row cost of the write path from
// everything upstream of it: 2,000-row batches on one connection, no UDP, no
// pipeline. It compares the shipped unnest insert against COPY, and the
// hypertable against a plain table with the same columns and indexes, so the
// remaining levers in docs/performance.md are numbers rather than guesses.
func TestInsertPathBenchmark(t *testing.T) {
	ctx := context.Background()
	dsn := sinktest.StartTimescale(t)
	be, err := New(ctx, dsn, 30)
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	require.NoError(t, be.Migrate(ctx))
	b := be.(*backend)

	records := benchRecords(benchBatch)
	ids, err := b.resolveExporters(ctx, records)
	require.NoError(t, err)

	_, err = b.pool.Exec(ctx, `CREATE TABLE flow_records_plain (LIKE flow_records INCLUDING ALL)`)
	require.NoError(t, err)

	build := func() *columns {
		cols := newColumns(len(records))
		for i := range records {
			cols.add(&records[i], ids[records[i].ExporterAddr])
		}
		return cols
	}

	type method func(ctx context.Context, table string) error
	unnest := func(ctx context.Context, table string) error {
		cols := build()
		sql := strings.Replace(insertSQL, "INSERT INTO flow_records (", "INSERT INTO "+table+" (", 1)
		_, err := b.pool.Exec(ctx, sql, cols.args()...)
		return err
	}
	copyDirect := func(ctx context.Context, table string) error {
		_, err := b.pool.CopyFrom(ctx, pgx.Identifier{table}, copyColumns, build().copySource())
		return err
	}
	// The only COPY shape that keeps ON CONFLICT DO NOTHING, and therefore
	// the dedup contract: stage without indexes, then insert-select.
	copyStaged := func(ctx context.Context, table string) error {
		tx, err := b.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		colList := strings.Join(copyColumns, ", ")
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`CREATE TEMP TABLE flow_stage ON COMMIT DROP AS SELECT %s FROM flow_records WITH NO DATA`, colList)); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"flow_stage"}, copyColumns, build().copySource()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (%s) SELECT %s FROM flow_stage ON CONFLICT DO NOTHING`, table, colList, colList)); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	run := func(name, table string, m method) {
		t.Helper()
		_, err := b.pool.Exec(ctx, "TRUNCATE "+table)
		require.NoError(t, err)
		for range benchWarmup {
			require.NoError(t, m(ctx, table))
		}
		start := time.Now()
		for range benchBatches {
			require.NoError(t, m(ctx, table))
		}
		elapsed := time.Since(start)
		var n int64
		require.NoError(t, b.pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n))
		require.EqualValues(t, (benchWarmup+benchBatches)*benchBatch, n)
		perBatch := elapsed / benchBatches
		t.Logf("%-52s %7.1f ms/batch  %8.0f rows/s  (%d rows, 1 connection)",
			name, float64(perBatch.Microseconds())/1000, float64(benchBatches*benchBatch)/elapsed.Seconds(), n)
	}

	run("hypertable, unnest (shipped fallback path)", "flow_records", unnest)
	run("plain table, unnest", "flow_records_plain", unnest)
	run("hypertable, COPY direct (shipped fast path)", "flow_records", copyDirect)
	run("plain table, COPY direct (no dedup)", "flow_records_plain", copyDirect)
	run("hypertable, COPY staged + ON CONFLICT", "flow_records", copyStaged)
	run("plain table, COPY staged + ON CONFLICT", "flow_records_plain", copyStaged)

	for _, idx := range []string{
		"idx_flow_records_exporter_received", "idx_flow_records_src_addr",
	} {
		_, err := b.pool.Exec(ctx, "DROP INDEX "+idx)
		require.NoError(t, err)
	}
	run("hypertable, unnest, 2 secondary indexes dropped", "flow_records", unnest)
	run("hypertable, COPY direct, 2 secondary indexes dropped", "flow_records", copyDirect)
}

func benchRecords(n int) []flow.FlowRecord {
	now := time.Now().UTC()
	exporter := netip.MustParseAddr("203.0.113.10")
	src, dst := netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("198.51.100.200")
	out := make([]flow.FlowRecord, n)
	for i := range out {
		out[i] = flow.FlowRecord{
			ReceivedAt:    now.Add(time.Duration(i) * time.Microsecond),
			ExporterAddr:  exporter,
			FlowType:      flow.FlowTypeNetFlow5,
			FirstSwitched: now.Add(-time.Second),
			LastSwitched:  now,
			SrcAddr:       src,
			DstAddr:       dst,
			SrcPort:       51514,
			DstPort:       443,
			Protocol:      6,
			TCPFlags:      0x12,
			Packets:       12,
			Bytes:         9000,
			SamplingRate:  1,
		}
	}
	return out
}
