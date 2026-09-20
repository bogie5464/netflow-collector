// Package clickhouse is the ClickHouse backend: a flow.Backend over the native
// protocol via clickhouse-go. Everything engine-specific stays inside this
// package. It passes the same conformance suite as the Postgres and MariaDB
// backends with no branch in the suite.
package clickhouse

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/netip"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pressly/goose/v3"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/migrations"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// ErrBadCursor is returned by Query when q.Cursor cannot be decoded.
var ErrBadCursor = errors.New("clickhouse: undecodable cursor")

type backend struct {
	conn          driver.Conn
	db            *sql.DB // goose speaks database/sql; nothing else uses this
	retentionDays int
}

// New connects to dsn (clickhouse://user:pass@host:9000/db) and returns the
// backend. Migrate must run before the first write.
func New(ctx context.Context, dsn string, retentionDays int) (flow.Backend, error) {
	// Any query parameter the driver does not recognise becomes a ClickHouse
	// session setting, so NFC_CLICKHOUSE_DSN can carry async_insert and
	// friends; the sink itself forces none. Each WriteBatch is one
	// synchronous INSERT and therefore one part, which is why
	// docs/runbook.md asks for a large NFC_BATCH_SIZE on this backend.
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return &backend{conn: conn, db: clickhouse.OpenDB(opts), retentionDays: retentionDays}, nil
}

// Migrate applies the embedded goose migrations and then the retention TTL.
// Every step is idempotent.
func (b *backend) Migrate(ctx context.Context) error {
	migrations.Lock.Lock()
	goose.SetBaseFS(migrations.FS)
	goose.SetLogger(goose.NopLogger())
	err := goose.SetDialect("clickhouse")
	if err == nil {
		err = goose.UpContext(ctx, b.db, "clickhouse")
	}
	migrations.Lock.Unlock()
	if err != nil {
		return fmt.Errorf("clickhouse: migrate: %w", err)
	}
	return b.applyRetention(ctx)
}

// applyRetention sets the table TTL from NFC_RETENTION_DAYS, and only when it
// differs: MODIFY TTL rewrites table metadata and schedules a mutation, which
// is not something to do on every restart.
func (b *backend) applyRetention(ctx context.Context) error {
	want := fmt.Sprintf("toIntervalDay(%d)", b.retentionDays)
	var n uint64
	err := b.conn.QueryRow(ctx, `
		SELECT count() FROM system.tables
		 WHERE database = currentDatabase() AND name = 'flow_records' AND engine_full LIKE ?`,
		"%TTL toDateTime(received_at) + "+want+"%").Scan(&n)
	if err != nil {
		return fmt.Errorf("clickhouse: inspect retention: %w", err)
	}
	if n == 1 {
		return nil
	}
	if err := b.conn.Exec(ctx,
		"ALTER TABLE flow_records MODIFY TTL toDateTime(received_at) + "+want); err != nil {
		return fmt.Errorf("clickhouse: set retention: %w", err)
	}
	return nil
}

// Close releases both connections.
func (b *backend) Close() error {
	return errors.Join(b.conn.Close(), b.db.Close())
}

// WriteBatch records every exporter the batch mentions, then inserts the
// batch in one native-protocol INSERT. Dedup is the table's business (see the
// migration header): the sink writes every record the same way and never
// branches on where it came from.
func (b *backend) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := b.touchExporters(ctx, records); err != nil {
		return err
	}
	batch, err := b.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare insert: %w", err)
	}
	for i := range records {
		if err := batch.Append(insertRow(&records[i])...); err != nil {
			return fmt.Errorf("clickhouse: append record: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: insert %d records: %w", len(records), err)
	}
	return nil
}

// touchExporters writes one row per distinct exporter in the batch — never
// one per record — carrying the batch's latest ReceivedAt. The
// AggregatingMergeTree keeps the earliest first_seen_at and latest
// last_seen_at across every row ever written, so this is the upsert.
func (b *backend) touchExporters(ctx context.Context, records []flow.FlowRecord) error {
	seen := map[netip.Addr]time.Time{}
	for i := range records {
		r := &records[i]
		if t, ok := seen[r.ExporterAddr]; !ok || r.ReceivedAt.After(t) {
			seen[r.ExporterAddr] = micro(r.ReceivedAt)
		}
	}
	batch, err := b.conn.PrepareBatch(ctx, `INSERT INTO exporters (id, ip_address, first_seen_at, last_seen_at)`)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare exporters: %w", err)
	}
	for addr, last := range seen {
		if err := batch.Append(exporterID(addr), addr, last, last); err != nil {
			return fmt.Errorf("clickhouse: append exporter %s: %w", addr, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: touch %d exporters: %w", len(seen), err)
	}
	return nil
}

// exporterID derives the exporters row id from the address itself: FNV-1a
// over the 16-byte form, masked to 63 bits so it round-trips through int64.
// Being a pure function of the address, it needs no lookup and no cache, and
// any number of collector instances writing to one ClickHouse agree on it
// without coordination.
func exporterID(a netip.Addr) uint64 {
	h := fnv.New64a()
	b := a.As16()
	_, _ = h.Write(b[:])
	id := h.Sum64() & (1<<63 - 1)
	if id == 0 {
		id = 1
	}
	return id
}

// ListExporters returns every known exporter, ordered by id. It implements
// flow.ExporterLister, an optional capability discovered by type assertion.
func (b *backend) ListExporters(ctx context.Context) ([]flow.Exporter, error) {
	rows, err := b.conn.Query(ctx,
		`SELECT id, ip_address, label, first_seen_at, last_seen_at FROM exporters FINAL ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: list exporters: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []flow.Exporter
	for rows.Next() {
		var (
			e  flow.Exporter
			id uint64
			ip netip.Addr
		)
		if err := rows.Scan(&id, &ip, &e.Label, &e.FirstSeenAt, &e.LastSeenAt); err != nil {
			return nil, fmt.Errorf("clickhouse: scan exporter: %w", err)
		}
		e.ID = int64(id)
		e.IPAddress = ip.Unmap()
		e.FirstSeenAt, e.LastSeenAt = e.FirstSeenAt.UTC(), e.LastSeenAt.UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: list exporters rows: %w", err)
	}
	return out, nil
}

// Query answers one page ordered by (received_at DESC, seq DESC) using a
// keyset cursor over the same pair. FINAL collapses a redelivered record
// that has not been merged away yet.
func (b *backend) Query(ctx context.Context, q flow.FlowQuery) (flow.FlowResultPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	// Timestamps travel as integer microseconds so the comparison is exact
	// at the column's precision rather than at whatever the driver formats.
	where := []string{"received_at >= fromUnixTimestamp64Micro(?)", "received_at < fromUnixTimestamp64Micro(?)"}
	args := []any{q.Start.UnixMicro(), q.End.UnixMicro()}
	if q.ExporterID != 0 {
		where, args = append(where, "exporter_id = ?"), append(args, uint64(q.ExporterID))
	}
	if q.SrcAddr.IsValid() {
		where, args = append(where, "src_addr = toIPv6(?)"), append(args, q.SrcAddr.String())
	}
	if q.DstAddr.IsValid() {
		where, args = append(where, "dst_addr = toIPv6(?)"), append(args, q.DstAddr.String())
	}
	if q.Protocol != nil {
		where, args = append(where, "protocol = ?"), append(args, *q.Protocol)
	}
	if q.Cursor != "" {
		c, err := decodeCursor(q.Cursor)
		if err != nil {
			return flow.FlowResultPage{}, err
		}
		where, args = append(where, "(received_at, seq) < (fromUnixTimestamp64Micro(?), ?)"), append(args, c.T, c.Seq)
	}
	args = append(args, limit+1)
	sqlText := selectSQL + " WHERE " + strings.Join(where, " AND ") + " ORDER BY received_at DESC, seq DESC LIMIT ?"

	rows, err := b.conn.Query(ctx, sqlText, args...)
	if err != nil {
		return flow.FlowResultPage{}, fmt.Errorf("clickhouse: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		page    flow.FlowResultPage
		lastSeq uint64
	)
	for rows.Next() {
		rec, seq, err := scanRecord(rows)
		if err != nil {
			return flow.FlowResultPage{}, fmt.Errorf("clickhouse: scan: %w", err)
		}
		if len(page.Records) == limit {
			page.HasMore = true
			break
		}
		page.Records = append(page.Records, rec)
		lastSeq = seq
	}
	if err := rows.Err(); err != nil {
		return flow.FlowResultPage{}, fmt.Errorf("clickhouse: rows: %w", err)
	}
	if page.HasMore {
		last := page.Records[len(page.Records)-1]
		page.NextCursor = encodeCursor(cursor{T: last.ReceivedAt.UnixMicro(), Seq: lastSeq})
	}
	return page, nil
}

// cursor is the opaque page token: the keyset pair of the last row returned,
// with received_at as integer microseconds.
type cursor struct {
	T   int64  `json:"t"`
	Seq uint64 `json:"s"`
}

func encodeCursor(c cursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(s string) (cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, fmt.Errorf("%w: %w", ErrBadCursor, err)
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return cursor{}, fmt.Errorf("%w: %w", ErrBadCursor, err)
	}
	return c, nil
}
