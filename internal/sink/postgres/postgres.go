// Package postgres is the PostgreSQL/TimescaleDB backend: a flow.Backend over
// pgxpool. Everything engine-specific stays inside this package.
package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/migrations"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
	// cacheCap bounds the exporter id cache; when full it is cleared rather
	// than grown, which is cheap because a miss is one upsert.
	cacheCap = 4096
)

// ErrBadCursor is returned by Query when q.Cursor cannot be decoded.
var ErrBadCursor = errors.New("postgres: undecodable cursor")

type backend struct {
	pool          *pgxpool.Pool
	retentionDays int

	mu    sync.Mutex
	cache map[netip.Addr]int64
}

// New connects to dsn and returns the backend. Migrate must run before the
// first write.
func New(ctx context.Context, dsn string, retentionDays int) (flow.Backend, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &backend{pool: pool, retentionDays: retentionDays, cache: map[netip.Addr]int64{}}, nil
}

// Migrate applies the embedded goose migrations for this backend and then the
// Timescale retention policy. Every step is idempotent.
func (b *backend) Migrate(ctx context.Context) error {
	migrations.Lock.Lock()
	defer migrations.Lock.Unlock()

	goose.SetBaseFS(migrations.FS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("postgres: migrate: %w", err)
	}
	db := stdlib.OpenDBFromPool(b.pool)
	defer func() { _ = db.Close() }() // closes the wrapper only, not the pool
	if err := goose.UpContext(ctx, db, "postgres"); err != nil {
		return fmt.Errorf("postgres: migrate: %w", err)
	}
	_, err := b.pool.Exec(ctx,
		`SELECT add_retention_policy('flow_records', drop_after => make_interval(days => $1), if_not_exists => TRUE)`,
		b.retentionDays)
	if err != nil {
		return fmt.Errorf("postgres: retention policy: %w", err)
	}
	return nil
}

// Close releases the pool.
func (b *backend) Close() error {
	b.pool.Close()
	return nil
}

// WriteBatch resolves each record's exporter, then inserts every row in one
// statement with ON CONFLICT DO NOTHING. The nullable unique index on
// (received_at, dedup_key) does the dedup work; there is no branch on where a
// record came from.
func (b *backend) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ids, err := b.resolveExporters(ctx, tx, records)
	if err != nil {
		return err
	}
	cols := newColumns(len(records))
	for i := range records {
		cols.add(&records[i], ids[records[i].ExporterAddr])
	}
	_, err = tx.Exec(ctx, insertSQL, cols.args()...)
	if err != nil {
		return fmt.Errorf("postgres: insert %d records: %w", len(records), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

// resolveExporters maps every distinct ExporterAddr in the batch to its id,
// creating the row on first sight and advancing last_seen_at once per batch
// per exporter — never once per record.
func (b *backend) resolveExporters(ctx context.Context, tx pgx.Tx, records []flow.FlowRecord) (map[netip.Addr]int64, error) {
	seen := map[netip.Addr]time.Time{}
	for i := range records {
		r := &records[i]
		if t, ok := seen[r.ExporterAddr]; !ok || r.ReceivedAt.After(t) {
			seen[r.ExporterAddr] = r.ReceivedAt.UTC().Truncate(time.Microsecond)
		}
	}
	ids := make(map[netip.Addr]int64, len(seen))
	for addr, last := range seen {
		id, ok := b.cached(addr)
		if ok {
			if _, err := tx.Exec(ctx,
				`UPDATE exporters SET last_seen_at = $2 WHERE id = $1 AND last_seen_at < $2`, id, last); err != nil {
				return nil, fmt.Errorf("postgres: touch exporter %s: %w", addr, err)
			}
		} else {
			err := tx.QueryRow(ctx, `
				INSERT INTO exporters (ip_address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)
				ON CONFLICT (ip_address) DO UPDATE
				   SET last_seen_at = GREATEST(exporters.last_seen_at, EXCLUDED.last_seen_at)
				RETURNING id`, addr, last).Scan(&id)
			if err != nil {
				return nil, fmt.Errorf("postgres: upsert exporter %s: %w", addr, err)
			}
			b.remember(addr, id)
		}
		ids[addr] = id
	}
	return ids, nil
}

func (b *backend) cached(addr netip.Addr) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id, ok := b.cache[addr]
	return id, ok
}

func (b *backend) remember(addr netip.Addr, id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.cache) >= cacheCap {
		clear(b.cache)
	}
	b.cache[addr] = id
}

// ListExporters returns every known exporter, ordered by id. It implements
// flow.ExporterLister, an optional capability discovered by type assertion —
// flow.Backend itself never widens.
func (b *backend) ListExporters(ctx context.Context) ([]flow.Exporter, error) {
	rows, err := b.pool.Query(ctx,
		`SELECT id, ip_address, label, first_seen_at, last_seen_at FROM exporters ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list exporters: %w", err)
	}
	defer rows.Close()

	var out []flow.Exporter
	for rows.Next() {
		var (
			e   flow.Exporter
			ip  netip.Prefix
			lbl *string
		)
		if err := rows.Scan(&e.ID, &ip, &lbl, &e.FirstSeenAt, &e.LastSeenAt); err != nil {
			return nil, fmt.Errorf("postgres: scan exporter: %w", err)
		}
		e.IPAddress = ip.Addr()
		e.Label = lbl
		e.FirstSeenAt, e.LastSeenAt = e.FirstSeenAt.UTC(), e.LastSeenAt.UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list exporters rows: %w", err)
	}
	return out, nil
}

// Query answers one page ordered by (received_at DESC, seq DESC) using a
// keyset cursor over the same pair.
func (b *backend) Query(ctx context.Context, q flow.FlowQuery) (flow.FlowResultPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	var (
		where = []string{"f.received_at >= $1", "f.received_at < $2"}
		args  = []any{q.Start.UTC(), q.End.UTC()}
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if q.ExporterID != 0 {
		add("f.exporter_id = $%d", q.ExporterID)
	}
	if q.SrcAddr.IsValid() {
		add("f.src_addr = $%d", q.SrcAddr)
	}
	if q.DstAddr.IsValid() {
		add("f.dst_addr = $%d", q.DstAddr)
	}
	if q.Protocol != nil {
		add("f.protocol = $%d", int16(*q.Protocol))
	}
	if q.Cursor != "" {
		c, err := decodeCursor(q.Cursor)
		if err != nil {
			return flow.FlowResultPage{}, err
		}
		args = append(args, c.at(), c.Seq)
		where = append(where, fmt.Sprintf("(f.received_at, f.seq) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, limit+1)
	sql := selectSQL + " WHERE " + strings.Join(where, " AND ") +
		fmt.Sprintf(" ORDER BY f.received_at DESC, f.seq DESC LIMIT $%d", len(args))

	rows, err := b.pool.Query(ctx, sql, args...)
	if err != nil {
		return flow.FlowResultPage{}, fmt.Errorf("postgres: query: %w", err)
	}
	defer rows.Close()

	var (
		page    flow.FlowResultPage
		lastSeq int64
	)
	for rows.Next() {
		rec, seq, err := scanRecord(rows)
		if err != nil {
			return flow.FlowResultPage{}, fmt.Errorf("postgres: scan: %w", err)
		}
		if len(page.Records) == limit {
			page.HasMore = true
			break
		}
		page.Records = append(page.Records, rec)
		lastSeq = seq
	}
	if err := rows.Err(); err != nil {
		return flow.FlowResultPage{}, fmt.Errorf("postgres: rows: %w", err)
	}
	if page.HasMore {
		last := page.Records[len(page.Records)-1]
		page.NextCursor = encodeCursor(cursor{T: last.ReceivedAt.UnixNano(), Seq: lastSeq})
	}
	return page, nil
}

// cursor is the opaque page token: the keyset pair of the last row returned.
type cursor struct {
	T   int64 `json:"t"`
	Seq int64 `json:"s"`
}

func (c cursor) at() time.Time { return time.Unix(0, c.T).UTC() }

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
