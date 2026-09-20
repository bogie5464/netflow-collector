// Package mariadb is the MariaDB backend: a flow.Backend over database/sql
// with go-sql-driver/mysql. Everything engine-specific stays inside this
// package. It passes the same conformance suite as the Postgres backend with
// no branch in the suite, which is the whole point of having two.
package mariadb

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/migrations"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
	cacheCap     = 4096
	// insertRows bounds one multi-row INSERT so its placeholder count stays
	// well under the server's 65535 limit (20 columns per row).
	insertRows = 1000
	// Retention is a chunked DELETE on a ticker because v1 does not
	// partition flow_records (see the migration header).
	retentionInterval = time.Hour
	retentionChunk    = 10000
)

// ErrBadCursor is returned by Query when q.Cursor cannot be decoded.
var ErrBadCursor = errors.New("mariadb: undecodable cursor")

type backend struct {
	db            *sql.DB
	retentionDays int
	log           *slog.Logger

	mu    sync.Mutex
	cache map[netip.Addr]int64

	retentionStarted bool
	stopRetention    chan struct{}
	retentionDone    chan struct{}
}

// New opens dsn and returns the backend. The DSN must carry
// parseTime=true&loc=UTC: without them the driver returns strings or
// reinterprets every DATETIME(6) in the process's local zone.
func New(ctx context.Context, dsn string, retentionDays int) (flow.Backend, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mariadb: parse DSN: %w", err)
	}
	if !cfg.ParseTime || cfg.Loc == nil || cfg.Loc.String() != "UTC" {
		return nil, errors.New("mariadb: NFC_MARIADB_DSN must include parseTime=true&loc=UTC")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mariadb: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mariadb: ping: %w", err)
	}
	return &backend{
		db:            db,
		retentionDays: retentionDays,
		log:           slog.Default().With("backend", "mariadb"),
		cache:         map[netip.Addr]int64{},
		stopRetention: make(chan struct{}),
		retentionDone: make(chan struct{}),
	}, nil
}

// Migrate applies the embedded goose migrations and starts the retention
// ticker. Every step is idempotent.
func (b *backend) Migrate(ctx context.Context) error {
	migrations.Lock.Lock()
	goose.SetBaseFS(migrations.FS)
	goose.SetLogger(goose.NopLogger())
	err := goose.SetDialect("mysql")
	if err == nil {
		err = goose.UpContext(ctx, b.db, "mariadb")
	}
	migrations.Lock.Unlock()
	if err != nil {
		return fmt.Errorf("mariadb: migrate: %w", err)
	}
	b.mu.Lock()
	if !b.retentionStarted {
		b.retentionStarted = true
		go b.retentionLoop()
	}
	b.mu.Unlock()
	return nil
}

// Close stops retention and releases the pool.
func (b *backend) Close() error {
	b.mu.Lock()
	started := b.retentionStarted
	b.retentionStarted = false
	b.mu.Unlock()
	if started {
		close(b.stopRetention)
		<-b.retentionDone
	}
	return b.db.Close()
}

func (b *backend) retentionLoop() {
	defer close(b.retentionDone)
	t := time.NewTicker(retentionInterval)
	defer t.Stop()
	for {
		b.applyRetention()
		select {
		case <-t.C:
		case <-b.stopRetention:
			return
		}
	}
}

// applyRetention deletes expired rows in bounded chunks so the hottest table
// is never locked for long.
func (b *backend) applyRetention() {
	cutoff := time.Now().UTC().Add(-time.Duration(b.retentionDays) * 24 * time.Hour)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		res, err := b.db.ExecContext(ctx, `DELETE FROM flow_records WHERE received_at < ? ORDER BY received_at LIMIT ?`, cutoff, retentionChunk)
		cancel()
		if err != nil {
			b.log.Error("retention delete failed", "err", err)
			return
		}
		if n, _ := res.RowsAffected(); n < retentionChunk {
			return
		}
	}
}

// WriteBatch resolves each record's exporter, then inserts the rows with
// INSERT IGNORE. The nullable unique key on (received_at, dedup_key) does
// the dedup work; there is no branch on where a record came from.
func (b *backend) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mariadb: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ids, err := b.resolveExporters(ctx, tx, records)
	if err != nil {
		return err
	}
	for start := 0; start < len(records); start += insertRows {
		end := min(start+insertRows, len(records))
		sqlText, args := insertStatement(records[start:end], ids)
		if _, err := tx.ExecContext(ctx, sqlText, args...); err != nil {
			return fmt.Errorf("mariadb: insert %d records: %w", end-start, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mariadb: commit: %w", err)
	}
	return nil
}

func (b *backend) resolveExporters(ctx context.Context, tx *sql.Tx, records []flow.FlowRecord) (map[netip.Addr]int64, error) {
	seen := map[netip.Addr]time.Time{}
	for i := range records {
		r := &records[i]
		if t, ok := seen[r.ExporterAddr]; !ok || r.ReceivedAt.After(t) {
			seen[r.ExporterAddr] = micro(r.ReceivedAt)
		}
	}
	ids := make(map[netip.Addr]int64, len(seen))
	for addr, last := range seen {
		id, ok := b.cached(addr)
		if ok {
			if _, err := tx.ExecContext(ctx,
				`UPDATE exporters SET last_seen_at = ? WHERE id = ? AND last_seen_at < ?`, last, id, last); err != nil {
				return nil, fmt.Errorf("mariadb: touch exporter %s: %w", addr, err)
			}
		} else {
			// LAST_INSERT_ID(id) makes the update path report the existing id.
			res, err := tx.ExecContext(ctx, `
				INSERT INTO exporters (ip_address, first_seen_at, last_seen_at) VALUES (?, ?, ?)
				ON DUPLICATE KEY UPDATE
				    last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at)),
				    id = LAST_INSERT_ID(id)`, addr.AsSlice(), last, last)
			if err != nil {
				return nil, fmt.Errorf("mariadb: upsert exporter %s: %w", addr, err)
			}
			if id, err = res.LastInsertId(); err != nil {
				return nil, fmt.Errorf("mariadb: exporter id %s: %w", addr, err)
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
	rows, err := b.db.QueryContext(ctx,
		`SELECT id, ip_address, label, first_seen_at, last_seen_at FROM exporters ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("mariadb: list exporters: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []flow.Exporter
	for rows.Next() {
		var (
			e  flow.Exporter
			ip []byte
		)
		if err := rows.Scan(&e.ID, &ip, &e.Label, &e.FirstSeenAt, &e.LastSeenAt); err != nil {
			return nil, fmt.Errorf("mariadb: scan exporter: %w", err)
		}
		e.IPAddress, _ = netip.AddrFromSlice(ip)
		e.FirstSeenAt, e.LastSeenAt = e.FirstSeenAt.UTC(), e.LastSeenAt.UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mariadb: list exporters rows: %w", err)
	}
	return out, nil
}

// Query answers one page ordered by (received_at DESC, seq DESC) using a
// keyset cursor over the same pair, via a row constructor comparison.
func (b *backend) Query(ctx context.Context, q flow.FlowQuery) (flow.FlowResultPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	where := []string{"f.received_at >= ?", "f.received_at < ?"}
	args := []any{q.Start.UTC(), q.End.UTC()}
	if q.ExporterID != 0 {
		where, args = append(where, "f.exporter_id = ?"), append(args, q.ExporterID)
	}
	if q.SrcAddr.IsValid() {
		where, args = append(where, "f.src_addr = ?"), append(args, q.SrcAddr.AsSlice())
	}
	if q.DstAddr.IsValid() {
		where, args = append(where, "f.dst_addr = ?"), append(args, q.DstAddr.AsSlice())
	}
	if q.Protocol != nil {
		where, args = append(where, "f.protocol = ?"), append(args, *q.Protocol)
	}
	if q.Cursor != "" {
		c, err := decodeCursor(q.Cursor)
		if err != nil {
			return flow.FlowResultPage{}, err
		}
		where, args = append(where, "(f.received_at, f.seq) < (?, ?)"), append(args, c.at(), c.Seq)
	}
	args = append(args, limit+1)
	sqlText := selectSQL + " WHERE " + strings.Join(where, " AND ") + " ORDER BY f.received_at DESC, f.seq DESC LIMIT ?"

	rows, err := b.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return flow.FlowResultPage{}, fmt.Errorf("mariadb: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		page    flow.FlowResultPage
		lastSeq int64
	)
	for rows.Next() {
		rec, seq, err := scanRecord(rows)
		if err != nil {
			return flow.FlowResultPage{}, fmt.Errorf("mariadb: scan: %w", err)
		}
		if len(page.Records) == limit {
			page.HasMore = true
			break
		}
		page.Records = append(page.Records, rec)
		lastSeq = seq
	}
	if err := rows.Err(); err != nil {
		return flow.FlowResultPage{}, fmt.Errorf("mariadb: rows: %w", err)
	}
	if page.HasMore {
		last := page.Records[len(page.Records)-1]
		page.NextCursor = encodeCursor(cursor{T: last.ReceivedAt.UnixNano(), Seq: lastSeq})
	}
	return page, nil
}

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
