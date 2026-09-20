# Extending the collector

The architecture exists so that these two procedures are short. Both are gated by the same
commands as everything else: `go vet ./... && go tool golangci-lint run ./... && go test -race ./...`.

## Adding a Source

A source turns some transport — another protocol, a message bus, a file, a historical store being
replayed — into `flow.FlowRecord` values. The contract is one method:

```go
type Source interface {
    Start(ctx context.Context, out chan<- FlowRecord) error
}
```

It deliberately names no network type. Your adapter owns its wire format, its connection and its
goroutines; the pipeline owns backpressure.

1. **Create the package** `internal/source/<name>/<name>.go` and export
   `New(cfg config.Config) (flow.Source, error)`. `Start` blocks until `ctx` is cancelled, releases
   its resources and returns `nil`; a non-nil return means the source failed and the process exits.
2. **Never block on `out`.** Send with a `select` on `ctx.Done()` so cancellation is always
   honoured. Do not batch, retry, fan out or write — a source produces records and nothing else.
3. **Set the two fields that decide semantics.**
   - `ReceivedAt`: for a live wire protocol, the moment you accepted the datagram, truncated to
     microseconds. For a replay or a bus with redelivery, the timestamp carried **in the record** —
     never your own clock, or a redelivery becomes a different row.
   - `DedupKey`: `nil` for an append-only source with no redelivery. A 16-byte digest of
     `(exporter_addr, first_switched, src_addr, dst_addr, src_port, dst_port, protocol)` when the
     transport can deliver the same record twice (`kafka.DedupKey` is the reference implementation).
4. **Count your own decode failures** with
   `netflow_packets_decode_errors_total{source="<name>"}` (`netflow.DecodeErrors`). A malformed
   input increments it, is skipped, and never stops the loop.
5. **Test without the network where you can.** Construct the input programmatically — the NetFlow
   tests build their own v5 and v9 datagrams from the wire layout; a pasted hex capture is
   unreadable and usually wrong. If a broker is genuinely needed, pin its image in
   `docker-compose.yml` and start it with `docker compose up -d --wait <service>`.
6. **Wire it up.** Add the name to `config.Config`'s `NFC_SOURCES` `oneof` list and any new
   `NFC_*` variables — required **only** when your source is enabled — to `.env.example` and
   `internal/config/config.go`; add the constructor to `newSource` in `internal/app/run.go`.
7. **Document it** with an entry below.

Verify:

```bash
go build ./...
go test -race ./internal/source/<name>/...
test -f internal/flow/ports.go && ! grep -qE '\bnet\.(Conn|Addr|PacketConn)\b' internal/flow/ports.go
go tool golangci-lint run ./...
```

### Sources shipped

| Name | Package | Transport | `ReceivedAt` | `DedupKey` |
|---|---|---|---|---|
| `netflow` | `internal/source/netflow` | UDP, NetFlow v5/v9/IPFIX via goflow2 | collector clock | nil (append-only) |
| `kafka` | `internal/source/kafka` | franz-go consumer group, JSON per message | message `received_at`, else record timestamp | digest of the natural key |

## Adding a Sink

A backend implements `flow.Backend`: `WriteBatch(ctx, []FlowRecord) error`,
`Query(ctx, FlowQuery) (FlowResultPage, error)`, `Migrate(ctx) error`, `Close() error`. Nothing
else. If you want to add a method to the interface, stop — the answer is almost always a private
helper on your struct, or an optional capability discovered by type assertion (see
`api.ExporterLister`).

Five artefacts, all required:

| # | Artefact | Where |
|---|---|---|
| 1 | **Implementation** — `New(ctx, dsn, retentionDays) (flow.Backend, error)`; every engine-specific type stays inside the package | `internal/sink/<engine>/<engine>.go` |
| 2 | **Conformance test** — calls `sinktest.Conformance(t, factory)` **unmodified**; the factory starts a container pinned to the compose tag and resets tables per call | `internal/sink/<engine>/<engine>_test.go` |
| 3 | **Migration directory** — hand-authored `00001_flow_schema.sql` with `-- +goose Up` / `-- +goose Down`, added to the `//go:embed` line | `migrations/<engine>/`, `migrations/embed.go` |
| 4 | **docker-compose service** — pinned image tag, named volume, healthcheck, a `127.0.0.1` host port that does not collide with 15432 / 13306 / 19092 | `docker-compose.yml` |
| 5 | **Docs entry** — a row in this file's table below and in `README.md`'s backend table; a runbook section if retention differs from Timescale's chunk drop | `docs/extending.md`, `README.md`, `docs/runbook.md` |

Rules that decide whether it works:

- **The schema must give `UNIQUE (received_at, dedup_key)` with NULLs distinct.** That is what
  dedups Kafka redelivery while leaving the UDP path append-only. If your engine treats NULLs as
  equal in a unique index you need another mechanism, and you must say so here.
- **Write conflict-tolerantly** (`ON CONFLICT DO NOTHING`, `INSERT IGNORE`, or the engine's
  equivalent) and never branch on where a record came from.
- **Truncate every timestamp to microseconds on write.** Go carries nanoseconds; the conformance
  suite compares at microsecond precision and fails a correct implementation that forgets.
- **Keyset pagination**: `(received_at, seq) < (t, s)` with `ORDER BY received_at DESC, seq DESC`,
  an opaque base64url cursor of `{"t","s"}`, and an `ErrBadCursor` for an undecodable one.
- **Resolve exporters through a bounded cache** and advance `last_seen_at` at most once per batch
  per exporter — never per record.
- **Do not edit `internal/sink/sinktest/conformance.go`.** If the suite needs a branch for your
  engine, the interface leaked; the build gate diffs that file against its committed state.

Then wire it: add the name to `config.Config`'s `NFC_SINKS` `oneof` list, the DSN variable
(required **only** when the engine is in `NFC_SINKS`) to `.env.example` and
`internal/config/config.go`, and the constructor to `newBackend` in `internal/app/run.go`.

Verify:

```bash
go build ./...
go test -race ./internal/sink/<engine>/...
git diff --quiet HEAD -- internal/sink/sinktest/conformance.go
go test -race ./internal/sink/...
go tool golangci-lint run ./...
```

### Sinks shipped

| Name | Package | Engine | Migration dir | Compose service | Retention |
|---|---|---|---|---|---|
| `postgres` | `internal/sink/postgres` | PostgreSQL 17 + TimescaleDB | `migrations/postgres/` | `timescaledb` (`127.0.0.1:15432`) | `add_retention_policy` on the hypertable |
| `mariadb` | `internal/sink/mariadb` | MariaDB 11.8 | `migrations/mariadb/` | `mariadb` (`127.0.0.1:13306`) | chunked `DELETE` on an hourly ticker |
