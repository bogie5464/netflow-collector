---
name: add-storage-backend
description: Use when adding a new storage backend to NetFlow Collector — ClickHouse, SQLite, DuckDB, S3/Parquet, or any other engine that should implement flow.Sink and flow.Querier. Triggers on "add a backend", "support <engine>", "write flows to <engine>", "new sink". Covers the five artefacts a backend needs and the conformance suite it must pass unmodified.
---

# Add a storage backend

## When to use

Someone asks NetFlow Collector to write flow records into an engine it does not support yet, or to
read them back from one. This is the single most reused workflow in this project — the whole
architecture exists so that this procedure is short.

## Steps

1. **Read the contract first.** `internal/flow/ports.go`. A storage backend implements
   `flow.Backend`: `WriteBatch(ctx, []FlowRecord) error`, `Query(ctx, FlowQuery) (FlowResultPage,
   error)`, `Migrate(ctx) error`, `Close() error`. Nothing else. If you find yourself wanting to
   add a method to the interface, stop — the answer is almost always a private helper on your
   struct, or an optional capability discovered by type assertion (`flow.ExporterLister`,
   `flow.Pinger`).

   **Decide whether it is storage at all.** A sink nothing can be queried back from — a message
   bus, object storage, a webhook — implements only `flow.Sink`, `flow.Pinger` (so `/readyz` can
   see it) and `io.Closer`, skips steps 3–5, and gets its own tests instead of the conformance
   suite. `internal/sink/kafka` is the shipped example; the app sorts `NFC_SINKS` entries by
   asserting `flow.Backend`, so nothing else changes.

2. **Create the package.** `internal/sink/<engine>/<engine>.go`, `package <engine>`. Export
   `New(ctx context.Context, dsn string, retentionDays int) (flow.Backend, error)`. Keep every
   engine-specific type inside this package; nothing outside it may import your driver.

3. **Write the schema.** `migrations/<engine>/00001_flow_schema.sql`, hand-authored with goose's
   `-- +goose Up` / `-- +goose Down` annotations. It must reproduce the two entities in
   `migrations/postgres/00001_flow_schema.sql` (`exporters`, `flow_records`) and, critically, a
   **unique constraint on `(received_at, dedup_key)` where NULL dedup keys are distinct** — that is
   what dedups Kafka redelivery while leaving the UDP path append-only. If your engine treats NULLs
   as equal in a unique index, or has no unique index at all, you need a different mechanism and
   you must say so in `docs/extending.md`; do not silently change the semantics.
   `migrations/clickhouse/00001_flow_schema.sql` is the worked example: the uniqueness is built
   into a `ReplacingMergeTree` sorting key, with the branch in the DDL and none in the sink.

4. **Embed the migrations.** Add your directory to the `//go:embed` line in `migrations/embed.go`.
   The directive cannot reference a parent directory, which is why that file lives at the root of
   `migrations/` — do not move it.

5. **Run the conformance suite, unmodified.** Create `internal/sink/<engine>/<engine>_test.go`:

   ```go
   func TestNewEngineConformance(t *testing.T) {
       sinktest.Conformance(t, func(t *testing.T) (flow.Sink, flow.Querier) {
           // start a container pinned to the tag in docker-compose.yml, call New, t.Cleanup(Close)
       })
   }
   ```

   **Do not edit `internal/sink/sinktest/conformance.go` to make your engine pass.** If it needs
   a branch for your engine, the interface has leaked and the fix is in your backend or in the
   interface — raise it, do not patch the suite. Adding an assertion that states a contract
   property every backend must hold is different, and welcome.

   If a test outside your package needs the container (the end-to-end throughput test in
   `internal/app` does), put `Start<Engine>(t)` and the image constant in a new file under
   `internal/sink/sinktest/` — see `sinktest/clickhouse.go` — never in `conformance.go`.

6. **Add a compose service.** `docker-compose.yml` with a **pinned image tag**, a named volume, a
   healthcheck, and a host port on `127.0.0.1` that does not collide with 15432 / 13306 / 19092.
   The compose file owns the tag; copy that literal into your test file, never the reverse.

7. **Wire it up — every place that names a backend.**
   - `internal/config/config.go`: the `Sink<Engine>` constant, the `NFC_SINKS` `oneof` list, the
     `NFC_<ENGINE>_DSN` field (`required_if=<Engine>Enabled true`), the `<Engine>Enabled` derived
     field and its `slices.Contains` line in `parse`. `internal/config/config_test.go` asserts the
     exact `oneof` message — update it.
   - `internal/app/run.go`: **both** switches — `CheckNames` (rejects unknown names before any
     database is touched) and `newBackend` (constructs).
   - `.env.example`: the DSN with a working local value, and the comment listing the sinks.
   - `docker-compose.yml`: the `collector` service's `NFC_<ENGINE>_DSN` and `depends_on`, on top
     of the service from step 6.
   - `CLAUDE.md`: the stack line and the environment table.
   - If the driver is a new dependency, build the image and check the binary against the size
     gate in `.github/workflows/ci.yml` before committing.

8. **Document it.** The engine's row in `docs/extending.md`'s *Sinks shipped* table and in
   `README.md`'s backend table, plus the README's `NFC_SINKS` row, DSN row and quick-start port
   comment. If it has a retention story different from Timescale's partition-drop, put it in
   `docs/runbook.md`. Measure it: add a `TestEndToEndUDPThroughput<Engine>` case to
   `internal/app/loadtest_e2e_test.go` and record the result in `docs/performance.md`.

## Verify

```bash
go build ./...                                        # expect: exit 0
go test -race ./internal/sink/<engine>/...            # expect: exit 0, ok, every conformance assertion passes
git diff --quiet HEAD -- internal/sink/sinktest/conformance.go   # expect: exit 0 unless you added a contract assertion
go test -race ./internal/sink/...                     # expect: exit 0 — every backend still passes
go tool golangci-lint run ./...                       # expect: exit 0
```

## Do not

- **Do not add a method to `flow.Sink` or `flow.Querier`.** Two backends exist so that this pressure
  is visible; a third that needs a wider interface means the second one was carrying a Postgres
  assumption all along.
- **Do not expose SQL, a query builder, or a `*sql.DB` through the interface.** `WriteBatch` takes a
  slice and `Query` takes a `FlowQuery`; anything else makes the boundary decorative.
- **Do not add an engine branch to the conformance suite** to make your backend pass.
- **Do not forget microsecond truncation.** Go timestamps carry nanoseconds and most engines do not.
  Truncate on write or the round-trip assertion fails on a correct implementation.
- **Do not use an unpinned image tag** in `docker-compose.yml`. `:latest` turns a green suite into a
  time bomb.
