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

1. **Read the contract first.** `internal/flow/ports.go`. A backend implements `flow.Backend`:
   `WriteBatch(ctx, []FlowRecord) error`, `Query(ctx, FlowQuery) (FlowResultPage, error)`,
   `Migrate(ctx) error`, `Close() error`. Nothing else. If you find yourself wanting to add a method
   to the interface, stop — the answer is almost always a private helper on your struct.

2. **Create the package.** `internal/sink/<engine>/<engine>.go`, `package <engine>`. Export
   `New(ctx context.Context, dsn string, retentionDays int) (flow.Backend, error)`. Keep every
   engine-specific type inside this package; nothing outside it may import your driver.

3. **Write the schema.** `migrations/<engine>/00001_flow_schema.sql`, hand-authored with goose's
   `-- +goose Up` / `-- +goose Down` annotations. It must reproduce the entities in `blueprint.md`
   §4 and, critically, a **unique constraint on `(received_at, dedup_key)` where NULL dedup keys are
   distinct** — that is what dedups Kafka redelivery while leaving the UDP path append-only. If your
   engine treats NULLs as equal in a unique index, you need a different mechanism and you must say
   so in `docs/extending.md`; do not silently change the semantics.

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

   **You may not edit `internal/sink/sinktest/conformance.go`.** If it needs a branch for your
   engine, the interface has leaked and the fix is in your backend or in the interface — raise it,
   do not patch the suite. Step 6 of the build order gates on this file being byte-identical to its
   committed state.

6. **Add a compose service.** `docker-compose.yml` with a **pinned image tag**, a named volume, a
   healthcheck, and a host port on `127.0.0.1` that does not collide with 15432 / 13306 / 19092.
   The compose file owns the tag; copy that literal into your test file, never the reverse.

7. **Wire it up.** Add the `NFC_<ENGINE>_DSN` variable to `.env.example` and to
   `internal/config/config.go` (required **only** when your engine appears in `NFC_SINKS`), and add
   the constructor to the switch in `internal/app/run.go`.

8. **Document it.** One entry under `## Adding a Sink` in `docs/extending.md`, and the engine's row
   in `README.md`'s backend table. If it has a retention story different from Timescale's
   partition-drop, put it in `docs/runbook.md`.

## Verify

```bash
go build ./...                                        # expect: exit 0
go test -race ./internal/sink/<engine>/...            # expect: exit 0, ok, all six conformance assertions pass
git diff --quiet HEAD -- internal/sink/sinktest/conformance.go   # expect: exit 0 — the suite was not touched
go test -race ./internal/sink/...                     # expect: exit 0 — every backend still passes
go tool golangci-lint run ./...                       # expect: exit 0
```

## Do not

- **Do not add a method to `flow.Sink` or `flow.Querier`.** Two backends exist so that this pressure
  is visible; a third that needs a wider interface means the second one was carrying a Postgres
  assumption all along.
- **Do not expose SQL, a query builder, or a `*sql.DB` through the interface.** `WriteBatch` takes a
  slice and `Query` takes a `FlowQuery`; anything else makes the boundary decorative.
- **Do not edit the conformance suite** to make your backend pass.
- **Do not forget microsecond truncation.** Go timestamps carry nanoseconds and most engines do not.
  Truncate on write or the round-trip assertion fails on a correct implementation.
- **Do not use an unpinned image tag** in `docker-compose.yml`. `:latest` turns a green suite into a
  time bomb.
