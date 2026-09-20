---
description: goose migration conventions, embedding rules, and the forward-only production policy
paths:
  - "migrations/**"
---

# Migrations

- One migration system per database: **goose, driven through its Go API**, never through a CLI and
  never through a GUI. `Migrate(ctx)` on each backend calls `goose.SetBaseFS(migrations.FS)`, sets
  its dialect, and runs `goose.UpContext` against its own subdirectory.
- Migration files here are **hand-authored** and numbered (`00001_flow_schema.sql`). They are *not*
  produced by `goose create`, so their names are ours to choose. Every file carries `-- +goose Up`
  and `-- +goose Down`.
- `migrations/embed.go` must stay at the **root** of `migrations/`. A `//go:embed` directive is
  resolved relative to the directory of the file containing it and cannot reference a parent, so
  moving this file into `internal/sink/*` breaks the embed at compile time.
- Adding a backend means adding its directory to the `//go:embed` line. An empty directory makes
  `//go:embed` fail with "no matching files" — add the directory and its first migration together.
- Every DDL statement is idempotent (`IF NOT EXISTS`, `CREATE OR REPLACE`, `if_not_exists => TRUE`).
  Running `Migrate` twice must change nothing and exit 0; there is a build gate on it.
- **Never hand-edit a migration that has already run.** Add a new numbered one.
- Production is **forward-only**. The `Down` block exists for local teardown and is never the
  rollback plan — on a table this size a rollback is a new migration, not a reversal.
- Destructive changes are **expand → migrate → contract** across separate releases: add the new
  column, backfill in bounded batches, dual-write, switch reads, drop the old column later. A rename
  in one step breaks every instance of the old code still running during the deploy.
- Backfills run in bounded chunks with a `received_at` predicate. An unbounded `UPDATE` on
  `flow_records` will lock the hottest table in the system.
- Any unique index on the Postgres hypertable **must include `received_at`**, the partitioning
  column. `uq_flow_records_dedup (received_at, dedup_key)` already does; a new one must too.
- Timescale retention is a policy (`add_retention_policy`), not a `DELETE`. MariaDB has no
  partitioning in v1, so its retention is a bounded chunked `DELETE` on a ticker — if you change
  that, update `docs/runbook.md` in the same commit.
