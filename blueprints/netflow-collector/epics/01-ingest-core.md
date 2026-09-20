# Epic 01: Ingest core

> After this epic a NetFlow datagram or a Kafka message becomes a durable row in two independently
> implemented storage backends, with backpressure counted rather than silent.

| | |
|---|---|
| **Epic id** | `01-ingest-core` |
| **Tasks** | `E1-T1` … `E1-T7` |
| **Depends on** | nothing — start here |
| **Unlocks** | `02-serving-and-operations` |
| **Parallel with** | nothing — this epic builds the spine every later task reads |

You do not need any other file to complete this epic. Everything below is repeated here on purpose.

---

## Stack

Go static binary · `net/http` stdlib ServeMux · pgx/v5 + TimescaleDB · database/sql +
go-sql-driver/mysql + MariaDB · goose migrations embedded with `//go:embed` · goflow2/v2 decoding ·
franz-go Kafka · slog + OpenTelemetry + Prometheus · Docker Compose.
Module path: `github.com/bogie5464/netflow-collector`. Package manager: `go` modules. The toolchain is
pinned by the `toolchain` directive in `go.mod`; dependency versions live in `go.mod` and `go.sum` —
read them, never guess one.

| Task | Command |
|---|---|
| Build | `go build -o bin/collector ./cmd/collector` |
| Vet | `go vet ./...` |
| Lint | `go tool golangci-lint run ./...` |
| Test (one package) | `go test -race ./internal/sink/postgres/...` |
| Test (all) | `go test -race ./...` |
| Validate config | `./bin/collector -validate-config` |
| Local services up | `docker compose up -d --wait` |
| Local services down | `docker compose down` |

**Gate:** `go vet ./... && go tool golangci-lint run ./... && go test -race ./...` passes before any
task here is marked done.

Tasks E1-T4, E1-T5 and E1-T6 start pinned database containers through `testcontainers-go` and need a
running Docker daemon. E1-T7 needs the compose `redpanda` service — start it with
`docker compose up -d --wait redpanda`. `docker-compose.yml` shipped in `workspace/` and is already
at the project root; you do not write it, and you never substitute a fake for a service the
acceptance criteria name.

## Directory subtree

Only the parts this epic touches:

```
cmd/collector/
  main.go                       # NEW in E1-T1, edited in E1-T5 — flags, version, calls app.Run
internal/
  flow/
    flow.go                     # NEW in E1-T1 — FlowRecord, FlowType, FlowQuery, FlowResultPage
    ports.go                    # NEW in E1-T1 — Source, Sink, Querier, Backend. THE contract
  config/
    config.go                   # NEW in E1-T1 — .env loader, env->struct, validated once at boot
    config_test.go              # NEW in E1-T1
  source/
    netflow/
      netflow.go                # NEW in E1-T2 — UDP listener implementing flow.Source
      decode.go                 # NEW in E1-T2 — goflow2/v2 -> flow.FlowRecord mapping
      netflow_test.go           # NEW in E1-T2 — builds its own v5 datagram
    kafka/
      kafka.go                  # NEW in E1-T7 — franz-go consumer, sets DedupKey
      kafka_test.go             # NEW in E1-T7
  obs/
    metrics.go                  # NEW in E1-T3 — the Prometheus instruments, created once
  pipeline/
    pipeline.go                 # NEW in E1-T3 — bounded channel, batcher, worker pool, fan-out
    pipeline_test.go            # NEW in E1-T3
  sink/
    postgres/
      postgres.go               # NEW in E1-T4 — flow.Backend over pgxpool
      postgres_test.go          # NEW in E1-T4
    mariadb/
      mariadb.go                # NEW in E1-T6 — flow.Backend over database/sql
      mariadb_test.go           # NEW in E1-T6
    sinktest/
      conformance.go            # NEW in E1-T4 — the six assertions. FROZEN from E1-T6 onward
  app/
    run.go                      # NEW in E1-T5, edited in E1-T6 and E1-T7 — the only wiring layer
    e2e_udp_postgres_test.go    # NEW in E1-T5 — the vertical slice
migrations/
  embed.go                      # NEW in E1-T4, edited in E1-T6 — //go:embed postgres mariadb
  postgres/00001_flow_schema.sql  # NEW in E1-T4, hand-authored
  mariadb/00001_flow_schema.sql   # NEW in E1-T6, hand-authored
docker-compose.yml              # exists, shipped in workspace/, read-only for this epic
.env.example / .env             # exist, read-only for this epic
```

Everything outside this subtree is out of scope. If a task seems to require editing a file not
listed here, stop and report — it means the epic boundary is wrong.

## Data model touched here

| Entity | Fields this epic adds or reads | Notes |
|---|---|---|
| `exporters` | `id`, `ip_address`, `first_seen_at`, `last_seen_at`, `label`, `created_at`, `updated_at` | Unique index on `ip_address` makes first-sight upsert race-free. Sinks resolve `ExporterAddr` to `exporter_id` through a bounded in-memory cache and advance `last_seen_at` at most once per batch per exporter, never per record |
| `flow_records` | `received_at`, `seq`, `exporter_id`, `flow_type`, `first_switched`, `last_switched`, `src_addr`, `dst_addr`, `src_port`, `dst_port`, `protocol`, `tcp_flags`, `packets`, `bytes`, `sampling_rate`, `input_iface`, `output_iface`, `src_as`, `dst_as`, `next_hop`, `dedup_key` | Append-only. Hypertable on `received_at` in Postgres; unpartitioned in MariaDB v1. `UNIQUE (received_at, dedup_key)` — NULLs are distinct in both engines, which is what makes Kafka redelivery dedup while leaving the UDP path append-only. `seq` is a monotonic tie-breaker for the keyset cursor, not a public id. FK to `exporters` is `ON DELETE RESTRICT` |

Timestamps: both engines store **microseconds**, Go carries nanoseconds. Truncate to microseconds on
write and in every fixture, or round-trip equality fails on correct code.

## Contracts

**Consumed** — already exists, do not rebuild:

| From | Interface | Guarantee |
|---|---|---|
| `workspace/` (copied to the project root by Bootstrap) | `docker-compose.yml` | `timescaledb` on 127.0.0.1:15432, `mariadb` on 127.0.0.1:13306, `redpanda` on 127.0.0.1:19092, all with healthchecks; database, user and password are all `netflow`. This file owns the image tags |
| `workspace/` | `.env.example` → `.env` | Every `NFC_*` variable with a working local value, present before task one |
| `workspace/` | `.golangci.yml` | golangci-lint v2 schema, `blueprints` excluded |

**Produced** — later epics depend on exactly these signatures. Changing one breaks them:

| Export | Signature | Used by |
|---|---|---|
| `internal/flow` → `Source` | `Start(ctx context.Context, out chan<- FlowRecord) error` | `02-serving-and-operations`, and every future input adapter |
| `internal/flow` → `Sink` | `WriteBatch(ctx context.Context, records []FlowRecord) error` | `internal/pipeline`, every future backend |
| `internal/flow` → `Querier` | `Query(ctx context.Context, q FlowQuery) (FlowResultPage, error)` | `internal/api` in `E2-T1` |
| `internal/flow` → `Backend` | `Sink; Querier; Migrate(ctx) error; Close() error` | `internal/app/run.go` |
| `internal/config` → `Config`, `Load()` | env→struct, validated once, `.env` found by walking up | every package in epic 02 |
| `internal/obs` → the six instruments | package-level, created once | `internal/pipeline`, `internal/api`, `E2-T3` |
| `internal/sink/sinktest` → `Conformance` | `Conformance(t *testing.T, newBackend func(t *testing.T) (flow.Sink, flow.Querier))` | every backend, forever |
| `internal/app` → `Run(ctx) error` | wires config → sources → pipeline → backends → signals | `cmd/collector/main.go`, `E2-T1`, `E2-T3` |

## Conventions that bite in this area

- **`internal/flow` imports nothing from this module.** Anything it imports becomes part of the
  contract every adapter must accept.
- **`ports.go` must never name `net.Conn`, `net.Addr` or `net.PacketConn`.** E1-T2 has a build gate
  on that grep, because a future database-import `Source` has to satisfy the interface without
  pretending to be a socket.
- **`Sink` stays batch-shaped.** No SQL string, no query builder, no `*sql.DB`, no dialect flag. The
  moment one leaks through, MariaDB is implementing a Postgres API.
- **`testify/require`, never `testify/assert`.** `assert` keeps executing past a failed nil check and
  the follow-on panic hides the real error.
- **`//go:embed` cannot reference a parent directory.** That is the only reason `migrations/embed.go`
  sits at the root of `migrations/`. Do not move it. Embedding an empty directory fails with "no
  matching files", so a backend's directory and its first migration land together.
- **Build fixtures, do not paste hex blobs.** `netflow_test.go` constructs its own NetFlow v5
  datagram with `encoding/binary` from the field layout below, then asserts the decoder returns what
  the test encoded.
- **NetFlow v5 wire layout** — big-endian throughout. Header, 24 bytes: `version` u16, `count` u16,
  `sys_uptime` u32, `unix_secs` u32, `unix_nsecs` u32, `flow_sequence` u32, `engine_type` u8,
  `engine_id` u8, `sampling_interval` u16. Then `count` records of 48 bytes: `srcaddr` u32,
  `dstaddr` u32, `nexthop` u32, `input` u16, `output` u16, `dPkts` u32, `dOctets` u32, `first` u32,
  `last` u32, `srcport` u16, `dstport` u16, pad u8, `tcp_flags` u8, `prot` u8, `tos` u8, `src_as`
  u16, `dst_as` u16, `src_mask` u8, `dst_mask` u8, pad u16.
- **Read goflow2's exported surface before mapping it — it is two packages, not one.**
  `go doc github.com/netsampler/goflow2/v2/decoders/netflowlegacy` for v5 (fixed layout,
  `DecodeMessage`/`PacketNetFlowV5`) and `go doc github.com/netsampler/goflow2/v2/decoders/netflow`
  for v9/IPFIX (`DecodeMessageVersion`/`NetFlowTemplateSystem`) — take the decoder entry point and
  template-system type from each package itself rather than from memory, then map into
  `flow.FlowRecord`. Dispatch on the packet's first 2 bytes (the wire version). v9 and IPFIX need the
  template system to persist across datagrams; v5 needs no template at all.
- **Backpressure is drop-and-count.** The pipeline never blocks a source and never grows its buffer.
  `ingested + dropped == offered` is an invariant with a test.
- **`ReceivedAt` on the Kafka path comes from the message**, never the consumer's clock — otherwise a
  redelivered message becomes a different row and `(received_at, dedup_key)` uniqueness does nothing.
- Every `Verify` command exits 0 when the task is correct. Where the right answer is a failure, the
  line asserts the specific code (`cmd; test $? -eq 2`), never a bare command and never `!` alone.

Full project rules: `CLAUDE.md`. Area rules: `.claude/rules/flow-contracts.md`,
`.claude/rules/migrations.md`, `.claude/rules/testing.md`. Both sit in the project root — the builder
copied them there from the bundle's `workspace/` before task one.

---

## Tasks

Listed in the same order as `tasks.json`. That order is the build order — work top to bottom and do
not re-rank by priority or by what looks quick.

### `E1-T1` — Scaffold domain model, ports, config and binary

**Depends on:** nothing · **Priority:** p0 — metadata for scope cuts, not a running order

Write the contract before anything implements it. `internal/flow/flow.go` holds `FlowRecord` (use
`netip.Addr` for every address and `time.Time` for every timestamp; `DedupKey []byte` is nil for
append-only sources), `FlowType` as a string constant set, `FlowQuery` (mandatory `Start`/`End`,
optional exporter/address/protocol filters, `Limit`, opaque `Cursor`) and `FlowResultPage`.
`internal/flow/ports.go` holds the four interfaces and nothing else. `internal/config/config.go`
defines the full `Config` struct for **every** `NFC_*` variable in `.env.example` — later steps must
not add required variables, or they would break this step's gate — with `caarlos0/env` for binding
and `validator/v10` for rules, and variables required only when their feature is enabled
(`required_if` on `NFC_SINKS` / `NFC_SOURCES`). `Load()` first walks up from the working directory
for the nearest `.env`, at most ten levels, and sets only keys not already exported, so `go test`
inside a package directory still finds it. `main.go` handles `-version`, `-validate-config`,
`-dump-openapi` and `-healthcheck` **before** config validation so those flags keep working as later
steps tighten what a full boot requires; `version` is a package-level var stamped by `-ldflags`,
defaulting to `dev`. Exit codes: 0 clean, 1 unexpected, 2 invalid config, 3 migration failure.

**Files**
- `internal/flow/flow.go` — new
- `internal/flow/ports.go` — new
- `internal/config/config.go` — new
- `internal/config/config_test.go` — new
- `cmd/collector/main.go` — new

**Acceptance**

Copied verbatim from this task's `acceptance` array in `tasks.json`. Each one is decidable by a
command below, on this machine, during the build.

1. **WHEN** `go build -o bin/collector ./cmd/collector` runs **THE SYSTEM SHALL** exit 0 and produce an executable at `bin/collector`.
2. **WHEN** `./bin/collector -version` runs **THE SYSTEM SHALL** exit 0 and print a line beginning `netflow-collector `.
3. **WHEN** `./bin/collector -validate-config` runs with `NFC_SINKS=nope` **THE SYSTEM SHALL** exit with code 2 and write a message containing `NFC_SINKS` to stderr.
4. **WHEN** a `.env` file exists at or above the working directory **THE SYSTEM SHALL** load its keys before parsing configuration and **SHALL NOT** overwrite a variable already exported in the shell.
5. **WHEN** `go test ./internal/config/... ./internal/flow/...` runs **THE SYSTEM SHALL** exit 0 with 0 failures.
6. **WHEN** `go vet ./...` runs **THE SYSTEM SHALL** exit 0.

**Verify** — every command, in order, run from the project root. Each one exits 0 when this task is
correct; the last one exiting 0 is what makes the task done.

```bash
go build -o bin/collector ./cmd/collector
./bin/collector -version | grep -qE '^netflow-collector '
env NFC_SINKS=nope ./bin/collector -validate-config >/tmp/nfc-step1.err 2>&1; test $? -eq 2
grep -q 'NFC_SINKS' /tmp/nfc-step1.err
go test ./internal/config/... ./internal/flow/...
go vet ./...
```

**Checkpoint**

```bash
git add -A && git commit -m "E1-T1: scaffold domain model, ports, config and binary"
git tag step-01-scaffold
git ls-files --error-unmatch .env.example    # expect: exit 0 — no ignore rule swallowed it
git ls-files --error-unmatch .golangci.yml   # expect: exit 0
```

### `E1-T2` — Add NetFlow v5/v9/IPFIX UDP source

**Depends on:** `E1-T1` · **Priority:** p0

A `net.ListenPacket` loop on `NFC_NETFLOW_ADDR` with a read buffer of 65535 bytes, handing each
datagram to `decode.go`, which sniffs the wire version (the packet's first 2 bytes) and dispatches
to one of **two separate goflow2/v2 packages** — `decoders/netflowlegacy` for v5 (fixed layout,
`DecodeMessage`/`PacketNetFlowV5`) and `decoders/netflow` for v9/IPFIX (template-driven,
`DecodeMessageVersion`/`NetFlowTemplateSystem`) — before mapping either result to `flow.FlowRecord`.
Keep the template system for v9/IPFIX alive across datagrams and keyed by exporter — a template
arrives once and every later datagram from that exporter depends on it. Set `ReceivedAt` once per
datagram from the collector's clock, truncated to microseconds, and leave `DedupKey` nil: UDP is
append-only here by design, because NetFlow has no retransmission and paying for dedup would cost a
unique-index write on every record to prevent something that does not happen. Never block on the
output channel — `select` on `ctx.Done()` so cancellation is always honoured. The test builds its
own v5 datagram from the layout in this epic's conventions; do not paste a capture.

**Files**
- `internal/source/netflow/netflow.go` — new
- `internal/source/netflow/decode.go` — new
- `internal/source/netflow/netflow_test.go` — new

**Acceptance**

1. **WHEN** a NetFlow v5 datagram carrying 2 flow records is sent to the listener's UDP port **THE SYSTEM SHALL** emit exactly 2 records on the output channel with `flow_type` `netflow5`.
2. **WHEN** a v5 record is decoded **THE SYSTEM SHALL** reproduce the `srcaddr`, `dstaddr`, `srcport`, `dstport`, `prot`, `dPkts` and `dOctets` values that the test encoded into the datagram.
3. **WHEN** a datagram shorter than the 24-byte NetFlow v5 header arrives **THE SYSTEM SHALL** increment `netflow_packets_decode_errors_total{source="netflow"}`, emit no record, and keep listening.
4. **WHEN** the context passed to `Start` is cancelled **THE SYSTEM SHALL** close its UDP socket and return within 1 second with a nil error.
5. **WHEN** `internal/flow/ports.go` is inspected **THE SYSTEM SHALL** contain no reference to `net.Conn`, `net.Addr` or `net.PacketConn`, so a non-network adapter can implement `Source` unchanged.

**Verify**

```bash
go test -race ./internal/source/netflow/...
test -f internal/flow/ports.go
! grep -qE '\bnet\.(Conn|Addr|PacketConn)\b' internal/flow/ports.go
go vet ./...
go tool golangci-lint run ./...
```

The `test -f` line runs first on purpose: it guarantees the file is readable, so the grep below it
can only return 0 (match) or 1 (no match) and never 2 (error), which would otherwise satisfy `!`
vacuously.

**Checkpoint**

```bash
git add -A && git commit -m "E1-T2: add NetFlow v5/v9/IPFIX UDP source"
git tag step-02-netflow-source
```

### `E1-T3` — Add bounded ingest pipeline with drop accounting

**Depends on:** `E1-T1` · **Priority:** p0

`internal/obs/metrics.go` creates the six instruments once, at package level, over
`prometheus/client_golang`'s default registerer: `netflow_records_ingested_total{source}`,
`netflow_records_dropped_total{reason}`, `netflow_batch_write_duration_seconds{sink}`,
`netflow_batch_write_errors_total{sink}`, `netflow_pipeline_buffer_length`, and
`netflow_record_visibility_lag_seconds`. `pipeline.go` owns a `chan flow.FlowRecord` of capacity
`NFC_PIPELINE_BUFFER`; the ingest side does a non-blocking `select` with a `default` that increments
the drop counter — that `default` branch is the entire backpressure strategy, and it must never be
replaced with a blocking send or a growing slice. The batcher flushes at `NFC_BATCH_SIZE` or after
`NFC_BATCH_INTERVAL`, whichever comes first; `NFC_WORKERS` goroutines take batches and call
`WriteBatch` on every sink **in parallel**, collecting errors per sink rather than aborting the
batch. A slow or failing sink degrades that sink's metrics and nothing else.

**Files**
- `internal/obs/metrics.go` — new
- `internal/pipeline/pipeline.go` — new
- `internal/pipeline/pipeline_test.go` — new

**Acceptance**

1. **WHEN** a source offers more records than the bounded channel can hold and every sink is blocked **THE SYSTEM SHALL** drop the excess, increment `netflow_records_dropped_total{reason="buffer_full"}` once per dropped record, and never block the sending source.
2. **WHEN** 10000 records are offered to a pipeline whose sink accepts instantly **THE SYSTEM SHALL** write 10000 records and report `netflow_records_dropped_total` of 0.
3. **WHEN** pending records reach `NFC_BATCH_SIZE` **THE SYSTEM SHALL** flush a batch of exactly that size, and **WHEN** `NFC_BATCH_INTERVAL` elapses with a partial batch **THE SYSTEM SHALL** flush the partial batch.
4. **WHEN** two sinks are configured and one returns an error **THE SYSTEM SHALL** still deliver the batch to the other, increment `netflow_batch_write_errors_total` for the failing sink label, and keep running.
5. **WHEN** the pipeline's context is cancelled **THE SYSTEM SHALL** flush the pending partial batch to every sink before returning.
6. **WHEN** a load of records passes through the pipeline **THE SYSTEM SHALL** satisfy `ingested_total + dropped_total == offered` with no record unaccounted for.

**Verify**

```bash
go test -race ./internal/pipeline/... ./internal/obs/...
go vet ./...
go tool golangci-lint run ./...
```

**Checkpoint**

```bash
git add -A && git commit -m "E1-T3: add bounded ingest pipeline with drop accounting"
git tag step-03-pipeline
```

### `E1-T4` — Add Postgres/TimescaleDB backend and conformance suite

**Depends on:** `E1-T3` · **Priority:** p0

Two artefacts of equal importance: the backend, and the suite that will judge every backend after
it. Write the hand-authored goose migration (both `-- +goose Up` and `-- +goose Down`), embed it via
`migrations/embed.go` with `//go:embed postgres` — only `postgres` for now, because embedding an
empty `mariadb/` fails to compile. `postgres.go` exposes `New(ctx, dsn, retentionDays) (flow.Backend, error)`
over `pgxpool`: `Migrate` runs goose then applies `add_retention_policy`; `WriteBatch` resolves
exporters through a bounded cache and inserts with `ON CONFLICT DO NOTHING` so the nullable
`dedup_key` does the dedup work without any branch on record provenance; `Query` builds the keyset
predicate `(received_at, seq) < ($1, $2)` with `ORDER BY received_at DESC, seq DESC`.
`sinktest/conformance.go` is the load-bearing artefact of this whole project: six assertions,
engine-agnostic, driven by a factory the caller supplies. Its container helper starts
`timescale/timescaledb:2.30.1-pg17` through `testcontainers-go` with an explicit `wait.ForSQL`
strategy — take that tag literally from `docker-compose.yml`, which owns it.

**Files**
- `migrations/postgres/00001_flow_schema.sql` — new
- `migrations/embed.go` — new
- `internal/sink/postgres/postgres.go` — new
- `internal/sink/sinktest/conformance.go` — new
- `internal/sink/postgres/postgres_test.go` — new

**Acceptance**

1. **WHEN** `Migrate` runs against an empty PostgreSQL/TimescaleDB database **THE SYSTEM SHALL** create the `exporters` and `flow_records` tables and convert `flow_records` into a hypertable partitioned on `received_at`, and a second run **SHALL** change nothing and exit 0.
2. **WHEN** `sinktest.Conformance` runs against the Postgres backend **THE SYSTEM SHALL** pass all six conformance assertions with 0 failures.
3. **WHEN** a batch is written and then queried over its time range **THE SYSTEM SHALL** return every record with every field equal to the value written, comparing timestamps at microsecond precision.
4. **WHEN** a batch whose records carry a non-nil `DedupKey` is written twice **THE SYSTEM SHALL** leave the `flow_records` row count unchanged.
5. **WHEN** a batch whose records carry a nil `DedupKey` is written twice **THE SYSTEM SHALL** double the `flow_records` row count, preserving append-only semantics on the UDP path.
6. **WHEN** 25 records are paged with a limit of 10 **THE SYSTEM SHALL** return them across 3 pages with no record repeated or missing and `HasMore` false on the last page.

**Verify**

```bash
go test -race -run TestPostgres ./internal/sink/postgres/...
grep -q 'timescale/timescaledb:2.30.1-pg17' docker-compose.yml
grep -q 'timescale/timescaledb:2.30.1-pg17' internal/sink/sinktest/conformance.go
go vet ./...
go tool golangci-lint run ./...
```

**Checkpoint**

```bash
git add -A && git commit -m "E1-T4: add Postgres/TimescaleDB backend and conformance suite"
git tag step-04-postgres-backend
```

### `E1-T5` — Wire the UDP to Postgres vertical slice

**Depends on:** `E1-T2`, `E1-T4` · **Priority:** p0

The step that proves the spine before it is multiplied. `internal/app/run.go` is the **only** place
concrete types are constructed: read `Config`, build the backends named in `NFC_SINKS`, call
`Migrate` on each **before** any listener binds, build the sources named in `NFC_SOURCES`, start the
pipeline, install a `signal.NotifyContext` for SIGINT/SIGTERM, and on cancellation stop sources,
flush, close backends, and return. An unknown name in either list is a config error — exit 2 naming
it; a migration failure is exit 3 with nothing listening. `main.go` shrinks to flag handling plus
`app.Run(ctx)`. The end-to-end test starts the real app against a Timescale container on an
ephemeral UDP port, sends one constructed v5 datagram over `net.Dial("udp", ...)`, and polls the
`Querier` until the record appears or 5 seconds elapse.

**Files**
- `internal/app/run.go` — new
- `cmd/collector/main.go` — edit: delegate to `app.Run`, keep the four short-circuit flags
- `internal/app/e2e_udp_postgres_test.go` — new

**Acceptance**

1. **WHEN** the collector starts with `NFC_SOURCES=netflow` and `NFC_SINKS=postgres` against a live TimescaleDB **THE SYSTEM SHALL** apply migrations at boot and then listen on `NFC_NETFLOW_ADDR`.
2. **WHEN** a NetFlow v5 datagram containing 1 flow record is sent to that port **THE SYSTEM SHALL** make the record retrievable through `Querier.Query` within 5 seconds.
3. **WHEN** `NFC_SINKS` names a backend the binary does not implement **THE SYSTEM SHALL** exit with code 2 and name the unknown backend in its message.
4. **WHEN** the process receives SIGTERM **THE SYSTEM SHALL** stop its sources, flush pending batches, close every backend, and exit 0 within 10 seconds.
5. **WHEN** migrations fail at boot **THE SYSTEM SHALL** exit with code 3 without binding any listener.

**Verify**

```bash
go build -o bin/collector ./cmd/collector
go test -race -run TestVerticalSlice ./internal/app/...
env NFC_SINKS=nosuchdb ./bin/collector -validate-config >/tmp/nfc-step5.err 2>&1; test $? -eq 2
grep -q 'nosuchdb' /tmp/nfc-step5.err
./bin/collector -version | grep -qE '^netflow-collector '
```

**Checkpoint**

```bash
git add -A && git commit -m "E1-T5: wire the UDP to Postgres vertical slice"
git tag step-05-vertical-slice
```

### `E1-T6` — Add MariaDB backend passing the same suite

**Depends on:** `E1-T5` · **Priority:** p0

This is the task the architecture exists for. Write the MariaDB migration (`DATETIME(6)`,
`VARBINARY(16)` addresses, `AUTO_INCREMENT` primary key on `seq`, `UNIQUE KEY (received_at,
dedup_key)`), add `mariadb` to the `//go:embed` line, and implement `flow.Backend` over
`database/sql` with `go-sql-driver/mysql`. Use `INSERT IGNORE` as the conflict-tolerant form and row
constructors `(received_at, seq) < (?, ?)` for the cursor — MariaDB supports them. The DSN must
carry `parseTime=true&loc=UTC`, or the driver reinterprets every timestamp in the process's local
zone. There is **no** partitioning in v1: MariaDB's `RANGE` partitioning requires the partitioning
expression in every unique key, which would force `dedup_key` into it and break the dedup semantics,
so retention is a bounded chunked `DELETE` on a ticker. Then run the **existing** suite:
`sinktest.Conformance(t, factory)`. If it needs a MariaDB branch, the interface leaked — fix the
backend, never the suite.

**Files**
- `migrations/mariadb/00001_flow_schema.sql` — new
- `migrations/embed.go` — edit: `//go:embed postgres mariadb`
- `internal/sink/mariadb/mariadb.go` — new
- `internal/sink/mariadb/mariadb_test.go` — new
- `internal/app/run.go` — edit: register `mariadb` in the sink switch

**Acceptance**

1. **WHEN** `sinktest.Conformance` runs against the MariaDB backend **THE SYSTEM SHALL** pass all six assertions with 0 failures, using the same suite source file as the Postgres backend with no MariaDB-specific branch inside it.
2. **WHEN** `internal/sink/sinktest/conformance.go` is compared against its committed state at tag `step-04-postgres-backend` **THE SYSTEM SHALL** report no difference.
3. **WHEN** `Migrate` runs against an empty MariaDB database **THE SYSTEM SHALL** create `exporters` and `flow_records` with a unique index on `(received_at, dedup_key)`, and a second run **SHALL** change nothing and exit 0.
4. **WHEN** a record carrying an IPv6 `src_addr` is written and read back through the MariaDB backend **THE SYSTEM SHALL** return the same address unchanged.
5. **WHEN** `NFC_SINKS=postgres,mariadb` **THE SYSTEM SHALL** fan each batch out to both backends and record `netflow_batch_write_duration_seconds` separately per sink label.

**Verify**

```bash
go test -race -run TestMariaDB ./internal/sink/mariadb/...
git diff --quiet step-04-postgres-backend -- internal/sink/sinktest/conformance.go
grep -q 'mariadb:11.8.9' docker-compose.yml
grep -q 'mariadb:11.8.9' internal/sink/mariadb/mariadb_test.go
go test -race ./internal/sink/...
```

The `git diff --quiet` line is legitimate here because this task does **not** touch
`conformance.go` — the file was committed two tasks ago, so the comparison is decidable at Verify
time. Exit 0 means the suite is untouched.

**Checkpoint**

```bash
git add -A && git commit -m "E1-T6: add MariaDB backend passing the same suite"
git tag step-06-mariadb-backend
```

### `E1-T7` — Add Kafka source with dedup on write

**Depends on:** `E1-T6` · **Priority:** p1

A franz-go consumer group over `NFC_KAFKA_BROKERS` / `NFC_KAFKA_TOPIC` / `NFC_KAFKA_GROUP`, decoding
one JSON flow record per message — the schema is in `blueprint.md` §5, *Kafka ingest message
schema*. Two rules decide correctness: `ReceivedAt` comes from the message's `received_at`, falling
back to the Kafka record timestamp, and **never** from the consumer's clock; and `DedupKey` is a
16-byte digest of `(exporter_addr, first_switched, src_addr, dst_addr, src_port, dst_port,
protocol)`. Together they make a redelivered message collide with itself on `(received_at,
dedup_key)` and be discarded by the sink's conflict-tolerant insert — with no code in any sink that
knows Kafka exists. Unknown JSON fields are ignored; a message that fails to decode increments the
decode-error counter, is committed past, and does not wedge the consumer. The test produces to the
compose `redpanda` service and retries its first metadata fetch for up to 30 s.

**Files**
- `internal/source/kafka/kafka.go` — new
- `internal/source/kafka/kafka_test.go` — new
- `internal/app/run.go` — edit: register `kafka` in the source switch

**Acceptance**

1. **WHEN** a JSON flow message is produced to `NFC_KAFKA_TOPIC` **THE SYSTEM SHALL** decode it and emit one record whose fields equal the message's fields.
2. **WHEN** a message is decoded **THE SYSTEM SHALL** set `ReceivedAt` from the message's `received_at` field, falling back to the Kafka record timestamp, and **SHALL NOT** use the consumer's wall clock.
3. **WHEN** a message is decoded **THE SYSTEM SHALL** populate `DedupKey` with a 16-byte digest of `(exporter_addr, first_switched, src_addr, dst_addr, src_port, dst_port, protocol)`.
4. **WHEN** the same message is delivered twice and written through a backend **THE SYSTEM SHALL** leave the `flow_records` row count unchanged.
5. **WHEN** a message fails to decode **THE SYSTEM SHALL** increment `netflow_packets_decode_errors_total{source="kafka"}`, commit past it, and keep consuming.
6. **WHEN** the context passed to `Start` is cancelled **THE SYSTEM SHALL** close the client and return within 5 seconds.

**Verify**

```bash
docker compose up -d --wait redpanda
go test -race -run TestKafka ./internal/source/kafka/...
go test -race ./...
go tool golangci-lint run ./...
```

**Checkpoint**

```bash
git add -A && git commit -m "E1-T7: add Kafka source with dedup on write"
git tag step-07-kafka-source
```

---

## Epic acceptance

The epic is done when every task is `done` **and**:

1. **WHEN** a NetFlow v5 datagram is sent to a collector configured with `NFC_SOURCES=netflow` and `NFC_SINKS=postgres,mariadb` **THE SYSTEM SHALL** make the record retrievable through both backends' `Querier` within 5 seconds.
2. **WHEN** every sink is blocked and the bounded channel fills **THE SYSTEM SHALL** drop records, count every drop, keep the UDP listener responsive, and satisfy `ingested + dropped == offered`.

```bash
go vet ./... && go tool golangci-lint run ./... && go test -race ./...
go test -race -run 'TestVerticalSlice|TestPostgres|TestMariaDB' ./internal/...
```

Run from the project root.

## Pitfalls

- **Embedding an empty directory.** `//go:embed mariadb` before `migrations/mariadb/` has a file in
  it fails to compile with "no matching files". E1-T4 embeds only `postgres`; E1-T6 adds the second.
- **Moving `migrations/embed.go`.** The directive resolves relative to its own directory and cannot
  reference a parent, so it cannot live inside `internal/sink/*`. This is the one Go-specific
  resolution trap in this project.
- **Nanosecond timestamps.** Go carries ns; both engines store µs. A conformance failure that looks
  like a data-loss bug is almost always a missing `Truncate(time.Microsecond)`.
- **MariaDB DSN without `parseTime=true&loc=UTC`.** Values come back as strings or in local time and
  every timestamp assertion fails in a way that looks like a driver bug.
- **A blocking send on the pipeline channel.** It works under test load and silently converts
  backpressure into unbounded latency on the UDP listener in production. The `default` branch is the
  design.
- **`testify/assert`.** A failed nil assertion is followed by a panic that hides the real error.
- **Taking `ReceivedAt` from the consumer's clock on the Kafka path.** Every criterion still passes
  except the dedup one, and the failure reads as a database problem.
- **`go mod tidy` before the code imports its dependencies.** Tidy prunes requires nothing uses; on a
  nearly-empty module it strips everything Bootstrap installed. The tidy gate lives in E2-T6.
- **Patching `conformance.go` to make MariaDB pass.** E1-T6 has a gate on that file being unchanged,
  and the gate is the point of the epic.

## Before moving on

- [ ] Every task in this epic is `done` in `tasks.json` — no task left `in_progress`.
- [ ] Every `verify` command of every task in this epic passed, not just the first one.
- [ ] No `verify` command was edited, and none was skipped because a file it names did not exist.
- [ ] **Every task in this epic has its `checkpoint` tag in version control** — one tag per task,
      matching the `checkpoint` value in `tasks.json`. `git tag -l 'step-*'` lists them.
- [ ] Gate command passes clean, run from the project root.
- [ ] Every "Produced" contract above exists with the stated signature.
- [ ] No file outside the subtree was modified.
- [ ] `.env.example` needed no new variable — every `NFC_*` key was defined in `E1-T1`. If you added
      one anyway, it must be optional until the step that consumes it.
- [ ] One commit per task, each prefixed with its task id, each followed by its checkpoint tag.
