# NetFlow Collector

A Go daemon that ingests NetFlow v5/v9/IPFIX and Kafka flow records into pluggable storage
backends, and serves them over a small REST API. No vendor lock-in: every backend implements the
same `flow.Backend` contract and passes the same conformance suite, so swapping or adding storage
engines never touches the ingest path or the API.

## Commands

| Task | Command |
|---|---|
| Build | `go build -o bin/collector ./cmd/collector` |
| Run | `./bin/collector` — API on http://127.0.0.1:8080, NetFlow UDP on :2055 |
| Version | `./bin/collector -version` |
| Validate config | `./bin/collector -validate-config` — exit 2 names the bad variable |
| Vet | `go vet ./...` |
| Lint | `go tool golangci-lint run ./...` |
| Format | `go tool golangci-lint fmt ./...` · check: `test -z "$(gofmt -l ./cmd ./internal ./migrations)"` |
| Tests | `go test -race ./...` · one package: `go test -race ./internal/sink/postgres/...` |
| Load test | `go test -tags=load -run TestPipelineLoad ./internal/pipeline/...` (no `-race`: it would fail the throughput floor) |
| Tidy check | `go mod tidy -diff` |
| Vuln scan | `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` |
| Services up | `docker compose up -d --wait` |
| Services down | `docker compose down` |
| Services reset | `docker compose down -v && docker compose up -d --wait` (destroys data) |
| Full stack | `docker compose --profile app up -d --build --wait` |
| OpenAPI | `./bin/collector -dump-openapi > docs/openapi.json` |

**Gate:** `go vet ./... && go tool golangci-lint run ./... && go test -race ./...` must pass before
any task is marked done.

Toolchain is pinned by the `toolchain` directive in `go.mod`. Dependency versions live in `go.mod`
and `go.sum` — read them, never guess one.

## Stack

Go static binary · `net/http` stdlib ServeMux · pgx/v5 + TimescaleDB (default backend) ·
database/sql + go-sql-driver/mysql (MariaDB backend) · goose migrations embedded via `//go:embed` ·
goflow2/v2 decoding · franz-go Kafka · slog + OpenTelemetry + Prometheus · Docker Compose.

## Architecture

**Ingest path.** exporter UDP datagram → `internal/source/netflow` (implements `flow.Source`) →
bounded channel in `internal/pipeline` → batcher (size `NFC_BATCH_SIZE` or age `NFC_BATCH_INTERVAL`)
→ worker pool → `WriteBatch` fanned out in parallel to every configured `flow.Sink`
(`internal/sink/postgres`, `internal/sink/mariadb`).

**Query path.** `GET /v1/flows` → `internal/api/auth.go` middleware → `internal/api/flows.go`
(validator/v10 over the request struct) → `flow.Querier` → the backend's keyset query → envelope.

**Backpressure is drop-and-count, never block.** When the channel is full the pipeline discards the
record and increments `netflow_records_dropped_total{reason="buffer_full"}`. It must never block a
source and never grow the buffer. `ingested + dropped == offered` is an invariant with a test.

**Boundaries.** Cross one the wrong way and the abstraction is gone:

| Package | May import | Must never import |
|---|---|---|
| `internal/flow` | stdlib only | anything in this module |
| `internal/pipeline` | `flow`, `obs` | any concrete source or sink |
| `internal/source/*` | `flow`, `obs` | `pipeline`, `api`, any sink |
| `internal/sink/*` | `flow`, `obs`, `migrations` | `pipeline`, `api`, any source |
| `internal/api` | `flow`, `obs`, `config` | any concrete backend package |
| `internal/app` | everything | — it is the only wiring layer |

**Where things live.**

| Concern | Single source of truth |
|---|---|
| The two pluggable boundaries | `internal/flow/ports.go` — `Source`, `Sink`, `Querier`, `Backend`, and nothing else |
| Optional storage capabilities | `internal/flow/exporter.go` — e.g. `ExporterLister`, discovered by type assertion (`backend.(flow.ExporterLister)`). Never added to `Backend` itself |
| Domain types | `internal/flow/flow.go` — `FlowRecord`, `FlowQuery`, `FlowResultPage` |
| Env access | `internal/config/config.go` — validated once at boot; nothing else reads `os.Getenv`, except `cmd/collector/main.go`'s `-healthcheck` path, which reads `NFC_HTTP_ADDR` directly because it must run before config validation |
| Schema | `migrations/postgres/*.sql`, `migrations/mariadb/*.sql`, embedded by `migrations/embed.go` |
| Storage conformance | `internal/sink/sinktest/conformance.go` — every backend passes it unmodified |
| Metrics | `internal/obs/metrics.go` — instruments are created once, here (`netflow_packets_decode_errors_total` at package scaffold time, the other six with the pipeline) |
| Image tags, ports, credentials | `docker-compose.yml` |

## Code rules

1. **`internal/flow` is the contract.** It imports nothing from this module. `ports.go` must never
   name `net.Conn`, `net.Addr` or `net.PacketConn` — a database-import `Source` has to satisfy it.
2. **`Sink` stays batch-shaped.** Never add a query builder, a SQL string parameter, or a
   `*sql.DB` to it. The moment it leaks SQL, MariaDB and every future backend are dead.
3. **A new backend passes `sinktest.Conformance` unmodified.** If the suite needs a branch for your
   backend, the interface is wrong, not the suite.
4. **`context.Context` is the first parameter** of every I/O function. Never store it in a struct.
5. **Wrap errors with `%w`** and inspect with `errors.Is`/`errors.As`. Never compare error strings.
6. **Truncate every timestamp to microseconds before writing.** Postgres `timestamptz` and MariaDB
   `DATETIME(6)` both store µs; Go carries ns. Skipping this breaks round-trip equality.
7. **Never use `testify/assert` — use `require`.** `assert` keeps running past a failed nil check
   and the follow-on panic hides the real failure.
8. **300 lines per file, soft cap.** Split by responsibility, not by type.
9. **No new dependency without a reason in the commit message.** Check stdlib first.

## Design system

NOT APPLICABLE — headless daemon and REST API, no UI in v1. Output shapes are the JSON envelope in
`blueprint.md` §5 and the Prometheus metric names in `internal/obs/metrics.go`.

## Environment

`internal/config` loads the nearest `.env` by walking **up** from the working directory (so
`go test` inside a package still finds it) and never overwrites a variable already exported in the
shell. `.env.example` is committed and stays in sync; `.env` is not.

| Variable | Required | Used by | Source |
|---|---|---|---|
| `NFC_SOURCES` | yes | `internal/app/run.go` | `netflow`, `kafka`, or both |
| `NFC_SINKS` | yes | `internal/app/run.go` | `postgres`, `mariadb`, or both |
| `NFC_POSTGRES_DSN` | if `postgres` enabled | `internal/sink/postgres` | `docker-compose.yml` host port 15432 |
| `NFC_MARIADB_DSN` | if `mariadb` enabled | `internal/sink/mariadb` | `docker-compose.yml` host port 13306 |
| `NFC_KAFKA_BROKERS` `_TOPIC` `_GROUP` | if `kafka` enabled | `internal/source/kafka` | `docker-compose.yml` host port 19092 |
| `NFC_API_KEYS` | if the API is enabled | `internal/api/auth.go` | `openssl rand -hex 32` |
| `NFC_HTTP_ADDR` `NFC_NETFLOW_ADDR` | yes | `internal/app/run.go` | `.env.example` |
| `NFC_PIPELINE_BUFFER` `NFC_BATCH_SIZE` `NFC_BATCH_INTERVAL` `NFC_WORKERS` | yes (defaulted) | `internal/pipeline` | `.env.example` |
| `NFC_RETENTION_DAYS` `NFC_LOG_LEVEL` `NFC_OTEL_ENABLED` `NFC_OTEL_ENDPOINT` | yes (defaulted) | sinks, obs | `.env.example` |

## Rules

Deferred conventions — read the matching file before editing that area:

| File | Applies to |
|---|---|
| `.claude/rules/flow-contracts.md` | `internal/flow/**`, `internal/source/**`, `internal/sink/**` |
| `.claude/rules/migrations.md` | `migrations/**` |
| `.claude/rules/api.md` | `internal/api/**` |
| `.claude/rules/testing.md` | `**/*_test.go`, `internal/sink/sinktest/**` |

Build order and resumable state: `blueprints/netflow-collector/tasks.json` and `epics/` (a local,
gitignored design bundle; the build it describes is complete).

## Non-negotiable

1. Never widen `Sink`, `Querier` or `Source` to make one backend easier. Two implementations exist
   precisely to stop that.
2. Never block a source on a slow sink, and never let the pipeline buffer grow past
   `NFC_PIPELINE_BUFFER`. Drop and count.
3. Never take `received_at` from the consumer's clock on the Kafka path — redelivery dedup depends
   on it coming from the message.
4. Never commit secrets, `.env`, or `bin/`. Never log a full API key.
5. Never hand-edit a migration that has already run; add a new one. Production migrations are
   forward-only, expand-then-contract.
6. Never mark a task done with a failing gate command, and never edit a `Verify` command to pass.
