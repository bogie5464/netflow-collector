# NetFlow Collector

[![CI](https://github.com/bogie5464/netflow-collector/actions/workflows/ci.yml/badge.svg)](https://github.com/bogie5464/netflow-collector/actions/workflows/ci.yml)
[![Coverage](https://raw.githubusercontent.com/bogie5464/netflow-collector/main/.github/badges/coverage.svg)](https://github.com/bogie5464/netflow-collector/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A single Go daemon that ingests NetFlow v5/v9/IPFIX datagrams and Kafka flow messages, stores
them in pluggable storage backends, and serves them over a small, authenticated REST API.

**No vendor lock-in.** Every storage engine implements the same `flow.Backend` contract and passes
the same conformance suite, so PostgreSQL/TimescaleDB and MariaDB are equally first-class today,
and a third backend — ClickHouse, SQLite, S3/Parquet, whatever your fleet already runs — is a
documented procedure (`docs/extending.md`), not a rewrite. Switching backends, or running two of
them side by side, never touches the ingest path, the API, or anything upstream of `flow.Sink`.

- **Ingest:** UDP NetFlow/IPFIX (`goflow2` decoding) and Kafka (`franz-go`), through a bounded
  pipeline that drops-and-counts under backpressure instead of blocking an exporter.
- **Storage:** PostgreSQL/TimescaleDB (recommended) and MariaDB, behind one `flow.Backend`
  contract that both implementations prove with the same conformance suite.
- **Query:** `GET /v1/flows` with a mandatory time range, whitelisted filters and keyset cursor
  pagination; `GET /v1/exporters`; `/healthz`, `/readyz`, `/metrics` for operations.

Anyone who needs to store and query flow data can run this — it started as tooling for network
engineers and SREs, and that's still the sharpest use case, but there's nothing network-specific
about the storage, query or ops layers that would stop anyone else from using it.

## Quick start

Three commands take a fresh checkout to a running collector. Go 1.27.1 (pinned in `go.mod`) and
Docker with Compose v2 are the only prerequisites.

```bash
cp .env.example .env                              # every NFC_* variable with a working local value
docker compose up -d --wait                       # TimescaleDB :15432, MariaDB :13306, Redpanda :19092
go build -o bin/collector ./cmd/collector && ./bin/collector
```

The collector listens for NetFlow on `:2055/udp` and serves the API on `:8080`. Send it a
datagram from an exporter and query it back:

```bash
curl -s -H 'Authorization: Bearer dev-local-key' \
  'http://127.0.0.1:8080/v1/flows?start=2026-01-01T00:00:00Z&end=2027-01-01T00:00:00Z&limit=10' | jq
```

The full stack, collector included, runs as containers with
`docker compose --profile app up -d --build --wait`.

## Everyday commands

| Task | Command |
|---|---|
| Build | `go build -o bin/collector ./cmd/collector` |
| Validate configuration | `./bin/collector -validate-config` — exit 2 names the offending variable |
| Version | `./bin/collector -version` |
| Vet · lint | `go vet ./...` · `go tool golangci-lint run ./...` |
| Tests (needs Docker) | `go test -race ./...` |
| Load test | `go test -tags=load -run TestPipelineLoad ./internal/pipeline/...` (no `-race`) |
| OpenAPI document | `./bin/collector -dump-openapi > docs/openapi.json` |
| Services up · down | `docker compose up -d --wait` · `docker compose down` |

The gate every change must pass: `go vet ./... && go tool golangci-lint run ./... && go test -race ./...`.

## Configuration

Every setting is an `NFC_*` environment variable, documented with a working local value in
`.env.example`. The binary loads the nearest `.env` by walking up from the working directory and
never overwrites a variable already exported in the shell. Validation happens once at boot; an
invalid value exits with code 2 and names the variable.

| Variable | Meaning |
|---|---|
| `NFC_SOURCES` | Comma-separated inputs: `netflow`, `kafka` |
| `NFC_SINKS` | Comma-separated backends: `postgres`, `mariadb` — batches fan out to all of them |
| `NFC_NETFLOW_ADDR` · `NFC_HTTP_ADDR` | UDP listen address · API listen address |
| `NFC_POSTGRES_DSN` · `NFC_MARIADB_DSN` | Required only when the backend is in `NFC_SINKS`; the MariaDB DSN must carry `parseTime=true&loc=UTC` |
| `NFC_KAFKA_BROKERS` · `_TOPIC` · `_GROUP` | Required only when `kafka` is in `NFC_SOURCES` |
| `NFC_API_KEYS` | Comma-separated bearer keys for `/v1`. Generate with `openssl rand -hex 32`. Empty while the API is enabled is a boot error, never "no key required" |
| `NFC_PIPELINE_BUFFER` · `NFC_BATCH_SIZE` · `NFC_BATCH_INTERVAL` · `NFC_WORKERS` | Backpressure and batching (defaults 65536 · 2000 · 1s · 4) |
| `NFC_RETENTION_DAYS` | Age after which flow records are dropped (default 30) |
| `NFC_LOG_LEVEL` · `NFC_OTEL_ENABLED` · `NFC_OTEL_ENDPOINT` | Logging level; optional OTLP tracing |

## Storage backends

| Backend | Driver | Partitioning | Retention | Notes |
|---|---|---|---|---|
| `postgres` (TimescaleDB) | `pgx/v5` | Hypertable on `received_at` | Timescale retention policy (chunk drop) | Recommended production default |
| `mariadb` | `database/sql` + `go-sql-driver/mysql` | None in v1 | Chunked `DELETE` on an hourly ticker | See the retention caveat in `docs/runbook.md` |

Both pass `internal/sink/sinktest.Conformance` unmodified. Adding a third is a documented
procedure: `docs/extending.md`.

## Process exit codes

| Code | Meaning |
|---|---|
| 0 | Clean shutdown, or a successful `-version` / `-validate-config` / `-dump-openapi` / `-healthcheck` |
| 1 | Unexpected runtime error |
| 2 | Invalid configuration — the message names the variable |
| 3 | Storage boot failure (migration or connection) — nothing was bound |

## Renaming the module

The module path is `github.com/bogie5464/netflow-collector`. To adopt the code under your own
organisation, rename the module and rewrite the import prefix in one pass:

```bash
OLD=github.com/bogie5464/netflow-collector
NEW=github.com/<org>/netflow-collector
go mod edit -module "$NEW"
grep -rl --include='*.go' "$OLD" cmd internal migrations | xargs sed -i "s|$OLD|$NEW|g"
go build ./... && go test -race ./...
```

## Documentation

- `docs/architecture.md` — the pipeline, the package boundaries, and why they are drawn that way.
- `docs/extending.md` — adding a source or a storage backend.
- `docs/runbook.md` — what to do when an alert fires.
- `docs/deploy.md` — image build, compose, release and rollback.
- `docs/openapi.json` — generated API document; never hand-edited.

## License

MIT — see [`LICENSE`](LICENSE). The Architect design bundle this project was built from
(`blueprints/netflow-collector/`) is kept out of the repository; it is a local design artefact.
