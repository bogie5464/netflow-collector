# Architecture

One process, two pluggable boundaries. Everything else exists to keep those boundaries honest.

## The ingest path

```
   exporter (router, switch)              another collector's kafka sink (internal/wire JSON)
          │ UDP datagram                          │ Kafka message
          ▼                                       ▼
   ┌───────────────────┐               ┌────────────────────┐
   │ source/netflow    │               │ source/kafka       │      implements flow.Source
   │ v5 / v9 / IPFIX   │               │ received_at from   │      Start(ctx, out chan<- FlowRecord)
   │ DedupKey = nil    │               │ message, DedupKey  │
   └─────────┬─────────┘               └──────────┬─────────┘
             │  flow.FlowRecord                    │
             └────────────────┬────────────────────┘
                              ▼
                 ┌──────────────────────────┐
                 │  Offer(): non-blocking    │   full → drop + count
                 │  ┌─────────────────────┐ │   netflow_records_dropped_total{reason="buffer_full"}
                 │  │ bounded channel     │ │   never blocks a source, never grows
                 │  │ cap NFC_PIPELINE_BUFFER │
                 │  └──────────┬──────────┘ │
                 │             ▼            │
                 │  batcher: NFC_BATCH_SIZE │
                 │      or NFC_BATCH_INTERVAL,│
                 │      whichever first     │
                 │             ▼            │
                 │  worker pool (NFC_WORKERS)│
                 └──────┬─────────────┬─────┘
                        │ WriteBatch  │ WriteBatch     fan-out in parallel, errors per sink
                        ▼             ▼
       ┌──────────────┐ ┌──────────────┐ ┌────────────────┐ ┌──────────────┐
       │ sink/postgres│ │ sink/mariadb │ │ sink/clickhouse│ │ sink/kafka   │
       │ TimescaleDB  │ │ MariaDB      │ │ ReplacingMT    │ │ produces     │  flow.Sink; the first
       │ COPY/ON CONF.│ │ INSERT IGNORE│ │ + FINAL        │ │ wire JSON    │  three are also
       └──────┬───────┘ └──────┬───────┘ └───────┬────────┘ └──────┬───────┘  flow.Backend
              └───────────────┴─────────┬────────┘                  │
                                        ▼                           ▼
                                 GET /v1/flows                another collector's
                            ──►  Grafana, curl, scripts       source/kafka (central tier)
```

The kafka sink and the kafka source speak the same format (`internal/wire`), which is what makes
one binary both halves of a tiered deployment: an edge tier decodes UDP and produces to a topic; a
central tier consumes the topic and writes to storage. Edge instances may also share v9/IPFIX
templates through a second, compacted topic (`netflow.TemplateStore`, implemented by
`internal/templatestore`), so any instance can decode any exporter. `docs/deploy.md` has the
layout and the Kubernetes manifests.

Source → bounded channel → batcher → worker pool → Sink fan-out. The invariant with a test:
`ingested + dropped == offered`.

## The query path

`GET /v1/flows` → request-id middleware → API key middleware (`/v1` subtree only) → the request
struct in `internal/api/flows.go` bound and validated → `flow.Querier.Query` → the backend's keyset
query `(received_at, seq) < (t, s) ORDER BY received_at DESC, seq DESC` → the envelope
`{"data": [...], "meta": {"next_cursor", "has_more"}}`. The cursor is opaque; the client never
constructs one.

## Deduplication without a branch in any sink

NetFlow over UDP is best-effort and never redelivers, so the UDP source leaves `DedupKey` nil.
Kafka redelivers, so the Kafka source sets `ReceivedAt` from the message (never its own clock) and
`DedupKey` to a 16-byte digest of the natural key. Every storage backend gives
`UNIQUE (received_at, dedup_key)` with NULLs distinct — as an index on Postgres and MariaDB, as a
`ReplacingMergeTree` sorting key on ClickHouse — and writes conflict-tolerantly. A redelivered
message therefore collides with itself exactly once; a UDP record never collides with anything. No
sink knows which source a record came from. The kafka sink does not carry the key at all: it is the
consuming transport's property, and the kafka source derives it on the way in.

## Boundaries

Cross one the wrong way and the abstraction is gone. The compiler cannot enforce this table;
code review does.

| Package | May import | Must never import |
|---|---|---|
| `internal/flow` | stdlib only | anything in this module |
| `internal/pipeline` | `flow`, `obs` | any concrete source or sink |
| `internal/wire` | `flow` | anything else — it is the collector's own message format, shared by a source and a sink |
| `internal/source/*` | `flow`, `obs`, `config`, `wire` | `pipeline`, `api`, any sink |
| `internal/sink/*` | `flow`, `obs`, `migrations`, `wire` | `pipeline`, `api`, any source |
| `internal/api` | `flow`, `obs`, `config` | any concrete backend package |
| `internal/app` | everything | — it is the only wiring layer |

Three rules follow from it:

1. **`internal/flow` is the contract.** `ports.go` holds `Source`, `Sink`, `Querier` and `Backend`
   and names no network type, so a file-replay or database-import source can satisfy it.
2. **`Sink` stays batch-shaped.** `WriteBatch(ctx, []FlowRecord) error` and nothing more. The
   moment SQL, a query builder or a `*sql.DB` leaks through, every other backend is implementing a
   Postgres API.
3. **A backend passes `sinktest.Conformance` unmodified.** If the suite needs a branch for an
   engine, the interface leaked; fix the backend or the interface, never the suite.

Optional capabilities are discovered by type assertion, so the contract never widens to make one
sink easier: `flow.ExporterLister` serves `GET /v1/exporters`, `flow.Pinger` answers `/readyz` for
a sink that cannot be queried, and `flow.Backend` itself is an assertion the app makes on each
`NFC_SINKS` entry — a storage engine gets migrated and queried, a transport like the kafka sink
only gets written to.

## Where things live

| Concern | Single source of truth |
|---|---|
| Domain types and ports | `internal/flow/flow.go`, `internal/flow/ports.go` |
| Environment access | `internal/config/config.go` — validated once at boot |
| Schema | `migrations/postgres/*.sql`, `migrations/mariadb/*.sql`, `migrations/clickhouse/*.sql`, embedded by `migrations/embed.go` |
| Kafka message format | `internal/wire/wire.go` |
| Storage conformance | `internal/sink/sinktest/conformance.go` |
| Metric names | `internal/obs/metrics.go` |
| Logging and redaction | `internal/obs/logging.go` |
| API envelope and parameters | `internal/api/server.go`, `internal/api/flows.go` → `docs/openapi.json` |
| Image tags, ports, credentials | `docker-compose.yml` |
| Build order and resumable state | `blueprints/netflow-collector/tasks.json` — local design bundle, not tracked |

## Process lifecycle

Boot: load and validate configuration (exit 2 on failure) → initialise logging and metrics →
construct backends and run every migration (exit 3 on failure, nothing bound yet) → start the
pipeline → bind the API and the sources. Shutdown on SIGINT/SIGTERM: sources stop,
the API drains, the pipeline drains its buffer and flushes the partial batch to every sink, backends
close, exit 0. The whole sequence is bounded by a 10 s shutdown budget.
