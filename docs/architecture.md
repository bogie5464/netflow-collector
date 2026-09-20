# Architecture

One process, two pluggable boundaries. Everything else exists to keep those boundaries honest.

## The ingest path

```
   exporter (router, switch)              producer (JSON flow messages)
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
              ┌──────────────┐ ┌──────────────┐
              │ sink/postgres│ │ sink/mariadb │        implements flow.Sink + flow.Querier
              │ TimescaleDB  │ │ MariaDB      │        ON CONFLICT DO NOTHING / INSERT IGNORE
              └──────┬───────┘ └──────┬───────┘
                     └───────┬────────┘
                             ▼
                      GET /v1/flows  ──►  Grafana, curl, scripts
```

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
`DedupKey` to a 16-byte digest of the natural key. Both backends carry
`UNIQUE (received_at, dedup_key)` where NULLs are distinct, and both write with a conflict-tolerant
insert. A redelivered message therefore collides with itself exactly once; a UDP record never
collides with anything. No sink knows which source a record came from.

## Boundaries

Cross one the wrong way and the abstraction is gone. The compiler cannot enforce this table;
code review does.

| Package | May import | Must never import |
|---|---|---|
| `internal/flow` | stdlib only | anything in this module |
| `internal/pipeline` | `flow`, `obs` | any concrete source or sink |
| `internal/source/*` | `flow`, `obs`, `config` | `pipeline`, `api`, any sink |
| `internal/sink/*` | `flow`, `obs`, `migrations` | `pipeline`, `api`, any source |
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

Optional capabilities — such as listing exporters for `GET /v1/exporters` — are discovered by type
assertion (`api.ExporterLister`), so the contract never widens to make one backend easier.

## Where things live

| Concern | Single source of truth |
|---|---|
| Domain types and ports | `internal/flow/flow.go`, `internal/flow/ports.go` |
| Environment access | `internal/config/config.go` — validated once at boot |
| Schema | `migrations/postgres/*.sql`, `migrations/mariadb/*.sql`, embedded by `migrations/embed.go` |
| Storage conformance | `internal/sink/sinktest/conformance.go` |
| Metric names | `internal/obs/metrics.go` |
| Logging and redaction | `internal/obs/logging.go` |
| API envelope and parameters | `internal/api/server.go`, `internal/api/flows.go` → `docs/openapi.json` |
| Image tags, ports, credentials | `docker-compose.yml` |
| Build order and resumable state | `blueprints/netflow-collector/tasks.json` — local design bundle, not tracked |

## Process lifecycle

Boot: load and validate configuration (exit 2 on failure) → initialise logging, metrics and
optional tracing → construct backends and run every migration (exit 3 on failure, nothing bound
yet) → start the pipeline → bind the API and the sources. Shutdown on SIGINT/SIGTERM: sources stop,
the API drains, the pipeline drains its buffer and flushes the partial batch to every sink, backends
close, exit 0. The whole sequence is bounded by a 10 s shutdown budget.
