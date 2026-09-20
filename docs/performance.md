# Performance

Two numbers matter: how fast the in-memory pipeline moves records when nothing is in the way,
and whether every record is accounted for when something is. The first is a floor that catches a
pathological change; the second is the invariant the whole backpressure design rests on.

Both are measured by `internal/pipeline/load_test.go`, behind the `load` build tag, **without**
`-race` (the detector costs roughly an order of magnitude and would fail the floor on correct code):

```bash
go test -tags=load -run TestPipelineLoad ./internal/pipeline/...
```

CI runs it nightly; the whole-tree `go test -race ./...` gate runs on every push.

## Measured throughput

Measured on 2026-09-19 at tag `step-13-load-verification`, three consecutive runs.

**Throughput scenario** — 200,000 synthetic records offered by one producer to a pipeline whose
sink accepts instantly; wall-clock from the first `Offer` until the sink has received every
record.

| Run | Records | Wall-clock | Records/second | Drops |
|---|---|---|---|---|
| 1 | 200,000 | ~45 ms | 4,435,853 | 0 |
| 2 | 200,000 | ~45 ms | 4,410,628 | 0 |
| 3 | 200,000 | ~50 ms | 3,986,535 | 0 |

Floor asserted by the test: **20,000 records/second, 0 drops.** The measured rate is roughly 200×
the floor and roughly 200× the §1 success metric for a single instance (≥ 20,000 records/second
sustained). The pipeline is therefore not the bottleneck at any realistic exporter fleet; the
storage backends are, and their write latency is what `netflow_batch_write_duration_seconds`
watches.

**Conservation scenario** — 8 producers offer 400,000 records as fast as they can to a pipeline
with a 1,024-record buffer and a sink that sleeps 2 ms per 256-record batch.

| Run | Offered | Ingested | Dropped | Ingested + dropped | Max pending | Producers blocked |
|---|---|---|---|---|---|---|
| 1 | 400,000 | 10,496 | 389,504 | 400,000 | 1,024 of 1,024 | never (≈ 42 ms total) |
| 2 | 400,000 | 11,008 | 388,992 | 400,000 | 1,024 of 1,024 | never |
| 3 | 400,000 | 9,984 | 390,016 | 400,000 | 1,024 of 1,024 | never |

`ingested + dropped == offered` held exactly on every run, the buffer never exceeded its capacity,
and after shutdown every ingested record had reached the sink. This is the hardware-independent
result: a record is never lost uncounted, and a slow sink never blocks a source.

These figures are in-memory and say nothing about a database. End-to-end throughput is bounded by
the slowest configured sink; see `docs/runbook.md` (*a sink is falling behind*) for the levers.

## End-to-end throughput (UDP → decode → Postgres)

The in-memory numbers above answer "is the pipeline the bottleneck" (no). They don't answer "how
much traffic can one collector actually absorb," because that question is dominated by the sink,
not the pipeline. `internal/app/loadtest_e2e_test.go` (also behind the `load` tag) answers it
directly: a real UDP socket, real `goflow2` v5 decode, the real pipeline, and a real TimescaleDB
container via `testcontainers-go` — no mocks, no in-memory sink.

```bash
go test -tags=load -run TestEndToEndUDPThroughput -v ./internal/app/...
```

**Scenario:** one exporter, one v5 datagram per packet (24-byte header + one 48-byte flow record =
72 bytes), sent back-to-back over loopback for a fixed 10-second window at the shipped-default
pipeline settings (65,536-record buffer, 2,000-record batches, 1s batch interval, 4 workers). The
sender, the collector and the database container all share the same 8 CPUs.

Two tests, same wire path, different sink — so the collector's own ceiling and the database's are
separate numbers:

| | Collector only (`TestEndToEndUDPThroughputStubSink`) | Collector + TimescaleDB (`TestEndToEndUDPThroughput`) |
|---|---|---|
| Send rate offered | ~256,000 datagrams/s (~18.4 MB/s) | ~194,000 datagrams/s (~14 MB/s; the database now competes for the CPU) |
| Records ingested | **~242,000 records/s (~17.4 MB/s)** | **~56,000 records/s (~4.0 MB/s)** durably committed |
| Dropped (pipeline buffer full) | **0** | ~1.25M of ~1.94M sent |
| Unaccounted (kernel UDP socket loss) | ~145,000 (~6% of sent) | ~126,000 |
| Mean `WriteBatch`, 2,000-row batch, 4 workers | — | ~156 ms |
| Batch write errors | — | 0 |

**Read the two columns together.** The collector — UDP socket, `goflow2` decode, the bounded
pipeline, batching — absorbs ~94% of everything a single sender can push over loopback on this
machine with zero pipeline drops; what it misses is lost in the kernel's UDP receive buffer before
the process ever sees it (a `net.core.rmem_max` / `SO_RCVBUF` matter, not a collector one). Put a
real database behind the same path and the number is set by that database: on this machine, four
Postgres backend processes each pin a CPU core and the collector process idles at ~1.3 cores.

**Sustained throughput is set by the database host.** The write path is as cheap as the
measurements below could make it while keeping the dedup contract; the remaining headroom is in
how the database is sized and configured — a dedicated host, its core count (`NFC_WORKERS` ≈ the
cores available to Postgres), its memory and storage, and its settings. `docs/runbook.md`
(*a sink is falling behind*) lists what to look at and the order to try it in.

### Where the Postgres write time goes

`internal/sink/postgres/insert_bench_load_test.go` (also behind the `load` tag) times 2,000-row
batches on one connection with no UDP and no pipeline, so each lever is a number:

```bash
go test -tags=load -run TestInsertPathBenchmark -v ./internal/sink/postgres/...
```

| Insert path | ms / batch | rows / s (1 connection) |
|---|---|---|
| hypertable, `unnest` insert with `ON CONFLICT DO NOTHING` (the fallback path) | 99.7 | 20,061 |
| hypertable, **`COPY` (the fast path)** | 58.3 | 34,308 |
| hypertable, `COPY` into a temp table then `INSERT … SELECT … ON CONFLICT` | 96.7 | 20,672 |
| plain table (same columns and indexes), `unnest` | 58.7 | 34,094 |
| plain table, `COPY` | 23.3 | 85,714 |
| hypertable, `unnest`, both secondary indexes dropped | 86.9 | 23,019 |
| hypertable, `COPY`, both secondary indexes dropped | 52.7 | 37,942 |

What the table decided:

- **`WriteBatch` COPYs first and falls back on a unique violation.** `COPY` is ~1.7× the rows per
  CPU-second of the `unnest` insert on the hypertable, but it cannot carry `ON CONFLICT DO
  NOTHING`. So the sink COPYs the batch; if the `(received_at, dedup_key)` index rejects a row — a
  redelivered Kafka record already stored — Postgres discards the whole COPY and the sink retries
  that batch through the conflict-tolerant insert. The sink never inspects `DedupKey` or where a
  record came from: the database decides which path a batch takes, and the index still does the
  dedup work. UDP batches (`dedup_key` NULL, never conflicting) always take the fast path; the
  conformance suite proves a batch mixing new and redelivered records stores exactly the new ones.
- **The staged-COPY design was not worth building.** It measured the same as the plain insert
  (96.7 vs 99.7 ms): the cost was never parsing or binding, it is the `INSERT … ON CONFLICT`
  executor path on the hypertable.
- **The hypertable costs ~40 ms per batch on either path** (+60–150% over a plain table). That is
  TimescaleDB's chunk routing, and it buys chunk-drop retention and time partitioning; it stays.
- **Indexes are ~13% of the batch.** `idx_flow_records_received_seq` duplicated the primary key
  and is dropped by migration `00002`; the two that remain serve the API's exporter and source
  address filters.
- **`synchronous_commit=off` changed nothing** (measured, not assumed): one WAL flush per
  2,000-row batch is ~20 per second.

**How `NFC_WORKERS` behaves against this database:**

| Workers | Mean batch latency | Ingested | Write errors |
|---|---|---|---|
| 1 | 151 ms | ~19,000 records/s | 0 |
| 4 (default) | 205 ms | ~43,000 records/s | 0 |
| 8 | 448 ms | ~41,000 records/s | 0 |

(Measured before the COPY fast path; the shape, not the absolute numbers, is the point.) Four
workers is the knee on this hardware: latency per batch rises a little because the inserts
genuinely run concurrently, and total throughput more than doubles. Eight workers just queue on the
same 8 CPUs — latency doubles and throughput does not move. Re-measure on your database before
raising the default; the knee is a property of the database host, not the collector.

**What the first run found and fixed.** It measured only ~18,000 records/s with 4 workers — no
better than 1 — and logged a handful of `deadlock detected (SQLSTATE 40P01)` errors. Both symptoms
had one cause: each batch's transaction touched (or inserted) the exporter row to advance
`last_seen_at` and then held that row lock until the batch committed, so every worker writing for
the same exporter queued on it, and two of them occasionally took the exporter lock and the
flow-record unique index in opposite orders. The exporter resolution now runs as autocommit
statements *before* the record insert, in both backends. The row lock lasts one statement instead
of one batch, the deadlock cycle no longer exists, and `last_seen_at` is still advanced at most
once per batch per exporter.

Numbers here are single-exporter, loopback, one dev machine running the sender, the collector and
the TimescaleDB container side by side — a methodology and an order of magnitude, not a capacity
guarantee. Re-measure against production-like hardware, network path and exporter fan-out before
sizing a real fleet.

## Configuration

Values the load test uses, and what they correspond to in production:

| Scenario | `NFC_PIPELINE_BUFFER` | `NFC_BATCH_SIZE` | `NFC_BATCH_INTERVAL` | `NFC_WORKERS` | Sink |
|---|---|---|---|---|---|
| Throughput | 65,536 (the shipped default) | 2,000 (default) | 1s (default) | 4 (default) | in-memory, instant |
| Conservation | 1,024 (deliberately small) | 256 | 10 ms | 2 | in-memory, 2 ms sleep per batch |

The throughput scenario runs the shipped defaults so the number is representative of an
unconfigured deployment. The conservation scenario shrinks the buffer so overload is reached in
milliseconds rather than minutes; the invariant it proves does not depend on the size.

Build: `go test -tags=load`, no `-race`, `CGO_ENABLED` irrelevant (no cgo in the tree).

## Machine

| | |
|---|---|
| CPU | 12th Gen Intel Core i7-12800HX — 16 cores, 24 threads, 1 socket |
| Memory | 15 GB |
| OS / kernel | Linux 7.2.5-3-omarchy (x86_64) |
| Go | go1.27.1 linux/amd64 (pinned by the `toolchain` directive in `go.mod`) |
| Other load | Developer workstation with the compose stack (TimescaleDB, MariaDB, Redpanda, collector) running in containers during the measurement |

Re-measure on the target hardware before relying on any number here; the test prints its
measurement with `-v`, and the floor it asserts is intentionally far below what any current
machine achieves.
