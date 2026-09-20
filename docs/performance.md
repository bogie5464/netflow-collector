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
pipeline settings (65,536-record buffer, 2,000-record batches, 1s batch interval, 4 workers).

| Metric | Value |
|---|---|
| Send rate offered | ~200,000–260,000 datagrams/s (~15–19 MB/s; the sender shares the CPU with the collector and the database) |
| Records durably ingested (committed to Postgres) | **~43,000 records/s (~3.1 MB/s)** |
| Mean `WriteBatch` latency, 2,000-row batch, 4 workers | ~205 ms |
| Dropped (pipeline buffer full) | ~1.5M of ~2.0M sent |
| Batch write errors | 0 |

The gap between "offered" and "ingested" is the real finding: **on this machine, Postgres write
throughput — not the pipeline, not decode, not the kernel UDP path — is the ceiling.** A single
exporter flooding at line rate saturates the pipeline buffer almost immediately because each
2,000-record batch is one round trip to the database. A slower or busier database caps out lower,
a faster one higher. This is exactly what `docs/runbook.md`'s "a sink is falling behind" playbook
(widen `NFC_WORKERS`, tune batch size, add a second sink, or move to bigger iron) exists for.

**How `NFC_WORKERS` behaves against this database:**

| Workers | Mean batch latency | Ingested | Write errors |
|---|---|---|---|
| 1 | 151 ms | ~19,000 records/s | 0 |
| 4 (default) | 205 ms | ~43,000 records/s | 0 |
| 8 | 448 ms | ~41,000 records/s | 0 |

Four workers is the knee on this hardware: latency per batch rises a little because the inserts
genuinely run concurrently, and total throughput more than doubles. Eight workers just queue on the
same database — latency doubles and throughput does not move. Re-measure on your database before
raising the default; the knee is a property of the database, not the collector.

**What this test found and fixed.** The first run measured only ~18,000 records/s with 4 workers —
no better than 1 worker — and logged a handful of `deadlock detected (SQLSTATE 40P01)` errors.
Both symptoms had one cause: each batch's transaction first touched (or inserted) the exporter row
to advance `last_seen_at`, and then held that row lock until the batch committed. With every
worker writing for the same exporter, the workers were serialised on that lock, and two of them
occasionally took the exporter lock and the flow-record unique index in opposite orders, which
Postgres's deadlock detector resolved by aborting one. The exporter resolution now runs as
autocommit statements *before* the record insert, in both `internal/sink/postgres` and
`internal/sink/mariadb`. The row lock lasts one statement instead of one batch, the deadlock cycle
no longer exists, and `last_seen_at` is still advanced at most once per batch per exporter. The
same run went from ~18,000 to ~43,000 records/s with zero write errors.

An earlier, narrower fix — not publishing a brand-new exporter's id to the in-process cache until
its transaction had committed, so a rolled-back insert could not poison the cache — is subsumed by
this one: an autocommitted upsert is durable by the time it returns, so its id is safe to cache
immediately.

Numbers here are single-exporter, loopback, one dev machine running the collector and the
TimescaleDB container side by side — they are a methodology and a rough order of magnitude, not a
capacity guarantee for any specific deployment. Re-measure against production-like hardware,
network path and exporter fan-out before sizing a real fleet.

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
