# Performance

Four layers are measured separately, because each answers a different question:

| Layer | What it measures | Measured on the dev machine |
|---|---|---|
| **Collector, wire to sink** (`TestEndToEndUDPThroughputStubSink`) | Real UDP socket, real `goflow2` decode, the real pipeline, a sink that only counts | **~242,000 records/s, zero pipeline drops** |
| Collector + ClickHouse (`TestEndToEndUDPThroughputClickHouse`) | The same wire path into a real ClickHouse container | **~206,000 records/s durably committed, zero pipeline drops** |
| Collector + TimescaleDB (`TestEndToEndUDPThroughput`) | The same wire path into a real Postgres/TimescaleDB container | ~56,000 records/s durably committed |
| Pipeline alone (`TestPipelineLoad*`) | In-memory `Offer` to batch to an instant sink; no socket, no decode | ~4,400,000 records/s |

The first row is the collector's capacity: what this binary can take off the wire and hand to a
sink on this hardware. The next two are what a particular database on the same hardware could
absorb of it — and with ClickHouse the answer is "all of it": the pipeline never filled, so the
collector was the limit, not the database. The last row is a floor that catches a pathological
change in the pipeline itself and proves the `ingested + dropped == offered` invariant; it is not
a throughput claim, because nothing in it touches a socket or a decoder.

All of them are behind the `load` build tag and run **without** `-race` (the detector costs roughly
an order of magnitude and would fail the floors on correct code):

```bash
go test -tags=load -run TestEndToEndUDPThroughput -v ./internal/app/...   # all three end-to-end tests
go test -tags=load -run TestPipelineLoad ./internal/pipeline/...            # pipeline only
```

CI runs all of them nightly; the whole-tree `go test -race ./...` gate runs on every push.

## What ~242,000 flows/s means for a 10 Gbps link

Flow rate is a property of the traffic mix, not the link speed, so the useful relation is
arithmetic, not a lookup: 10 Gbps is 1.25 GB/s, and dividing by the average flow size gives the
unsampled flow rate.

| Average flow size | Unsampled flows/s per 10 Gbps | Collector alone (~242K/s) | + ClickHouse (~206K/s) | + this TimescaleDB (~56K/s) |
|---|---|---|---|---|
| 100 KB (bulk transfers, backups, video) | ~12,500 | ~19 links | ~16 links | ~4 links |
| 50 KB (typical enterprise / Internet mix) | ~25,000 | ~10 links | ~8 links | ~2 links |
| 10 KB (chatty, many short sessions) | ~125,000 | ~2 links | ~1.6 links | 0.4 links |
| 1 KB (DNS-heavy, scans, DDoS) | ~1,250,000 | sample | sample | sample |

Read it as: one instance of the collector on an 8-CPU box comfortably takes several unsampled
10 Gbps links of ordinary traffic, and the database decides how many of those it will keep —
ClickHouse keeps nearly all of them, TimescaleDB about a quarter. Traffic in the bottom row is
why exporters sample; at 1:1000 it becomes ~1,250 flows/s and is not a capacity question at all.

Two caveats on the measurement side. The test sends one flow record per datagram (72 bytes);
real exporters pack up to 30 v5 records into a 1,464-byte datagram, so the collector sees far
fewer packets per record in production than in this test and the ~242,000 figure is conservative
on the packet path. And the ~6% the collector missed in that run was lost in the kernel's UDP
receive buffer before the process saw it — `net.core.rmem_max` / `SO_RCVBUF` territory, which the
test leaves at the distribution default. Measure the real rate on a real link with
`rate(netflow_records_ingested_total[5m])` before sizing from this table.

## Pipeline-only measurements

Measured on 2026-09-19 at tag `step-13-load-verification`, three consecutive runs, by
`internal/pipeline/load_test.go`.

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
directly: a real UDP socket, real `goflow2` v5 decode, the real pipeline, and a real database
container via `testcontainers-go` — no mocks, no in-memory sink. The Postgres path is analysed in
detail below; the ClickHouse result and how it was tuned are in the section after it.

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

## ClickHouse

Same test, same machine, same pipeline settings, ClickHouse as the only sink
(`TestEndToEndUDPThroughputClickHouse`). Three write configurations were measured before choosing
the default, because ClickHouse's async-insert feature looks made for this workload and is not:

| Insert mode (set via `NFC_CLICKHOUSE_DSN` query parameters) | Mean `WriteBatch` | Ingested | Pipeline drops |
|---|---|---|---|
| **Synchronous (the default — no parameters)** | **14.4 ms** | **~206,000 records/s** | **0** |
| `async_insert=1&wait_for_async_insert=1` (server default ~200 ms flush timer) | 285 ms | ~35,000 records/s | ~2.3M |
| same, `async_insert_busy_timeout_ms=10` | 49 ms | ~166,000 records/s | ~550K |
| `async_insert=1&wait_for_async_insert=0` | — | — | migrations fail: goose reads its version table back before the async write is visible and panics (`goose/v3` 3.28.0, `up.go:70`) |

Async inserts exist so that *many small clients* can share one on-disk part. This collector is
four large clients; waiting for the server's flush timer just adds the timer to every batch, and
with four workers that is the whole throughput budget. So the sink forces no settings, a
`WriteBatch` is one synchronous `INSERT`, and any query parameter the driver does not recognise is
passed through as a session setting for operators who want something else.

One synchronous `INSERT` is one on-disk part. At this test's 2,000-record batches that was ~100
parts per second — fine for ten seconds, not for a week, because merges have to keep up. The
lever is the collector's, not the database's: `NFC_BATCH_SIZE=20000` (with the default 1 s
`NFC_BATCH_INTERVAL` bounding latency) turns the same 200K records/s into ~10 inserts per second,
which is the shape ClickHouse wants. `docs/runbook.md` has the parts and merges queries to watch.

**What did not need doing.** No `COPY` equivalent, no index tuning, no commit-mode experiment:
ClickHouse's native batch insert at 14 ms per 2,000 rows is already ~14× cheaper per row than the
Postgres path at its best, which is the difference between a row store maintaining four B-trees
per insert and a column store appending sorted blocks. The dedup contract cost nothing at write
time either — `ReplacingMergeTree` collapses redeliveries at merge time and reads use `FINAL`.

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
