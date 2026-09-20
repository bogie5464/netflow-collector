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
