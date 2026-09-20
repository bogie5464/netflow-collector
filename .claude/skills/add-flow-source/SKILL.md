---
name: add-flow-source
description: Use when adding a new input adapter to NetFlow Collector — sFlow, a database-import replay, a file replay, gRPC, or any other producer of flow records that should implement flow.Source. Triggers on "add a source", "ingest from <thing>", "read flows from <thing>", "new input adapter", "replay historical flows".
---

# Add a flow source

## When to use

Flow records need to reach the pipeline from somewhere new: another protocol, a message bus, a file,
or a historical store being replayed. The database-import adapter named as a v1 non-goal in
`blueprint.md` §1 is exactly this procedure.

## Steps

1. **Read the contract.** `internal/flow/ports.go`:

   ```go
   type Source interface { Start(ctx context.Context, out chan<- FlowRecord) error }
   ```

   That is the whole interface, and it deliberately names no network type. Your adapter owns
   everything about its wire format, its connection, and its own goroutines.

2. **Create the package.** `internal/source/<name>/<name>.go`. Export
   `New(cfg config.Config) (flow.Source, error)`. `Start` blocks until `ctx` is cancelled, then
   releases its resources and returns `nil` — a non-nil return means the source failed, and
   `internal/app` will log it and shut the process down.

3. **Never block on `out`, and never drop silently.** Send with a `select` on `ctx.Done()` so
   cancellation is always honoured. Back-pressure is the pipeline's job: it owns the bounded channel
   and the drop counter. Your source's only obligation is to stop promptly.

4. **Set the two fields that decide semantics.**
   - `ReceivedAt` — for a live wire protocol, the moment you accepted the datagram. For a replay or
     a bus with redelivery, the timestamp carried **in the record**, never your own clock.
   - `DedupKey` — leave it `nil` for an append-only source with no redelivery (the UDP path). Set it
     to a 16-byte digest of `(exporter_addr, first_switched, src_addr, dst_addr, src_port, dst_port,
     protocol)` when the transport can deliver the same record twice. Those two fields together are
     what make one table serve both semantics without a branch in any sink.

5. **Count your own decode failures.** `obs.DecodeErrors.WithLabelValues("<name>").Inc()` —
   `netflow_packets_decode_errors_total{source="<name>"}` from `internal/obs/metrics.go`. A
   malformed input increments it, is skipped, and never stops the loop.

6. **Test without the network where you can.** Construct the input programmatically — the NetFlow
   test builds its own v5 datagram from the documented field layout rather than embedding a captured
   hex blob, which is why it still reads as a specification. If a broker or a server is genuinely
   needed, pin its image in `docker-compose.yml` and start it with `docker compose up -d --wait`.

7. **Wire it up.** Add the source's name to the `NFC_SOURCES` switch in `internal/app/run.go`, add
   any new variables to `.env.example` and to `internal/config/config.go` — required **only** when
   your source is enabled — and add a `## Adding a Source` entry to `docs/extending.md`.

## Verify

```bash
go build ./...                                        # expect: exit 0
go test -race ./internal/source/<name>/...            # expect: exit 0, ok, 0 failures
test -f internal/flow/ports.go                        # expect: exit 0 — guarantees the grep below can read it
! grep -qE '\bnet\.(Conn|Addr|PacketConn)\b' internal/flow/ports.go   # expect: no match → exit 0
go test -race ./internal/source/... ./internal/pipeline/...           # expect: exit 0
go tool golangci-lint run ./...                       # expect: exit 0
```

## Do not

- **Do not change the `Source` signature.** Putting a `net.Addr`, a `net.Conn`, or a decoded-packet
  type into it is the one change that makes a future file or database replay impossible, and the
  build order gates on that grep.
- **Do not do work on the pipeline's behalf** — no batching, no retry, no fan-out, no writing to a
  sink. A source produces `FlowRecord` values and nothing else.
- **Do not use your own clock for `ReceivedAt` on a redelivering transport.** It silently defeats
  `(received_at, dedup_key)` uniqueness and duplicates every replayed record.
- **Do not swallow a decode error.** Count it, skip it, keep going.
