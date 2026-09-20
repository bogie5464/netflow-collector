---
description: The two pluggable boundaries — Source, Sink, Querier — and the rules that keep them pluggable
paths:
  - "internal/flow/**"
  - "internal/source/**"
  - "internal/sink/**"
---

# Flow contracts

- `internal/flow` imports **nothing** from this module. Anything it imports becomes part of the
  contract every adapter must accept.
- `ports.go` must never name `net.Conn`, `net.Addr` or `net.PacketConn`. A database-import or
  file-replay `Source` has to satisfy the interface without pretending to be a socket. There is a
  build gate on this grep.
- `Sink` is `WriteBatch(ctx, []FlowRecord) error` and nothing more. No SQL string, no query builder,
  no `*sql.DB`, no dialect flag. The moment one leaks through, MariaDB and every future backend are
  implementing a Postgres API.
- `Querier` is `Query(ctx, FlowQuery) (FlowResultPage, error)`. `FlowQuery` carries a bounded time
  range, the whitelisted filters, a limit and an opaque cursor — it is not an expression tree and it
  is not a query language.
- Accept interfaces, return structs. A backend constructor returns `flow.Backend`; everything inside
  the package is unexported.
- `context.Context` is the first parameter of every I/O function, and is never stored in a struct.
- Errors wrap with `%w` and are inspected with `errors.Is`/`errors.As`. Never compare error strings.
- Returning a typed nil pointer as an `error` makes `err != nil` true. Return a bare `nil`.
- **Truncate every timestamp to microseconds before writing.** Postgres `timestamptz` and MariaDB
  `DATETIME(6)` store microseconds; Go carries nanoseconds. Without truncation the conformance
  suite's round-trip equality fails on a correct implementation.
- `DedupKey` semantics are load-bearing: `nil` means append-only (the UDP path), non-nil means the
  transport can redeliver (the Kafka path). Every sink writes conflict-tolerantly and branches on
  neither — the nullable unique index does the work.
- `ReceivedAt` comes from the record on any transport that can redeliver, and from the collector's
  clock only on a live, non-redelivering wire protocol.
- A sink resolves `ExporterAddr` to `exporter_id` through its own bounded cache. `last_seen_at` is
  advanced at most once per batch per exporter, never once per record.
- Soft cap 300 lines per file. Split by responsibility, not by type.
