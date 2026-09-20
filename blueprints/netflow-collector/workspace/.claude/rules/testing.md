---
description: Test conventions — require over assert, real databases, the conformance suite, and building fixtures instead of embedding blobs
paths:
  - "**/*_test.go"
  - "internal/sink/sinktest/**"
---

# Testing conventions

- **`testify/require`, never `testify/assert`.** `assert` records the failure and keeps executing,
  so a failed nil check is followed by a panic that hides the real error. `require` stops at the
  failure. This is a hard rule from the Go runtime track.
- **Use a real database, never a mock.** Postgres and MariaDB tests start a pinned container through
  `testcontainers-go` with an explicit `wait.ForSQL` strategy — a log-scraping wait strategy is
  engine-specific and flakes. Mocked database tests assert that the mock matches the mock.
- The Kafka test uses the **compose** `redpanda` service (`docker compose up -d --wait redpanda`)
  rather than a container per test, because a broker must advertise an address that is known before
  it starts. The test retries its first metadata fetch for up to 30 s.
- **Image tags are owned by `docker-compose.yml`.** Copy the literal tag into a test file; never the
  reverse. Two build gates grep both files for the same string.
- **`internal/sink/sinktest/conformance.go` is the contract, not a helper.** Every backend calls it
  with a factory and passes it **unmodified**. If it needs a branch for your engine, the interface
  leaked — fix the interface or the backend, never the suite. A build gate diffs this file against
  its committed state.
- **Build fixtures, do not embed blobs.** The NetFlow test constructs its own v5 datagram field by
  field with `encoding/binary` from the documented layout, then asserts the decoder returns what the
  test encoded. A pasted hex capture is unreadable, unverifiable, and usually wrong.
- **Truncate fixture timestamps to microseconds** (`time.Now().UTC().Truncate(time.Microsecond)`)
  before writing them. Both engines store microseconds; Go carries nanoseconds, and the difference
  fails round-trip equality on correct code.
- Table-driven subtests with `t.Run`. Name the subtest after the behaviour —
  `rejects a datagram shorter than the v5 header` — so a failure reads like a bug report.
- **Reset state per test.** Each conformance run gets its own container or its own truncated tables;
  no test may depend on another's rows or on execution order.
- Tests run with `-race` everywhere **except** the load test: the race detector costs roughly an
  order of magnitude and would fail the throughput floor. The load test is behind the `load` build
  tag and runs without `-race`.
- `t.Setenv` is the only way a test sets configuration. Never mutate the process environment
  directly, and never depend on a `.env` value that is not in `.env.example`.
- Every `Verify` command must exit 0 when the code is correct. A check whose success is a non-zero
  exit is wrapped so the assertion decides the status — `cmd; test $? -eq 2`, not a bare `cmd`.
- A flaky test is a broken test. Quarantine it the day it flakes; one tolerated flake teaches
  everyone to ignore red.
