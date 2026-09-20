# NetFlow Collector — agent instructions

A Go daemon that ingests NetFlow v5/v9/IPFIX and Kafka flow records into pluggable storage backends
and serves them over a small REST API.

## Commands

| Task | Command |
|---|---|
| Build | `go build -o bin/collector ./cmd/collector` |
| Vet | `go vet ./...` |
| Lint | `go tool golangci-lint run ./...` |
| Tests | `go test -race ./...` |
| Load test | `go test -tags=load -run TestPipelineLoad ./internal/pipeline/...` |
| Tidy check | `go mod tidy -diff` |
| Services up | `docker compose up -d --wait` |
| Full stack | `docker compose --profile app up -d --build --wait` |
| OpenAPI | `./bin/collector -dump-openapi > docs/openapi.json` |

**Gate:** `go vet ./... && go tool golangci-lint run ./... && go test -race ./...` must pass before
any task is marked done.

## Non-negotiable

1. Never widen `Sink`, `Querier` or `Source` to make one backend easier. Two implementations exist
   precisely to stop that, and both pass `internal/sink/sinktest/conformance.go` unmodified.
2. Never block a source on a slow sink, and never let the pipeline buffer grow past
   `NFC_PIPELINE_BUFFER`. Drop the record and increment the drop counter.
3. Never take `received_at` from the consumer's clock on the Kafka path — redelivery dedup depends
   on it coming from the message.
4. Never commit secrets, `.env`, or `bin/`. Never log a full API key.
5. Never hand-edit a migration that has already run; add a new one.
6. Never mark a task done with a failing gate command, and never edit a `Verify` command to pass.

Full architecture, import boundaries, code rules and the environment table: see `CLAUDE.md` in this
directory. Build order: `blueprints/netflow-collector/tasks.json` and `epics/`.
