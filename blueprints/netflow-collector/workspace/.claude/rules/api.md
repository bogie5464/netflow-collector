---
description: REST envelope, status codes, validation, cursor pagination and auth conventions for the query API
paths:
  - "internal/api/**"
---

# API conventions

- **One envelope, every endpoint, including errors.**
  Success: `{"data": ..., "meta": {"next_cursor": ..., "has_more": ...}}`.
  Error: `{"error": {"code", "message", "details", "request_id"}}`.
  A 200 carrying an error inside is never acceptable — it defeats every client, retry policy and
  monitor in the chain.
- `code` is a stable machine string from the enumerated set: `validation_error` (422),
  `bad_request` (400), `unauthorized` (401), `not_found` (404), `method_not_allowed` (405),
  `backend_unavailable` (503), `internal_error` (500). `message` is human-facing and may change.
- Every response carries `X-Request-Id`; every error body repeats it as `error.request_id`. The same
  id appears on every log line for that request.
- **Validate at the boundary.** Query parameters bind to a request struct with `validate` tags and
  are parsed before any handler logic. The handler receives a typed, trusted value or the request
  never reaches it.
- **The request struct is the only description of the parameters.** The OpenAPI builder reflects over
  the same struct, so the checked-in `docs/openapi.json` cannot drift from the handler. Never
  hand-maintain the document; regenerate it with `./bin/collector -dump-openapi` in the same commit
  as the change.
- An **unknown query parameter is a 422** naming it. Silently ignoring a typo'd filter returns a
  wrong answer that looks right, which on a traffic-analysis tool is worse than an error.
- `start` and `end` are **required** on `/v1/flows`. There is no unbounded scan of `flow_records`
  from the API, ever.
- `limit` defaults to 100 and is **clamped** to 1000 server-side — a client asking for more gets
  1000, not an error.
- Pagination is **keyset**, never offset. The cursor is an opaque base64url encoding of
  `{"t": <received_at unix nanos>, "s": <seq>}`; the predicate is `(received_at, seq) < (t, s)` with
  `ORDER BY received_at DESC, seq DESC`. Clients never construct a cursor.
- Never leak a backend error to the client. Log it at `error` with the request id and return
  `backend_unavailable`.
- Auth is middleware on the `/v1/` subtree only. `/healthz`, `/readyz` and `/metrics` are registered
  outside it and stay public — the thing scraping them has no credential story.
- Key comparison uses `crypto/subtle.ConstantTimeCompare` against every configured key. A 401 logs
  at `warn` with the **first six characters** of the presented key and never the whole value.
- `/healthz` (process is alive) and `/readyz` (backends answered) are separate endpoints with
  different meanings. Conflating them turns a database blip into a restart loop.
- The API depends on `flow.Querier`. It must never import a concrete backend package.
