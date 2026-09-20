# Runbook

One section per alert. Every section ends in commands, not advice. Metric names are the strings
in `internal/obs/metrics.go`; all expressions run against `GET /metrics` on `NFC_HTTP_ADDR`.

Before anything else, know which build you are looking at and whether the process considers
itself ready:

```bash
curl -s http://127.0.0.1:8080/healthz          # 200: the process is alive
curl -s http://127.0.0.1:8080/readyz | jq      # 200: every backend answered; 503 names the one that did not
docker compose --profile app exec collector /usr/local/bin/collector -version
```

## netflow_records_dropped_total is climbing

**Alert:** `rate(netflow_records_dropped_total{reason="buffer_full"}[5m]) > 0` sustained for 5 min.

**Meaning:** sources are offering records faster than the writers drain the bounded buffer. The
pipeline discards rather than blocks — by design, so an exporter is never held up — and the drop
counter is the only externally visible symptom. `netflow_pipeline_buffer_length` climbing past
50 % of `NFC_PIPELINE_BUFFER` is the leading indicator; it fires before drops begin.

**Diagnose** — is it the writers or the input?

```bash
M=http://127.0.0.1:8080/metrics
curl -s $M | grep -E '^netflow_(pipeline_buffer_length|records_dropped_total|records_ingested_total)'
curl -s $M | grep -E '^netflow_batch_write_(duration_seconds_(sum|count)|errors_total)'
```

A high buffer with rising write duration or write errors means a sink is the bottleneck: go to
[a sink is falling behind](#a-sink-is-falling-behind). A high buffer with fast, error-free writes
means the input rate outruns the writer pool.

**Three levers**, in the order to try them:

```bash
# 1. more writer goroutines (each takes a whole batch to every sink in parallel)
NFC_WORKERS=8
# 2. larger batches — fewer, bigger writes per sink
NFC_BATCH_SIZE=5000
# 3. a deeper buffer — absorbs bursts, does not fix a sustained shortfall
NFC_PIPELINE_BUFFER=262144
docker compose --profile app up -d collector      # a restart is the whole deploy
```

If all three are exhausted the instance is saturated: stop pointing more exporters at it. Clustering
is out of scope for v1; the honest answer is a second instance with its own exporter set.

## a sink is falling behind

**Alert:** `histogram_quantile(0.95, rate(netflow_batch_write_duration_seconds_bucket[5m])) > 0.5`
for 10 min on any `sink` label, or `rate(netflow_batch_write_errors_total[5m]) > 0` for 5 min.

**Meaning:** one backend is slow or failing. Because batches fan out in parallel and errors are
collected per sink, the other backend keeps its data; the slow one degrades only its own metrics —
but a write error is a whole batch lost for that sink, not one record.

**Diagnose** the named sink:

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E '^netflow_batch_write_(duration_seconds_(sum|count)|errors_total)\{sink='
curl -s http://127.0.0.1:8080/readyz | jq .
docker compose logs --since 10m collector | grep -E '"level":"(ERROR|WARN)"' | grep -E 'batch write|readiness'
```

**Postgres/TimescaleDB:** check chunk count, bloat and connection saturation.

```bash
docker compose exec timescaledb psql -U netflow -d netflow -c \
  "SELECT count(*) AS chunks FROM timescaledb_information.chunks WHERE hypertable_name='flow_records';"
docker compose exec timescaledb psql -U netflow -d netflow -c \
  "SELECT state, count(*) FROM pg_stat_activity WHERE datname='netflow' GROUP BY state;"
```

**MariaDB:** `flow_records` is not partitioned in v1, so retention is a chunked `DELETE` and the
table grows until it runs. A long-running delete competes with inserts.

```bash
docker compose exec mariadb mariadb -unetflow -pnetflow netflow -e \
  "SELECT table_rows, data_length/1048576 AS data_mb, index_length/1048576 AS index_mb FROM information_schema.tables WHERE table_name='flow_records';"
docker compose exec mariadb mariadb -unetflow -pnetflow netflow -e "SHOW FULL PROCESSLIST;"
```

**Recover:** fix the backend, or remove it from `NFC_SINKS` temporarily and restart — the other
sink continues to receive every batch. Records that were lost to write errors on the failing sink
are not replayed; the UDP path is best-effort by design. Re-add the sink when it is healthy.

## ingest has stopped

**Alert:** `rate(netflow_records_ingested_total[5m]) == 0` for 5 min while `NFC_SOURCES` is
non-empty.

**Meaning:** nothing is arriving, or nothing is being decoded. Distinguish the two first.

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E '^netflow_(records_ingested_total|packets_decode_errors_total)'
docker compose logs --since 10m collector | grep -E '"level":"(ERROR|WARN)"'
```

- **Decode errors climbing, ingest flat:** datagrams arrive but do not decode. For NetFlow v9/IPFIX
  the usual cause is a missing template — templates are sent periodically by the exporter and are
  required before any data set decodes; a collector restarted mid-cycle waits until the next
  template refresh. Check the exporter's template refresh interval. See the goflow2 watch item below
  if the errors are against a new exporter model.
- **Both flat:** nothing is reaching the port. Verify the listener, the port mapping and the path
  from the exporter.

```bash
ss -ulnp | grep 2055                                   # host: is anything listening on the UDP port?
docker compose --profile app ps                        # container: is the collector healthy?
sudo tcpdump -ni any udp port 2055 -c 5                # are datagrams arriving at the host at all?
```

- **Kafka source:** confirm the group is consuming and the topic has messages.

```bash
docker compose exec redpanda rpk group describe netflow-collector -X brokers=127.0.0.1:19092
docker compose exec redpanda rpk topic describe flows -X brokers=127.0.0.1:19092
```

A source that fails at boot stops the process with a logged error; a source that dies later is
logged and the process exits, so an orchestrator restart is the normal recovery.

## query visibility lag is high

**Alert:** `histogram_quantile(0.95, rate(netflow_record_visibility_lag_seconds_bucket[5m])) > 120`
for 10 min.

**Meaning:** records are taking too long from arrival to queryable. The dominant term is
`NFC_BATCH_INTERVAL` (a partial batch waits at most this long); everything above that is write
latency. If write duration is also high, this is [a sink is falling behind](#a-sink-is-falling-behind).

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E '^netflow_record_visibility_lag_seconds_(sum|count)'
grep -E '^NFC_BATCH_(INTERVAL|SIZE)=' .env
```

## readyz is 503 while healthz is 200

**Meaning:** the process is alive but a backend did not answer a ping inside 2 s. This is exactly
the case the two endpoints exist to separate: do **not** restart the collector — it is buffering and
will resume on its own; restarting it drops the in-flight buffer.

```bash
curl -s http://127.0.0.1:8080/readyz | jq .error.message    # names the backend
docker compose ps timescaledb mariadb
docker compose logs --since 5m timescaledb mariadb | tail -50
```

## migrations failed at boot (exit 3)

**Meaning:** the process exited with code 3 before binding any listener. Migrations are
forward-only and idempotent; the fix is a new numbered migration, never an edit to one that has
already run, and never a `Down` in production.

```bash
docker compose logs collector | tail -20
docker compose exec timescaledb psql -U netflow -d netflow -c "SELECT * FROM goose_db_version ORDER BY id;"
docker compose exec mariadb mariadb -unetflow -pnetflow netflow -e "SELECT * FROM goose_db_version ORDER BY id;"
```

## MariaDB retention caveat

MariaDB's `RANGE` partitioning requires the partitioning column in every unique key, which would
force `dedup_key` into the partition expression and break the nullable-unique dedup semantics. So
in v1 `flow_records` on MariaDB is unpartitioned and retention is a bounded `DELETE` in chunks of
10,000 rows, run by the backend hourly and once at boot. On a large table the first run after a
long gap takes a while and competes with inserts; TimescaleDB drops whole chunks as a metadata
operation and is the recommended production default for that reason.

```bash
docker compose exec mariadb mariadb -unetflow -pnetflow netflow -e \
  "SELECT min(received_at), max(received_at), count(*) FROM flow_records;"
```

## goflow2 v2 versus v3 — operational watch item

The NetFlow/IPFIX decoder is `github.com/netsampler/goflow2/v2`, pinned to the tagged v2 line. At
the time of pinning, v2 had gone roughly nine months without a new tag while a v3 sat untagged in
the repository. Two consequences an operator should watch for:

1. **A new exporter model whose datagrams do not decode** — `netflow_packets_decode_errors_total`
   climbing only for that exporter — may be exercising a template or information-element edge that
   v3 fixed. Capture one datagram and open an issue upstream before changing anything here.
2. **When v3 is tagged**, treat the upgrade as a change to `internal/source/netflow/decode.go` only:
   the mapping onto `flow.FlowRecord` is the whole surface, and `netflow_test.go` builds its own v5
   and v9 datagrams, so the gate proves the mapping still holds.

```bash
go list -m -versions github.com/netsampler/goflow2/v2        # tagged versions the proxy knows
go list -m -json github.com/netsampler/goflow2/v2 | jq .Version
curl -s http://127.0.0.1:8080/metrics | grep '^netflow_packets_decode_errors_total'
```

## rotating an API key

Keys are configuration, not rows. Add the new key, restart, move clients, remove the old key,
restart. A 401 logs only the first six characters of the presented key, so a rejected client can be
correlated without the log becoming a credential store.

```bash
NEW=$(openssl rand -hex 32)
sed -i "s|^NFC_API_KEYS=.*|&,$NEW|" .env && docker compose --profile app up -d collector
# ...move clients to $NEW, then drop the old key from NFC_API_KEYS and restart again
```
