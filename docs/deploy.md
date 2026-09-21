# Deployment

The collector is a single static binary in a distroless container. A deploy is a restart; a
rollback is a restart on the previous tag. Nothing in the process needs coordination, because the
only in-process state is the bounded ingest buffer, which is explicitly droppable.

## Building the image

```bash
VERSION=$(git describe --tags --always --dirty)
docker compose --profile app build --build-arg VERSION="$VERSION"
docker compose --profile app run --rm --entrypoint /usr/local/bin/collector collector -version
```

The Dockerfile builds with the pinned Go toolchain and `CGO_ENABLED=0`, strips the binary
(`-s -w`) and copies it onto `gcr.io/distroless/static:nonroot` at `/usr/local/bin/collector`,
running as `nonroot`. There is no shell in the image: the container healthcheck is the binary
checking itself with `-healthcheck`, which GETs `/healthz` on the `NFC_HTTP_ADDR` port. The
`VERSION` build argument is stamped into `-version`, so a running container identifies the image
that produced it. `.dockerignore` keeps `blueprints/`, `bin/`, `.git/` and `.env` out of the build
context; the image never contains a credential.

CI (`.github/workflows/ci.yml`) asserts the image stays under 30 MB, which is what proves the
distroless base and the static build actually held.

## Running the stack with Compose

```bash
docker compose up -d --wait                          # infrastructure only: timescaledb, mariadb, clickhouse, redpanda
docker compose --profile app up -d --build --wait    # plus the collector
docker compose --profile app ps                      # every service reports healthy
curl -s http://127.0.0.1:8080/readyz | jq
```

The compose file owns image tags, host ports and credentials; the collector's configuration is
written literally into its `environment:` block (it does not read `.env`). Ports are bound to
`127.0.0.1` on the host: `8080` API, `2055/udp` NetFlow, `15432`/`13306`/`19092` for the services.

Volumes hold the data. `docker compose down` keeps them; `docker compose down -v` destroys them —
that is the only way to lose stored flows, and it is not run by any script.

## Configuration in production

Every `NFC_*` variable is documented in `.env.example`. The ones that change between
environments:

- `NFC_POSTGRES_DSN` / `NFC_MARIADB_DSN` — point at the real databases; the MariaDB DSN must keep
  `parseTime=true&loc=UTC`.
- `NFC_API_KEYS` — generate with `openssl rand -hex 32`. Empty while the API is enabled exits 2.
- `NFC_RETENTION_DAYS` — sized by storage, not by wish.
- `NFC_HTTP_ADDR` — keep the API behind your ingress; `/metrics` stays open by design and exposes
  ingest and drop rates, so restrict it to the monitoring network.

Validate before starting:

```bash
docker compose --profile app run --rm --entrypoint /usr/local/bin/collector collector -validate-config
```

## Release

1. Tag `main`: `git tag v1.2.3 && git push --tags`.
2. CI builds the image as `netflow-collector:<tag>` and pushes it to your registry (add the push
   step with your registry credentials; the workflow ships with the build and size gate only).
3. Update the tag reference in your deployment (a compose override, a Helm value, a systemd unit)
   and restart. A restart is the whole deploy.

Migrations run at boot, before any listener binds, and are **forward-only and idempotent**: a
second run changes nothing. A failed migration exits 3 and serves nothing, so a bad release is
loud, not silent.

## Rollback

**Redeploy the previous image tag.** No state conversion, no coordination:

```bash
docker compose --profile app pull collector         # or point the tag reference at the previous version
docker compose --profile app up -d --no-build collector
docker compose --profile app exec collector /usr/local/bin/collector -version
```

This is a complete rollback, not half of one, because of the migration rule: schema changes are
expand → migrate → contract across separate releases, so release N-1 and release N can both read
the schema at every intermediate moment. `Down` blocks exist for local teardown and are never the
production rollback plan; a bad migration is corrected by a new migration.

The rollback window is limited only by how long you retain image tags; nothing in the data path
expires.

## Scaling out: a tiered deployment

One instance is bounded by one UDP read-and-decode loop (`docs/performance.md` has the number).
Past that, the same binary runs as two tiers with Kafka between them:

```
 exporters ──UDP──►  edge collector(s)   ──►  Kafka topic  ──►  central collector(s)  ──►  database
                     NFC_SOURCES=netflow                        NFC_SOURCES=kafka
                     NFC_SINKS=kafka                            NFC_SINKS=clickhouse (or postgres, …)
```

```bash
# edge: decodes, produces, serves /healthz /readyz /metrics — no database, /v1 answers 503
NFC_SOURCES=netflow NFC_SINKS=kafka NFC_KAFKA_BROKERS=kafka-1:9092,kafka-2:9092 NFC_KAFKA_TOPIC=flows

# central: consumes, stores, serves the API
NFC_SOURCES=kafka NFC_SINKS=clickhouse NFC_KAFKA_BROKERS=... NFC_KAFKA_TOPIC=flows NFC_KAFKA_GROUP=nfc-central
```

What the layout buys, and its rules:

- **Split exporters across edge instances, never packets.** NetFlow v9 and IPFIX templates live
  in the process that received them, keyed by exporter. Point each exporter at exactly one edge
  instance, or if a UDP load balancer sits in front, hash on source address so an exporter always
  lands on the same instance.
- **Central instances scale by consumer group.** Every central instance joins the same
  `NFC_KAFKA_GROUP`; Kafka assigns partitions among them. The kafka sink keys each message by
  exporter address, so one exporter's records stay on one partition and arrive in order.
- **Replay is the reason to keep the topic.** Kafka retention holds the last N days of flows;
  adding or rebuilding a storage backend means consuming the topic again with a fresh group, not
  starting from empty. This is what "no vendor lock-in" costs at 3 a.m. — nothing.
- **Not a durability upgrade on its own.** The pipeline drops under backpressure on every source,
  and the kafka source commits offsets as it fetches; a central tier whose database is down for
  longer than its buffer holds loses records like any other instance, and recovers them only by
  replaying the topic (`docs/runbook.md`).
- **One instance cannot be both tiers.** `kafka` in both `NFC_SOURCES` and `NFC_SINKS` is a
  configuration error, because one set of `NFC_KAFKA_*` variables means one topic feeding itself.

## What is intentionally not here

No Kubernetes manifests, no Helm chart, no cloud-specific modules. The unit of deployment is one
container with environment variables and two ports; every orchestrator can run that, and the
operator's existing ingress, monitoring and log pipeline do the rest. There is no cluster
membership or coordination between instances; the tiered layout above scales by adding
independent processes to either tier.
