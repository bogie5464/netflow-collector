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
 exporters ──UDP──►  one address  ──►  edge collectors    ──►  Kafka topic  ──►  central collectors  ──►  database
                                        NFC_SOURCES=netflow                        NFC_SOURCES=kafka
                                        NFC_SINKS=kafka                            NFC_SINKS=clickhouse (or postgres, …)
                                        NFC_KAFKA_TEMPLATE_TOPIC=…                 NFC_KAFKA_GROUP=…
```

```bash
# edge: decodes, produces, serves /healthz /readyz /metrics — no database, /v1 answers 503
NFC_SOURCES=netflow NFC_SINKS=kafka NFC_KAFKA_BROKERS=kafka-1:9092,kafka-2:9092 \
  NFC_KAFKA_TOPIC=netflow.flows NFC_KAFKA_TEMPLATE_TOPIC=netflow.templates

# central: consumes, stores, serves the API
NFC_SOURCES=kafka NFC_SINKS=clickhouse NFC_KAFKA_BROKERS=... NFC_KAFKA_TOPIC=netflow.flows NFC_KAFKA_GROUP=netflow-central
```

What the layout buys, and its rules:

- **One address for every exporter.** Routers point at one IP; nothing in their configuration
  changes when the edge tier scales. The reference Kubernetes manifests below give that address
  as a `Service`; on bare metal it is anycast or an L4 balancer in front of the edge instances.
- **Edge instances are interchangeable when they share templates.** NetFlow v9 and IPFIX
  templates arrive once, from one exporter, at one instance, and every later datagram from that
  exporter is undecodable without them. `NFC_KAFKA_TEMPLATE_TOPIC` names a compacted topic every
  edge instance publishes its templates to and consumes on start-up, before binding its socket —
  so any instance can decode any exporter, a new instance decodes its first datagram, and a
  replaced one loses nothing. Without it, keep each exporter on one instance (source-address
  affinity at the balancer) and expect a decode gap until the exporter's next template refresh
  whenever an exporter moves. v5 has no templates and needs neither.
- **The exporter's source address must survive the path.** Templates, the `exporters` table and
  the Kafka partition key are all keyed on it. A balancer that rewrites the source turns every
  router into the balancer.
- **Central instances scale by consumer group.** Every central instance joins the same
  `NFC_KAFKA_GROUP`; Kafka assigns partitions among them. The kafka sink keys each message by
  exporter address, so one exporter's records stay on one partition and arrive in order.
- **The bus is a contract.** Every message carries `"format": "netflow-collector/v1"`
  (`internal/wire`). Other consumers — a security pipeline, an operations dashboard — read the
  same topic without this project's code; the schema changes only by adding fields, and a change
  that would break a reader gets a new format value and a new topic.
- **Replay is the reason to keep the topic.** Kafka retention holds the last N days of flows;
  adding or rebuilding a storage backend means consuming the topic again with a fresh group, not
  starting from empty.
- **Size the edge buffer for the broker, not the database.** `NFC_PIPELINE_BUFFER` is the edge's
  only ride-through when the broker is briefly unavailable: ~260 bytes per record, so 2,000,000
  records is ~520 MB and ten seconds at 200,000 flows/s. The shipped default of 65,536 is a third
  of a second.
- **The central tier is at-least-once.** The kafka source is a pull source: the pipeline waits
  for it instead of dropping, and the source commits a poll's offsets only after every record in
  it has been written to every sink. If a sink rejects a batch — the database is down, a
  migration is running — the source rewinds to its last committed offset and delivers again; the
  sinks that did write those records collapse the duplicates on the dedup key. A database outage
  therefore costs throughput, never records, for as long as the topic retains them. The edge tier
  stays best-effort: nothing on a UDP wire will wait.
- **One instance cannot be both tiers.** `kafka` in both `NFC_SOURCES` and `NFC_SINKS` is a
  configuration error, because one set of `NFC_KAFKA_*` variables means one topic feeding itself.

## Kubernetes

`deploy/k8s/` is the tiered layout as Kustomize manifests. Edit `config.yaml` (brokers, topics)
and `secrets.yaml` (API keys, the storage DSN), then:

```bash
kubectl apply -k deploy/k8s
kubectl -n netflow get svc netflow-edge     # the one address to put in every exporter's configuration
```

| Object | What it is | The choices that matter |
|---|---|---|
| `Service/netflow-edge` | The one UDP address | `externalTrafficPolicy: Local` preserves the exporter's source address, so only nodes running an edge pod receive traffic — the Deployment spreads pods across nodes, and the balancer must spread across those nodes (MetalLB/Cilium in BGP mode with ECMP on premises; NLB with IP targets or a passthrough LB in the clouds; L2-mode MetalLB funnels everything through one node). `sessionAffinity: ClientIP` is optional once templates are shared, and free. |
| `Deployment/netflow-edge` + HPA + PDB | Edge tier | Readiness is the bound socket, so a scaled-up pod finishes its template replay before it receives traffic. The HPA is on CPU as a proxy; the honest signal is `netflow_pipeline_buffer_length` through the Prometheus adapter. The pipeline buffer and the memory limit are sized together. |
| `Deployment/netflow-central` + PDB | Central tier | Scales by replicas alone. `NFC_BATCH_SIZE=20000` because one insert is one ClickHouse part. |
| `Service/netflow-central` | The query API, `ClusterIP` | Put an ingress in front; the bearer key is the only authentication. |

The image is `ghcr.io/bogie5464/netflow-collector:<tag>`, published for every release by
`.github/workflows/release.yml` for `linux/amd64` and `linux/arm64`. CI renders and validates the
manifests on every push (`kustomize build | kubeconform -strict`).

## What is intentionally not here

No Helm chart and no cloud-specific modules: the Kustomize manifests are a reference layout to
copy into your own configuration, not a package to depend on. The unit of deployment is one
container with environment variables and two ports; every orchestrator can run that, and the
operator's existing ingress, monitoring and log pipeline do the rest. There is no cluster
membership or coordination between instances beyond the template topic; both tiers scale by
adding independent processes.
