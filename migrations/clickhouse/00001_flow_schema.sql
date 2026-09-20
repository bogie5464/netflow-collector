-- +goose Up
-- ClickHouse has no unique index, so the NULL-distinct UNIQUE (received_at, dedup_key)
-- that dedups Kafka redelivery on the other backends is built into the sorting key
-- instead. seq is derived by the table, never supplied by the sink: for a record with a
-- dedup key it is a function of that key, so a redelivery lands on an identical
-- (received_at, seq) and ReplacingMergeTree collapses it; for a record without one it is
-- random, so two UDP records at the same microsecond never collide. Reads use FINAL so
-- a redelivery is invisible before the merge runs. The branch on dedup_key lives here,
-- in the schema — the sink writes every record the same way.

CREATE TABLE IF NOT EXISTS exporters (
    id            UInt64,
    ip_address    IPv6,
    label         SimpleAggregateFunction(anyLast, Nullable(String)),
    first_seen_at SimpleAggregateFunction(min, DateTime64(6, 'UTC')),
    last_seen_at  SimpleAggregateFunction(max, DateTime64(6, 'UTC'))
) ENGINE = AggregatingMergeTree
ORDER BY ip_address;

CREATE TABLE IF NOT EXISTS flow_records (
    received_at    DateTime64(6, 'UTC'),
    dedup_key      String,
    seq            UInt64 MATERIALIZED if(dedup_key = '', rand64(), reinterpretAsUInt64(substring(dedup_key, 1, 8))),
    exporter_id    UInt64,
    exporter_addr  IPv6,
    flow_type      LowCardinality(String),
    first_switched DateTime64(6, 'UTC'),
    last_switched  DateTime64(6, 'UTC'),
    src_addr       IPv6,
    dst_addr       IPv6,
    src_port       UInt16,
    dst_port       UInt16,
    protocol       UInt8,
    tcp_flags      UInt8,
    packets        UInt64,
    bytes          UInt64,
    sampling_rate  UInt32,
    input_iface    UInt32,
    output_iface   UInt32,
    src_as         UInt32,
    dst_as         UInt32,
    next_hop       Nullable(IPv6)
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(received_at)
ORDER BY (received_at, seq);

-- Retention is a table TTL, applied by Migrate from NFC_RETENTION_DAYS after
-- this file runs, because a migration cannot read configuration.

-- +goose Down
DROP TABLE IF EXISTS flow_records;
DROP TABLE IF EXISTS exporters;
