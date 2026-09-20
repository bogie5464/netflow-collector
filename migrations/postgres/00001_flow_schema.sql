-- Hand-authored. Every statement is idempotent so Migrate can run twice and change nothing.
-- +goose Up
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS exporters (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ip_address    inet        NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    label         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_exporters_ip_address ON exporters (ip_address);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS trg_exporters_updated_at ON exporters;
CREATE TRIGGER trg_exporters_updated_at BEFORE UPDATE ON exporters
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS flow_records (
    received_at    timestamptz NOT NULL,
    seq            bigint GENERATED ALWAYS AS IDENTITY,
    exporter_id    bigint      NOT NULL REFERENCES exporters (id) ON DELETE RESTRICT,
    flow_type      text        NOT NULL CHECK (flow_type IN ('netflow5','netflow9','ipfix')),
    first_switched timestamptz NOT NULL,
    last_switched  timestamptz NOT NULL,
    src_addr       inet        NOT NULL,
    dst_addr       inet        NOT NULL,
    src_port       integer     NOT NULL,
    dst_port       integer     NOT NULL,
    protocol       smallint    NOT NULL,
    tcp_flags      smallint    NOT NULL DEFAULT 0,
    packets        bigint      NOT NULL,
    bytes          bigint      NOT NULL,
    sampling_rate  integer     NOT NULL DEFAULT 1,
    input_iface    integer     NOT NULL DEFAULT 0,
    output_iface   integer     NOT NULL DEFAULT 0,
    src_as         integer     NOT NULL DEFAULT 0,
    dst_as         integer     NOT NULL DEFAULT 0,
    next_hop       inet,
    dedup_key      bytea,
    PRIMARY KEY (received_at, seq)
);

SELECT create_hypertable('flow_records', by_range('received_at'), if_not_exists => TRUE);

-- NULL dedup_key rows are distinct, so UDP rows never collide; a redelivered
-- Kafka message collides with itself exactly once. Includes received_at because
-- every unique index on a hypertable must contain the partitioning column.
CREATE UNIQUE INDEX IF NOT EXISTS uq_flow_records_dedup
    ON flow_records (received_at, dedup_key);
CREATE INDEX IF NOT EXISTS idx_flow_records_exporter_received
    ON flow_records (exporter_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_flow_records_received_seq
    ON flow_records (received_at DESC, seq DESC);
CREATE INDEX IF NOT EXISTS idx_flow_records_src_addr
    ON flow_records (src_addr, received_at DESC);

-- +goose Down
DROP TABLE IF EXISTS flow_records;
DROP TABLE IF EXISTS exporters;
DROP FUNCTION IF EXISTS set_updated_at();
