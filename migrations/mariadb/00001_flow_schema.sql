-- Hand-authored. Every statement is idempotent so Migrate can run twice and change nothing.
-- No partitioning in v1: MariaDB RANGE partitioning would force dedup_key into the
-- partition expression and break the nullable-unique dedup semantics. Retention is a
-- bounded chunked DELETE run by the backend on a ticker.
-- +goose Up
CREATE TABLE IF NOT EXISTS exporters (
    id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ip_address    VARBINARY(16) NOT NULL,
    first_seen_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_seen_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    label         VARCHAR(255) NULL,
    created_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                               ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_exporters_ip_address (ip_address)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS flow_records (
    seq            BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    received_at    DATETIME(6)   NOT NULL,
    exporter_id    BIGINT        NOT NULL,
    flow_type      VARCHAR(16)   NOT NULL,
    first_switched DATETIME(6)   NOT NULL,
    last_switched  DATETIME(6)   NOT NULL,
    src_addr       VARBINARY(16) NOT NULL,
    dst_addr       VARBINARY(16) NOT NULL,
    src_port       SMALLINT UNSIGNED NOT NULL,
    dst_port       SMALLINT UNSIGNED NOT NULL,
    protocol       TINYINT UNSIGNED  NOT NULL,
    tcp_flags      TINYINT UNSIGNED  NOT NULL DEFAULT 0,
    packets        BIGINT UNSIGNED   NOT NULL,
    bytes          BIGINT UNSIGNED   NOT NULL,
    sampling_rate  INT UNSIGNED  NOT NULL DEFAULT 1,
    input_iface    INT UNSIGNED  NOT NULL DEFAULT 0,
    output_iface   INT UNSIGNED  NOT NULL DEFAULT 0,
    src_as         INT UNSIGNED  NOT NULL DEFAULT 0,
    dst_as         INT UNSIGNED  NOT NULL DEFAULT 0,
    next_hop       VARBINARY(16) NULL,
    dedup_key      VARBINARY(16) NULL,
    UNIQUE KEY uq_flow_records_dedup (received_at, dedup_key),
    KEY idx_flow_records_exporter_received (exporter_id, received_at),
    KEY idx_flow_records_received_seq (received_at, seq),
    KEY idx_flow_records_src_addr (src_addr, received_at),
    CONSTRAINT fk_flow_records_exporter FOREIGN KEY (exporter_id)
        REFERENCES exporters (id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- +goose Down
DROP TABLE IF EXISTS flow_records;
DROP TABLE IF EXISTS exporters;
