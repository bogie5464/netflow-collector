-- +goose Up
-- idx_flow_records_received_seq duplicated the primary key (received_at, seq):
-- a B-tree serves ORDER BY received_at DESC, seq DESC and the keyset predicate
-- by scanning the primary key backwards, so the index cost every insert and
-- served no read. docs/performance.md has the measurement.
DROP INDEX IF EXISTS idx_flow_records_received_seq;

-- +goose Down
CREATE INDEX IF NOT EXISTS idx_flow_records_received_seq
    ON flow_records (received_at DESC, seq DESC);
