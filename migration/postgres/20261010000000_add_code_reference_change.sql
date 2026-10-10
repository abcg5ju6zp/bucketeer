-- Add a monotonically increasing entity version to code references.
-- Existing rows start at version 1.
ALTER TABLE code_reference
    ADD COLUMN version BIGINT NOT NULL DEFAULT 1;

-- Transactional outbox and idempotency ledger for code reference changes.
-- Every create/update/delete inserts one row in the same transaction as the
-- reference mutation. The domain event is stored in event_data, so failures
-- after commit can republish the exact same event, and retries/lost responses
-- always resolve to the same change and version.
CREATE TABLE code_reference_change (
    id VARCHAR(36) NOT NULL,
    seq BIGSERIAL,
    code_reference_id VARCHAR(255) NOT NULL,
    environment_id VARCHAR(255) NOT NULL,
    feature_id VARCHAR(255) NOT NULL,
    change_type SMALLINT NOT NULL,
    before_version BIGINT NOT NULL,
    after_version BIGINT NOT NULL,
    request_hash CHAR(64) NOT NULL,
    event_data BYTEA,
    status SMALLINT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_code_reference_change_request UNIQUE (environment_id, request_hash),
    CONSTRAINT uq_code_reference_change_type UNIQUE (environment_id, code_reference_id, change_type),
    CONSTRAINT fk_code_reference_change_feature
        FOREIGN KEY (feature_id, environment_id)
        REFERENCES feature (id, environment_id)
);
CREATE INDEX idx_code_reference_change_scan
    ON code_reference_change (environment_id, feature_id, seq);

-- Backfill one confirmed CREATE change for every existing reference. These
-- rows are PUBLISHED and never republished, so event_data stays NULL. The
-- request hash is built from two core md5() calls, no extension required.
INSERT INTO code_reference_change (
    id, code_reference_id, environment_id, feature_id, change_type,
    before_version, after_version, request_hash, event_data, status,
    created_at, updated_at
)
SELECT
    gen_random_uuid(), id, environment_id, feature_id, 1,
    0, version,
    md5('backfill:' || id) || md5(id || ':backfill'),
    NULL, 2,
    created_at, updated_at
FROM code_reference;
