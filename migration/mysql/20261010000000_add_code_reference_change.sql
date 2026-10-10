-- Add a monotonically increasing entity version to code references.
-- Existing rows start at version 1.
ALTER TABLE code_reference
    ADD COLUMN version BIGINT NOT NULL DEFAULT 1
    COMMENT 'Monotonically increasing entity version' AFTER updated_at;

-- Transactional outbox and idempotency ledger for code reference changes.
-- Every create/update/delete inserts one row in the same transaction as the
-- reference mutation. The domain event is stored in event_data, so failures
-- after commit can republish the exact same event, and retries/lost responses
-- always resolve to the same change and version.
CREATE TABLE code_reference_change (
    id VARCHAR(36) NOT NULL COMMENT 'Stable change id, equals the domain event id',
    seq BIGINT NOT NULL AUTO_INCREMENT COMMENT 'Monotonically increasing change sequence',
    code_reference_id VARCHAR(255) NOT NULL,
    environment_id VARCHAR(255) NOT NULL,
    feature_id VARCHAR(255) NOT NULL,
    change_type TINYINT NOT NULL COMMENT '1:CREATE, 2:UPDATE, 3:DELETE',
    before_version BIGINT NOT NULL COMMENT 'Reference version before the change, 0 on create',
    after_version BIGINT NOT NULL COMMENT 'Reference version after the change, 0 on delete',
    request_hash CHAR(64) NOT NULL COMMENT 'Fingerprint of the request, used for idempotency',
    event_data LONGBLOB COMMENT 'Serialized domain event; NULL only for backfilled rows',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '1:PENDING, 2:PUBLISHED',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_code_reference_change_seq (seq),
    UNIQUE KEY uq_code_reference_change_request (environment_id, request_hash),
    UNIQUE KEY uq_code_reference_change_type (environment_id, code_reference_id, change_type),
    KEY idx_code_reference_change_scan (environment_id, feature_id, seq),
    CONSTRAINT fk_code_reference_change_feature
        FOREIGN KEY (feature_id, environment_id)
        REFERENCES feature (id, environment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Backfill one confirmed CREATE change for every existing reference. These
-- rows are PUBLISHED and never republished, so event_data stays NULL.
INSERT INTO code_reference_change (
    id, code_reference_id, environment_id, feature_id, change_type,
    before_version, after_version, request_hash, event_data, status,
    created_at, updated_at
)
SELECT
    UUID(), id, environment_id, feature_id, 1,
    0, version, SHA2(CONCAT('backfill:', id), 256), NULL, 2,
    created_at, updated_at
FROM code_reference;
