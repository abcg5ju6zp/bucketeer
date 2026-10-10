-- Tables for idempotent welcome-email delivery.
--
-- email_delivery: one row per logical email derived from an account creation
-- domain event. The ID is deterministically derived from the event ID, so the
-- same event delivered via Pub/Sub, Redis Stream or a claimed message after a
-- restart converges to the same row and at most one email is sent. The frozen
-- snapshot (recipient address, requested/resolved language, template version
-- and rendered content) is stored as JSONB.
--
-- email_delivery_attempt: one row per provider communication, recorded before
-- the provider is contacted. Stores the outcome (accepted, rejected, temporary
-- failure, unknown) and the provider receipt.

CREATE TABLE IF NOT EXISTS email_delivery (
    id VARCHAR(100) NOT NULL,                 -- stable delivery identity
    event_id VARCHAR(64) NOT NULL,            -- source domain event ID
    organization_id VARCHAR(64) NOT NULL,
    event_email VARCHAR(255) NOT NULL DEFAULT '',
    event_language VARCHAR(32) NOT NULL DEFAULT '',
    status INT NOT NULL DEFAULT 0,            -- DeliveryStatus
    snapshot JSONB DEFAULT NULL,              -- frozen Snapshot
    attempt_count INT NOT NULL DEFAULT 0,
    next_retry_at BIGINT NOT NULL DEFAULT 0,  -- epoch seconds
    last_provider VARCHAR(32) NOT NULL DEFAULT '',
    last_outcome INT NOT NULL DEFAULT 0,      -- AttemptResult
    last_error_code VARCHAR(64) NOT NULL DEFAULT '',
    last_error_message VARCHAR(511) NOT NULL DEFAULT '',
    accepted_provider VARCHAR(32) NOT NULL DEFAULT '',
    receipt_id VARCHAR(255) NOT NULL DEFAULT '',
    cancel_reason VARCHAR(255) NOT NULL DEFAULT '',
    lease_owner VARCHAR(64) NOT NULL DEFAULT '',
    lease_until BIGINT NOT NULL DEFAULT 0,    -- epoch seconds
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_email_delivery_status_next_retry_at
    ON email_delivery (status, next_retry_at);

CREATE TABLE IF NOT EXISTS email_delivery_attempt (
    id VARCHAR(120) NOT NULL,
    delivery_id VARCHAR(100) NOT NULL,
    seq INT NOT NULL,
    provider VARCHAR(32) NOT NULL,
    result INT NOT NULL DEFAULT 0,             -- AttemptResult
    receipt_id VARCHAR(255) NOT NULL DEFAULT '',
    status_code INT NOT NULL DEFAULT 0,
    error_code VARCHAR(64) NOT NULL DEFAULT '',
    error_message VARCHAR(511) NOT NULL DEFAULT '',
    started_at BIGINT NOT NULL,
    finished_at BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_email_delivery_attempt
        FOREIGN KEY (delivery_id)
        REFERENCES email_delivery (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_email_delivery_attempt_delivery_seq
    ON email_delivery_attempt (delivery_id, seq);
