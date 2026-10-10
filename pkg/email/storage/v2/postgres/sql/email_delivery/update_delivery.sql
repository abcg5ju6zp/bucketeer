UPDATE email_delivery SET
    status = $1,
    snapshot = $2,
    attempt_count = $3,
    next_retry_at = $4,
    last_provider = $5,
    last_outcome = $6,
    last_error_code = $7,
    last_error_message = $8,
    accepted_provider = $9,
    receipt_id = $10,
    cancel_reason = $11,
    lease_owner = $12,
    lease_until = $13,
    updated_at = $14
WHERE id = $15
