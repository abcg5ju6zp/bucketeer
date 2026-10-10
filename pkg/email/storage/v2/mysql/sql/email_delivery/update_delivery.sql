UPDATE email_delivery SET
    status = ?,
    snapshot = ?,
    attempt_count = ?,
    next_retry_at = ?,
    last_provider = ?,
    last_outcome = ?,
    last_error_code = ?,
    last_error_message = ?,
    accepted_provider = ?,
    receipt_id = ?,
    cancel_reason = ?,
    lease_owner = ?,
    lease_until = ?,
    updated_at = ?
WHERE id = ?
