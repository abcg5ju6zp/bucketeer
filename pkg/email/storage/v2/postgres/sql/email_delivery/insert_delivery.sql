INSERT INTO email_delivery (
    id, event_id, organization_id, event_email, event_language, status,
    snapshot, attempt_count, next_retry_at, last_provider, last_outcome,
    last_error_code, last_error_message, accepted_provider, receipt_id,
    cancel_reason, lease_owner, lease_until, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
)
