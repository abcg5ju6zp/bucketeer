INSERT INTO email_delivery_attempt (
    id, delivery_id, seq, provider, result, receipt_id, status_code,
    error_code, error_message, started_at, finished_at, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
