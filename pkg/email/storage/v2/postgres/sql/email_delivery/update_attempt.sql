UPDATE email_delivery_attempt SET
    result = $1,
    receipt_id = $2,
    status_code = $3,
    error_code = $4,
    error_message = $5,
    finished_at = $6
WHERE id = $7
