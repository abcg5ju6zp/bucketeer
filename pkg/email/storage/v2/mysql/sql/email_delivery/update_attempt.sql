UPDATE email_delivery_attempt SET
    result = ?,
    receipt_id = ?,
    status_code = ?,
    error_code = ?,
    error_message = ?,
    finished_at = ?
WHERE id = ?
