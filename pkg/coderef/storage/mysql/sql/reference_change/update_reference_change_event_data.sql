UPDATE code_reference_change
SET
    event_data = ?,
    updated_at = ?
WHERE
    id = ?
