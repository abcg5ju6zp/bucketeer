UPDATE code_reference_change
SET
    status = ?,
    updated_at = ?
WHERE
    id = ?
    AND status = ?
