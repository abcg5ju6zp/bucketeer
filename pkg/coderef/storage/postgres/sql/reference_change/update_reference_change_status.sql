UPDATE code_reference_change
SET
    status = $1,
    updated_at = $2
WHERE
    id = $3
    AND status = $4
