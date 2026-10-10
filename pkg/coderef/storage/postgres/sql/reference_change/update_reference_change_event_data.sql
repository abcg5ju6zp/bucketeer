UPDATE code_reference_change
SET
    event_data = $1,
    updated_at = $2
WHERE
    id = $3
