SELECT COALESCE(
    (
        SELECT MIN(seq)
        FROM code_reference_change
        WHERE environment_id = $1
          AND status = $2
    ) - 1,
    (
        SELECT COALESCE(MAX(seq), 0)
        FROM code_reference_change
        WHERE environment_id = $3
    )
)
