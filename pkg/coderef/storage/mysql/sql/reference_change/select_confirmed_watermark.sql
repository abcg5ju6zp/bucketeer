SELECT COALESCE(
    (
        SELECT MIN(seq)
        FROM code_reference_change
        WHERE environment_id = ?
          AND status = ?
    ) - 1,
    (
        SELECT COALESCE(MAX(seq), 0)
        FROM code_reference_change
        WHERE environment_id = ?
    )
)
