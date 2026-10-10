UPDATE
    experiment_termination
SET
    phase = $1,
    input_summary = $2,
    final_result_id = $3,
    owner_id = $4,
    owner_expires_at = $5,
    attempts = $6,
    last_error = $7,
    updated_at = $8,
    finalized_at = $9
WHERE
    id = $10 AND
    environment_id = $11 AND
    phase = $12
