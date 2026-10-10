UPDATE
    experiment_termination
SET
    phase = ?,
    input_summary = ?,
    final_result_id = ?,
    owner_id = ?,
    owner_expires_at = ?,
    attempts = ?,
    last_error = ?,
    updated_at = ?,
    finalized_at = ?
WHERE
    id = ? AND
    environment_id = ? AND
    phase = ?
