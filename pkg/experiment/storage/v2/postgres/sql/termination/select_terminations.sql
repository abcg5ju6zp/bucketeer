SELECT
    id,
    environment_id,
    experiment_id,
    reason,
    phase,
    snapshot,
    evaluation_watermark,
    goal_watermark,
    input_summary,
    final_result_id,
    owner_id,
    owner_expires_at,
    attempts,
    last_error,
    created_at,
    updated_at,
    finalized_at
FROM
    experiment_termination
WHERE
    environment_id = $1 AND
    phase IN (
