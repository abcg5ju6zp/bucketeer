INSERT INTO experiment (
    id,
    goal_id,
    feature_id,
    feature_version,
    variations,
    start_at,
    stop_at,
    stopped,
    stopped_at,
    created_at,
    updated_at,
    archived,
    deleted,
    goal_ids,
    name,
    description,
    base_variation_id,
    status,
    termination_id,
    final_result_id,
    maintainer,
    environment_id
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?
)
