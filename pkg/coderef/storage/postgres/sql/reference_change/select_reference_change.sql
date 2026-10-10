SELECT
    id,
    seq,
    code_reference_id,
    environment_id,
    feature_id,
    change_type,
    before_version,
    after_version,
    request_hash,
    event_data,
    status,
    created_at,
    updated_at
FROM
    code_reference_change
WHERE
    id = $1
