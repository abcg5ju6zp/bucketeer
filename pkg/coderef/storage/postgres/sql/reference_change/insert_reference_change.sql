INSERT INTO code_reference_change (
    id,
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
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING seq
