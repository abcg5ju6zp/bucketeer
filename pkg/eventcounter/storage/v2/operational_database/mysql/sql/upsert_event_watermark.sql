INSERT INTO event_watermark (
    environment_id,
    event_type,
    high_water_at,
    updated_at
) VALUES (
    ?, ?, ?, ?
)
ON DUPLICATE KEY UPDATE
    high_water_at = GREATEST(high_water_at, VALUES(high_water_at)),
    updated_at = GREATEST(updated_at, VALUES(updated_at))
