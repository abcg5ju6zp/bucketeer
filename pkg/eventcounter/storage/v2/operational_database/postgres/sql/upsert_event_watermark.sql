INSERT INTO event_watermark (
    environment_id,
    event_type,
    high_water_at,
    updated_at
) VALUES (
    $1, $2, $3, $4
)
ON CONFLICT (environment_id, event_type) DO UPDATE SET
    high_water_at = GREATEST(event_watermark.high_water_at, EXCLUDED.high_water_at),
    updated_at = GREATEST(event_watermark.updated_at, EXCLUDED.updated_at)
