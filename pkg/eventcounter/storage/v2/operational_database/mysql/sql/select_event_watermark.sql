SELECT
    high_water_at,
    updated_at
FROM
    event_watermark
WHERE
    environment_id = ? AND
    event_type = ?
