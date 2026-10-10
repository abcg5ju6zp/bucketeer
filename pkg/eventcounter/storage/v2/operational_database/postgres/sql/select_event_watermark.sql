SELECT
    high_water_at,
    updated_at
FROM
    event_watermark
WHERE
    environment_id = $1 AND
    event_type = $2
