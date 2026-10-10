-- Any change after the watermark (published or still pending) forces this
-- archival round to be abandoned.
SELECT 1
FROM code_reference_change
WHERE environment_id = $1
  AND feature_id = $2
  AND seq > $3
LIMIT 1
FOR UPDATE
