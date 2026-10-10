-- Any change after the watermark (published or still pending) forces this
-- archival round to be abandoned. The locking read also takes gap locks on
-- the scanned feature range to block concurrent inserts.
SELECT 1
FROM code_reference_change
WHERE environment_id = ?
  AND feature_id = ?
  AND seq > ?
LIMIT 1
FOR UPDATE
