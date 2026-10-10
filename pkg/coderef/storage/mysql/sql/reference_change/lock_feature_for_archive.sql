-- Lock the parent feature row first so concurrent reference mutations cannot
-- commit while the archival transaction finishes.
SELECT 1
FROM feature
WHERE environment_id = ?
  AND id = ?
FOR UPDATE
