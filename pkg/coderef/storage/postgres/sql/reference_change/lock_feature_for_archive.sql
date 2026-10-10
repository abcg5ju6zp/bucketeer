-- Lock the parent feature row first so concurrent reference mutations cannot
-- commit while the archival transaction finishes. A concurrent child insert
-- takes FOR KEY SHARE on this row and therefore blocks against FOR UPDATE.
SELECT 1
FROM feature
WHERE environment_id = $1
  AND id = $2
FOR UPDATE
