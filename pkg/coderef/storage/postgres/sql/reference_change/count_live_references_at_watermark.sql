SELECT c.feature_id, COUNT(1)
FROM code_reference_change c
WHERE c.environment_id = $1
  AND c.seq <= $2
  AND c.change_type = $3
  AND c.feature_id IN (%s)
  AND NOT EXISTS (
      SELECT 1
      FROM code_reference_change d
      WHERE d.environment_id = c.environment_id
        AND d.code_reference_id = c.code_reference_id
        AND d.seq <= $4
        AND d.change_type = $5
  )
GROUP BY c.feature_id
