SELECT c.feature_id, COUNT(1)
FROM code_reference_change c
WHERE c.environment_id = ?
  AND c.seq <= ?
  AND c.change_type = ?
  AND c.feature_id IN (%s)
  AND NOT EXISTS (
      SELECT 1
      FROM code_reference_change d
      WHERE d.environment_id = c.environment_id
        AND d.code_reference_id = c.code_reference_id
        AND d.seq <= ?
        AND d.change_type = ?
  )
GROUP BY c.feature_id
