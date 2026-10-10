UPDATE email_delivery
SET lease_owner = $1, lease_until = $2, updated_at = $3
WHERE id = $4
  AND status NOT IN ($5, $6, $7)
  AND (lease_until < $8 OR lease_owner = $9)
