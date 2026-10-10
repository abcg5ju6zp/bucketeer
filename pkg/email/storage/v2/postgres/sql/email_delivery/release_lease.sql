UPDATE email_delivery
SET lease_owner = '', lease_until = 0, updated_at = $1
WHERE id = $2 AND lease_owner = $3
