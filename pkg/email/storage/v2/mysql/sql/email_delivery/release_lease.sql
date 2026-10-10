UPDATE email_delivery
SET lease_owner = '', lease_until = 0, updated_at = ?
WHERE id = ? AND lease_owner = ?
