UPDATE email_delivery
SET lease_owner = ?, lease_until = ?, updated_at = ?
WHERE id = ?
  AND status NOT IN (?, ?, ?)
  AND (lease_until < ? OR lease_owner = ?)
