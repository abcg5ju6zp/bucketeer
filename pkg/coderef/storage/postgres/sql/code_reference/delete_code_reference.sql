DELETE FROM code_reference
WHERE
    id = $1
    AND environment_id = $2
    AND version = $3
