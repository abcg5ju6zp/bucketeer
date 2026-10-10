UPDATE code_reference
SET
    file_path = $1,
    file_extension = $2,
    line_number = $3,
    code_snippet = $4,
    content_hash = $5,
    aliases = $6,
    repository_name = $7,
    repository_owner = $8,
    repository_type = $9,
    repository_branch = $10,
    commit_hash = $11,
    updated_at = $12,
    version = $13
WHERE
    id = $14
    AND environment_id = $15
    AND version = $16
