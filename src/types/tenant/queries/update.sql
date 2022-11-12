UPDATE
    tenants
SET
    name = $2,
    short_description = $3,
    long_description = $4
WHERE
    id = $1
RETURNING xmin::text::bigint;