SELECT
    xmin::text::bigint AS xmin,
    *
FROM
    tenants
WHERE
    id = $1;