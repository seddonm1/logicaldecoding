SELECT
    xmin::text::bigint AS xmin,
    *
FROM
    tenants
WHERE
    id = ANY($1);