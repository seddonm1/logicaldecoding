INSERT INTO
    tenants (tenant_id, id, name, short_description, long_description)
VALUES
    ($1, $1, $2, $3, $4) RETURNING xmin::text::bigint;