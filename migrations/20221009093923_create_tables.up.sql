CREATE TABLE tenants (
    tenant_id UUID NOT NULL,
    id UUID PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    short_description TEXT,
    long_description TEXT
);