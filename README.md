# Logical Replication

A project to play with and test Postgres logical replication with Rust.

## How to

Start postgres with logical replication mode - see the `docker-compose.yaml`. Run `cargo test`. The main test is in `types/mod.rs`.

# Acknowledgements

Thank you to:

- `rust-postgres`: https://github.com/sfackler/rust-postgres/issues/116
- `postgres-decoderbufs`: https://github.com/debezium/postgres-decoderbufs
- this example: https://github.com/debate-map/app/blob/afc6467b6c6c961f7bcc7b7f901f0ff5cd79d440/Packages/app-server-rs/src/pgclient.rs
