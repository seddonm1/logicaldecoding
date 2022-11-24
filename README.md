# Logical Replication

A project to play with and test Postgres [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) using Rust.

## Why

[Logical replication](https://www.postgresql.org/docs/current/logical-replication.html) gives the ability to subscribe to the Postgres write-ahead-log messages and decode them into usable (and transactional) data. There are many uses for this functionality, for example:

- a web server could store (and invalidate) a local cache of a table in a database to prevent a database round-trip.
- a notification could be sent to a user as a result of an action by another user connected to a different web server instance.

[Logical replication](https://www.postgresql.org/docs/current/logical-replication.html) is lower level than the Postgres [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) functionality, causes [no performance impact](https://reorchestrate.com/posts/debezium-performance-impact/) and does not require the user to choose which tables to listen to.

## What

The main test is in [types/mod.rs](./src/types/mod.rs).

This test attempts to perform deterministic simulation by first attaching the `logicalreplication` listener to an empty database then:

1. Deterministically produce random batches of transactions against an in-memory representation of the table.
2. Applying the batched transactions to the Postgres database.
3. Listening to the logical replication stream and trying to apply them to a second in-memory representation of the table.
4. Stopping the test after `n` iterations and then testing that all three representations align.

## How

1. Start postgres with logical replication mode - see the `docker-compose.yaml` and the `Dockerfile` for configuration.
2. Run `cargo install sqlx-cli` to set up the [sqlx](https://github.com/launchbadge/sqlx) command line utility to allow database migrations.
3. Run `sqlx migrate run` to set up the intial database.
4. Run `cargo test`.

## Further

Ideas of what would be helpful:

- It would be good to build a [procedural macro](https://doc.rust-lang.org/reference/procedural-macros.html) similar to [structmap](https://crates.io/crates/structmap) which automates the generation of applying what is received from the logical decoding (effectively a vector of hashmaps) directly to structs.

- This version deliberately chooses [decoderbufs](https://github.com/debezium/postgres-decoderbufs) but work could be done to ensure it works with [wal2json](https://github.com/eulerto/wal2json) too and that output data is standardised.

## Acknowledgements

Thank you to:

- `rust-postgres`: https://github.com/sfackler/rust-postgres/issues/116
- `postgres-decoderbufs`: https://github.com/debezium/postgres-decoderbufs
- this example: https://github.com/debate-map/app/blob/afc6467b6c6c961f7bcc7b7f901f0ff5cd79d440/Packages/app-server-rs/src/pgclient.rs
