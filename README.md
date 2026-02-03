# Snowflake Connector for Rust

[![test](https://github.com/estie-inc/snowflake-connector-rs/actions/workflows/test.yml/badge.svg)](https://github.com/estie-inc/snowflake-connector-rs/actions/workflows/test.yml)
[![Crates.io](https://img.shields.io/crates/v/snowflake-connector-rs)](https://crates.io/crates/snowflake-connector-rs)

A Rust client for Snowflake, which enables you to connect to Snowflake and run queries.

## Usage

```rust
let client = SnowflakeClient::new(
    "USERNAME",
    SnowflakeAuthMethod::Password("PASSWORD".to_string()),
    SnowflakeClientConfig {
        account: "ACCOUNT".to_string(),
        role: Some("ROLE".to_string()),
        warehouse: Some("WAREHOUSE".to_string()),
        database: Some("DATABASE".to_string()),
        schema: Some("SCHEMA".to_string()),
        timeout: Some(std::time::Duration::from_secs(30)),
    },
)?;
let session = client.create_session().await?;

let query = "CREATE TEMPORARY TABLE example (id NUMBER, value STRING)";
session.query(query).await?;

let query = "INSERT INTO example (id, value) VALUES (1, 'hello'), (2, 'world')";
session.query(query).await?;

let query = "SELECT * FROM example ORDER BY id";
let rows = session.query(query).await?;
assert_eq!(rows.len(), 2);
assert_eq!(rows[0].get::<i64>("ID")?, 1);
assert_eq!(rows[0].get::<String>("VALUE")?, "hello");
```

## Features

This crate supports optional features to decrypt legacy keys that use DES or 3DES encryption.
These algorithms are considered insecure and should only be used for legacy compatibility.

- **`pkcs8-des`**: Enables DES decryption support
- **`pkcs8-3des`**: Enables 3DES decryption support
- **`external-browser-sso`**: Enables external browser SSO authentication support

> [!NOTE]
> The `external-browser-sso` feature is experimental. 
> The implementation and API may change in future releases, and stability or backward compatibility is not guaranteed.
> Use this feature with caution in production environments.
> Please open an issue for bugs or feature requests.

## External browser SSO environment variables
- `SF_AUTH_SOCKET_ADDR`: Listener bind host (default `localhost`).
- `SF_AUTH_SOCKET_PORT`: Listener port (default `0` to auto-pick).
