# Snowflake Connector for Rust

[![test](https://github.com/estie-inc/snowflake-connector-rs/actions/workflows/test.yml/badge.svg)](https://github.com/estie-inc/snowflake-connector-rs/actions/workflows/test.yml)
[![Crates.io](https://img.shields.io/crates/v/snowflake-connector-rs)](https://crates.io/crates/snowflake-connector-rs)

A Rust client for Snowflake, which enables you to connect to Snowflake and run queries.

## MSRV

The minimum supported Rust version (MSRV) is 1.88.

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

### External Browser SSO Use Cases

Typical configurations for the `external-browser-sso` feature:

- Local default (auto browser launch, localhost callback, auto-picked port)
   ```rust
   use snowflake_connector_rs::{ExternalBrowserConfig, SnowflakeAuthMethod};
   let auth = SnowflakeAuthMethod::ExternalBrowser(ExternalBrowserConfig::default());
   ```
- Docker/container setup (manual open with explicit callback bind address/port)
   ```rust
   use std::net::Ipv4Addr;
   use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthMethod};
   let external_browser = ExternalBrowserConfig::with_callback_listener(
       BrowserLaunchMode::Manual,
       Ipv4Addr::UNSPECIFIED.into(),
       3037,
   );
   let auth = SnowflakeAuthMethod::ExternalBrowser(external_browser);
   ```
- Without callback listener mode (manual redirected URL input)
   ```rust
   use std::num::NonZeroU16;
   use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthMethod};
   let redirect_port = NonZeroU16::new(3037).unwrap();
   let external_browser =
       ExternalBrowserConfig::without_callback_listener(BrowserLaunchMode::Manual, redirect_port);
   let auth = SnowflakeAuthMethod::ExternalBrowser(external_browser);
   ```

For Docker/container setup, make sure that:

- your Snowflake OAuth redirect URI allows the same callback port (for example `3037`), and
- the callback port is mapped to the host (for example `-p 3037:3037` or equivalent in Compose).

`0.0.0.0` binds on all interfaces in the container. Use the minimum required network exposure for your environment.

In `WithoutCallbackListener` mode:

- no local server is started, so `localhost:<redirect_port>` is not actually listened on by this connector.
- a non-zero `redirect_port` is still required because Snowflake uses `BROWSER_MODE_REDIRECT_PORT` to construct the browser redirect URL.
- the browser may show a connection error page at `localhost:<redirect_port>` after login; copy that redirected URL and paste it into the terminal prompt so the connector can extract the token.
