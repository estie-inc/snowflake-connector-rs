# Snowflake Connector for Rust

[![test](https://github.com/estie-inc/snowflake-connector-rs/actions/workflows/test.yml/badge.svg)](https://github.com/estie-inc/snowflake-connector-rs/actions/workflows/test.yml)
[![Crates.io](https://img.shields.io/crates/v/snowflake-connector-rs)](https://crates.io/crates/snowflake-connector-rs)

A Rust client for Snowflake, which enables you to connect to Snowflake and run queries.

## Usage

```rust
#[derive(Debug, PartialEq, snowflake_connector_rs::FromRow)]
struct ExampleRow {
    id: i64,
    value: String,
}

let session_config = SnowflakeSessionConfig::default()
    .with_role("ROLE")
    .with_warehouse("WAREHOUSE")
    .with_database("DATABASE")
    .with_schema("SCHEMA");

let query_config = SnowflakeQueryConfig::default()
    .with_async_query_completion_timeout(std::time::Duration::from_secs(30));

let client = SnowflakeClient::new(
    SnowflakeClientConfig::new(
        "USERNAME",
        "ACCOUNT",
        SnowflakeAuthConfig::password("PASSWORD"),
    )
    .with_session(session_config)
    .with_query(query_config),
)?;
let session = client.create_session().await?;

session
    .query("CREATE TEMPORARY TABLE example (id NUMBER, value STRING)")
    .await?;
session
    .query("INSERT INTO example (id, value) VALUES (1, 'hello'), (2, 'world')")
    .await?;

let dynamic_rows = session
    .query("SELECT id, value FROM example ORDER BY id")
    .await?
    .collect()
    .await?;
assert_eq!(dynamic_rows.len(), 2);

let rows: Vec<ExampleRow> = session
    .query_as::<ExampleRow, _>("SELECT id, value FROM example ORDER BY id")
    .await?
    .collect()
    .await?;
assert_eq!(
    rows,
    vec![
        ExampleRow {
            id: 1,
            value: "hello".to_string(),
        },
        ExampleRow {
            id: 2,
            value: "world".to_string(),
        },
    ]
);

let typed_table = session
    .query_as::<ExampleRow, _>("SELECT id, value FROM example ORDER BY id")
    .await?
    .collect_table()
    .await?;
assert_eq!(typed_table.row_count(), 2);

let mut result = session
    .query_as::<ExampleRow, _>("SELECT id, value FROM example ORDER BY id")
    .await?;
while let Some(table) = result.next_table().await? {
    for row in table.rows() {
        let row = row?;
        println!("{row:?}");
    }
}

let result = session.query("SELECT id, value FROM example ORDER BY id").await?;
let table = result.collect_table().await?;
assert_eq!(table.row_count(), 2);
```

## Authentication

The authentication method is chosen with `SnowflakeAuthConfig`, passed to `SnowflakeClientConfig::new`.

### Key-pair

Key-pair (JWT) authentication. Recommended for programmatic and unattended access. Requires the `key-pair-auth` feature (enabled by default).

```rust
use snowflake_connector_rs::{KeyPairAuthConfig, SnowflakeAuthConfig};

// Encrypted PKCS#8 PEM, with its passphrase:
let auth = SnowflakeAuthConfig::key_pair(
    KeyPairAuthConfig::encrypted_pem(pem, b"passphrase".to_vec()),
);

// Unencrypted PKCS#8 PEM:
let auth = SnowflakeAuthConfig::key_pair(KeyPairAuthConfig::unencrypted_pem(pem));
```

### OAuth

Authenticate with a Snowflake OAuth access token. Acquiring and refreshing the token is the caller's responsibility.

```rust
use snowflake_connector_rs::SnowflakeAuthConfig;

let auth = SnowflakeAuthConfig::oauth("OAUTH_ACCESS_TOKEN");
```

### Password and MFA

Snowflake enforces MFA for password sign-ins. Without a passcode, login relies on an out-of-band approval (for example a Duo push) that must be approved interactively, so plain password auth is unsuitable for unattended use — prefer key-pair or OAuth there.

```rust
use snowflake_connector_rs::{PasswordAuthConfig, SnowflakeAuthConfig};

// Plain password (an interactive MFA approval may still be required).
let auth = SnowflakeAuthConfig::password("PASSWORD");

// TOTP passcode sent as a separate value.
let auth = SnowflakeAuthConfig::password(
    PasswordAuthConfig::new("PASSWORD").with_passcode("123456"),
);

// Or the passcode already appended to the password.
let auth = SnowflakeAuthConfig::password(
    PasswordAuthConfig::new("PASSWORD123456").with_passcode_in_password(),
);
```

The MFA token cache is not supported, so an MFA factor may be requested on every connection.

### External browser SSO (experimental)

> [!NOTE]
> The `external-browser-sso` feature is experimental.
> The implementation and API may change in future releases, and stability or backward compatibility is not guaranteed.
> Use this feature with caution in production environments.
> Please open an issue for bugs or feature requests.

Requires the `external-browser-sso` feature. Typical configurations:

- Local default (auto browser launch, localhost callback, auto-picked port)
   ```rust
   use snowflake_connector_rs::{ExternalBrowserConfig, SnowflakeAuthConfig};
   let auth = SnowflakeAuthConfig::external_browser(ExternalBrowserConfig::default());
   ```
- Docker/container setup (manual open with explicit callback bind address/port)
   ```rust
    use std::net::Ipv4Addr;
    use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthConfig};
    let external_browser = ExternalBrowserConfig::with_callback_listener(
        BrowserLaunchMode::Manual,
        Ipv4Addr::UNSPECIFIED.into(),
        3037,
    );
    let auth = SnowflakeAuthConfig::external_browser(external_browser);
    ```
- Without callback listener mode (manual redirected URL input)
   ```rust
    use std::num::NonZeroU16;
    use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthConfig};
    let redirect_port = NonZeroU16::new(3037).unwrap();
    let external_browser =
        ExternalBrowserConfig::without_callback_listener(BrowserLaunchMode::Manual, redirect_port);
    let auth = SnowflakeAuthConfig::external_browser(external_browser);
    ```

For Docker/container setup, make sure that:

- your Snowflake OAuth redirect URI allows the same callback port (for example `3037`), and
- the callback port is mapped to the host (for example `-p 3037:3037` or equivalent in Compose).

`0.0.0.0` binds on all interfaces in the container. Use the minimum required network exposure for your environment.

In `WithoutCallbackListener` mode:

- no local server is started, so `localhost:<redirect_port>` is not actually listened on by this connector.
- a non-zero `redirect_port` is still required because Snowflake uses `BROWSER_MODE_REDIRECT_PORT` to construct the browser redirect URL.
- the browser may show a connection error page at `localhost:<redirect_port>` after login; copy that redirected URL and paste it into the terminal prompt so the connector can extract the token.

## Configuration

### Custom endpoint

To override the default Snowflake endpoint (e.g. for testing or non-default network setups):

```rust
use url::Url;
let auth = SnowflakeAuthConfig::password("PASSWORD");
let endpoint = SnowflakeEndpointConfig::custom_base_url(
    Url::parse("https://custom-host.example.com").unwrap(),
);
let client = SnowflakeClient::new(
    SnowflakeClientConfig::new("USERNAME", "ACCOUNT", auth)
        .with_endpoint(endpoint),
)?;
```

### Proxy

To route requests through an HTTP proxy:

```rust
use url::Url;
let auth = SnowflakeAuthConfig::password("PASSWORD");
let proxy = SnowflakeProxyConfig::new(
    Url::parse("http://proxy.example.com:8080").unwrap(),
)
.with_basic_auth("proxy_user", "proxy_pass");

let transport = SnowflakeTransportConfig::default().with_proxy(proxy);
let client = SnowflakeClient::new(
    SnowflakeClientConfig::new("USERNAME", "ACCOUNT", auth)
        .with_transport(transport),
)?;
```

## Cargo features

- `derive` (enabled by default): re-exports the `FromRow` derive macro.
- `key-pair-auth` (enabled by default): key-pair (JWT) authentication.
- `external-browser-sso`: external browser SSO authentication (experimental; see above).
- `pkcs8-des`: support for DES-encrypted private keys.
- `pkcs8-3des`: support for 3DES-encrypted private keys.

`pkcs8-des` and `pkcs8-3des` exist only for legacy compatibility. DES and 3DES are considered insecure and should not be used for new keys.

## MSRV

The minimum supported Rust version (MSRV) is 1.88.
