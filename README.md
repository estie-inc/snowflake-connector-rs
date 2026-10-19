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

let session_config = SessionConfig::new()
    .with_role("ROLE")
    .with_warehouse("WAREHOUSE")
    .with_database("DATABASE")
    .with_schema("SCHEMA");

let query_config = QueryConfig::new()
    // Client-side timeout for obtaining a query response.
    .with_query_response_timeout(std::time::Duration::from_secs(60));

let client = Client::new(
    ClientConfig::new(
        "USERNAME",
        "ACCOUNT",
        AuthConfig::password("PASSWORD"),
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

// The following SELECT examples show alternative consumption styles.
// In application code, choose the one that fits your use case.

let dynamic_rows = session
    .query("SELECT id, value FROM example ORDER BY id")
    .await?
    .collect::<Vec<_>>()
    .await?;
assert_eq!(dynamic_rows.len(), 2);

let rows = session
    .query_as("SELECT id, value FROM example ORDER BY id")
    .await?
    .collect::<Vec<ExampleRow>>()
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
```

## Authentication

The authentication method is chosen with `AuthConfig`, passed to `ClientConfig::new`.

### Key-pair

Key-pair (JWT) authentication. Recommended for programmatic and unattended access. Requires the `key-pair-auth` feature (enabled by default).

```rust
use snowflake_connector_rs::{KeyPairConfig, AuthConfig};

// Encrypted PKCS#8 PEM, with its passphrase:
let auth = AuthConfig::key_pair(
    KeyPairConfig::from_encrypted_pem(pem, b"passphrase".to_vec()),
);

// Unencrypted PKCS#8 PEM:
let auth = AuthConfig::key_pair(KeyPairConfig::from_pem(pem));
```

### OAuth

Authenticate with a Snowflake OAuth access token. Acquiring and refreshing the token is the caller's responsibility.

```rust
use snowflake_connector_rs::AuthConfig;

let auth = AuthConfig::oauth("OAUTH_ACCESS_TOKEN");
```

### Password and MFA

Snowflake enforces MFA for password sign-ins. Without a passcode, login relies on an out-of-band approval (for example a Duo push) that must be approved interactively, so plain password auth is unsuitable for unattended use — prefer key-pair or OAuth there.

```rust
use snowflake_connector_rs::{PasswordConfig, AuthConfig};

// Plain password (an interactive MFA approval may still be required).
let auth = AuthConfig::password("PASSWORD");

// TOTP passcode sent as a separate value.
let auth = AuthConfig::password(
    PasswordConfig::new("PASSWORD").with_passcode("123456"),
);

// Or the passcode already appended to the password.
let auth = AuthConfig::password(
    PasswordConfig::new("PASSWORD123456").with_passcode_in_password(),
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
   use snowflake_connector_rs::{ExternalBrowserConfig, AuthConfig};
   let auth = AuthConfig::external_browser(ExternalBrowserConfig::default());
   ```
- Docker/container setup (manual open with explicit callback bind address/port)
   ```rust
    use std::net::Ipv4Addr;
    use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, AuthConfig};
    let external_browser = ExternalBrowserConfig::callback_listener(
        BrowserLaunchMode::Manual,
        Ipv4Addr::UNSPECIFIED.into(),
        3037,
    );
    let auth = AuthConfig::external_browser(external_browser);
    ```
- Without callback listener mode (manual redirected URL input)
   ```rust
    use std::num::NonZeroU16;
    use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, AuthConfig};
    let redirect_port = NonZeroU16::new(3037).unwrap();
    let external_browser =
        ExternalBrowserConfig::manual_redirect(BrowserLaunchMode::Manual, redirect_port);
    let auth = AuthConfig::external_browser(external_browser);
    ```

For Docker/container setup, make sure that:

- your Snowflake OAuth redirect URI allows the same callback port (for example `3037`), and
- the callback port is mapped to the host (for example `-p 3037:3037` or equivalent in Compose).

`0.0.0.0` binds on all interfaces in the container. Use the minimum required network exposure for your environment.

In `ManualRedirect` mode:

- no local server is started, so `localhost:<redirect_port>` is not actually listened on by this connector.
- a non-zero `redirect_port` is still required because Snowflake uses `BROWSER_MODE_REDIRECT_PORT` to construct the browser redirect URL.
- the browser may show a connection error page at `localhost:<redirect_port>` after login; copy that redirected URL and paste it into the terminal prompt so the connector can extract the token.

#### Reusing the login with an ID token

Snowflake can return an ID token from an external browser SSO login, so later sessions can log in without opening the browser while the token is valid (as the official drivers do with `client_store_temporary_credential`). This requires:

- `ALLOW_ID_TOKEN = TRUE` on the account,
- the `CLIENT_STORE_TEMPORARY_CREDENTIAL` session parameter on the SSO login, and
- a client application identity Snowflake recognizes, set with `ClientConfig::with_client_app` (the server does not issue the token otherwise). The identity can change server defaults; for example, with the Go driver's identity the server returns query results in Arrow unless `GO_QUERY_RESULT_FORMAT` is set to `JSON`.

```rust
use snowflake_connector_rs::{AuthConfig, ClientConfig, ExternalBrowserConfig, SessionConfig};

let session_config = SessionConfig::new()
    .with_session_parameter("CLIENT_STORE_TEMPORARY_CREDENTIAL", serde_json::Value::Bool(true));
let config = ClientConfig::new(
    "USERNAME",
    "ACCOUNT",
    AuthConfig::external_browser(ExternalBrowserConfig::default()),
)
.with_session(session_config)
.with_client_app("APP_ID", "APP_VERSION");

// After `client.create_session().await?`, keep `session.id_token()` somewhere safe and log in later with:
let auth = AuthConfig::id_token("STORED_ID_TOKEN");
```

Storing the token and falling back to the browser once Snowflake rejects it (for example after it expires) is up to the application.

## Configuration

### Custom endpoint

To override the default Snowflake endpoint (e.g. for testing or non-default network setups):

```rust
use url::Url;
let auth = AuthConfig::password("PASSWORD");
let endpoint = EndpointConfig::custom_base_url(
    Url::parse("https://custom-host.example.com").unwrap(),
);
let client = Client::new(
    ClientConfig::new("USERNAME", "ACCOUNT", auth)
        .with_endpoint(endpoint),
)?;
```

### Proxy

To route requests through an HTTP proxy:

```rust
use url::Url;
let auth = AuthConfig::password("PASSWORD");
let proxy = ProxyConfig::new(
    Url::parse("http://proxy.example.com:8080").unwrap(),
)
.with_basic_auth("proxy_user", "proxy_pass");

let transport = TransportConfig::new().with_proxy(proxy);
let client = Client::new(
    ClientConfig::new("USERNAME", "ACCOUNT", auth)
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
