use std::fmt;

#[cfg(feature = "external-browser-sso")]
use crate::auth::external_browser::ExternalBrowserConfig;

#[derive(Clone)]
pub struct SnowflakeAuthConfig {
    kind: SnowflakeAuthConfigKind,
}

#[derive(Clone)]
pub(crate) enum SnowflakeAuthConfigKind {
    Password(PasswordAuthConfig),
    #[cfg(feature = "key-pair-auth")]
    KeyPair(KeyPairAuthConfig),
    Oauth(OauthAuthConfig),
    #[cfg(feature = "external-browser-sso")]
    ExternalBrowser(ExternalBrowserConfig),
}

impl SnowflakeAuthConfig {
    pub fn password(password: impl Into<String>) -> Self {
        Self {
            kind: SnowflakeAuthConfigKind::Password(PasswordAuthConfig::new(password)),
        }
    }

    #[cfg(feature = "key-pair-auth")]
    pub fn key_pair(config: KeyPairAuthConfig) -> Self {
        Self {
            kind: SnowflakeAuthConfigKind::KeyPair(config),
        }
    }

    pub fn oauth(token: impl Into<String>) -> Self {
        Self {
            kind: SnowflakeAuthConfigKind::Oauth(OauthAuthConfig::new(token)),
        }
    }

    #[cfg(feature = "external-browser-sso")]
    /// External browser SSO authentication.
    ///
    /// This is an experimental feature.
    /// The API and behavior may change in future releases without backward compatibility guarantees.
    ///
    /// ## Typical setup patterns
    ///
    /// ### Default (auto browser launch, localhost callback with auto-picked port)
    ///
    /// ```rust
    /// use snowflake_connector_rs::{ExternalBrowserConfig, SnowflakeAuthConfig};
    ///
    /// let auth = SnowflakeAuthConfig::external_browser(ExternalBrowserConfig::default());
    /// ```
    ///
    /// ### Docker/container mode (manual open + explicit callback bind address/port)
    ///
    /// ```rust
    /// use std::net::Ipv4Addr;
    /// use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthConfig};
    ///
    /// let external_browser = ExternalBrowserConfig::with_callback_listener(
    ///     BrowserLaunchMode::Manual,
    ///     Ipv4Addr::UNSPECIFIED.into(),
    ///     3037,
    /// );
    /// let auth = SnowflakeAuthConfig::external_browser(external_browser);
    /// ```
    ///
    /// ### Without callback listener mode (manual redirected-URL input)
    ///
    /// ```rust
    /// use std::num::NonZeroU16;
    /// use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthConfig};
    ///
    /// let redirect_port = NonZeroU16::new(3037).unwrap();
    /// let external_browser =
    ///     ExternalBrowserConfig::without_callback_listener(BrowserLaunchMode::Manual, redirect_port);
    /// let auth = SnowflakeAuthConfig::external_browser(external_browser);
    /// ```
    pub fn external_browser(config: ExternalBrowserConfig) -> Self {
        Self {
            kind: SnowflakeAuthConfigKind::ExternalBrowser(config),
        }
    }

    pub(crate) fn kind(&self) -> &SnowflakeAuthConfigKind {
        &self.kind
    }
}

impl fmt::Debug for SnowflakeAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            SnowflakeAuthConfigKind::Password(_) => f
                .debug_tuple("SnowflakeAuthConfig::Password")
                .field(&"<redacted>")
                .finish(),
            #[cfg(feature = "key-pair-auth")]
            SnowflakeAuthConfigKind::KeyPair(config) => f
                .debug_tuple("SnowflakeAuthConfig::KeyPair")
                .field(config)
                .finish(),
            SnowflakeAuthConfigKind::Oauth(_) => f
                .debug_tuple("SnowflakeAuthConfig::Oauth")
                .field(&"<redacted>")
                .finish(),
            #[cfg(feature = "external-browser-sso")]
            SnowflakeAuthConfigKind::ExternalBrowser(config) => f
                .debug_tuple("SnowflakeAuthConfig::ExternalBrowser")
                .field(config)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PasswordAuthConfig {
    password: String,
}

impl PasswordAuthConfig {
    fn new(password: impl Into<String>) -> Self {
        Self {
            password: password.into(),
        }
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }
}

#[derive(Clone)]
pub(crate) struct OauthAuthConfig {
    token: String,
}

impl OauthAuthConfig {
    fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

#[cfg(feature = "key-pair-auth")]
#[derive(Clone)]
pub struct KeyPairAuthConfig {
    pem: String,
    password: Option<Vec<u8>>,
}

#[cfg(feature = "key-pair-auth")]
impl KeyPairAuthConfig {
    pub fn encrypted_pem(pem: impl Into<String>, password: impl Into<Vec<u8>>) -> Self {
        Self {
            pem: pem.into(),
            password: Some(password.into()),
        }
    }

    pub fn unencrypted_pem(pem: impl Into<String>) -> Self {
        Self {
            pem: pem.into(),
            password: None,
        }
    }

    pub(crate) fn pem(&self) -> &str {
        &self.pem
    }

    pub(crate) fn password(&self) -> Option<&[u8]> {
        self.password.as_deref()
    }
}

#[cfg(feature = "key-pair-auth")]
impl fmt::Debug for KeyPairAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyPairAuthConfig")
            .field("pem", &"<redacted>")
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_debug_redacts_secret() {
        let debug = format!("{:?}", SnowflakeAuthConfig::password("secret"));
        assert!(debug.contains("SnowflakeAuthConfig::Password"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn oauth_debug_redacts_secret() {
        let debug = format!("{:?}", SnowflakeAuthConfig::oauth("oauth-token"));
        assert!(debug.contains("SnowflakeAuthConfig::Oauth"));
        assert!(!debug.contains("oauth-token"));
    }

    #[cfg(feature = "key-pair-auth")]
    #[test]
    fn key_pair_debug_redacts_secret() {
        let debug = format!(
            "{:?}",
            SnowflakeAuthConfig::key_pair(KeyPairAuthConfig::encrypted_pem(
                "pem-body",
                b"super-secret".to_vec(),
            ))
        );
        assert!(debug.contains("SnowflakeAuthConfig::KeyPair"));
        assert!(!debug.contains("pem-body"));
        assert!(!debug.contains("super-secret"));
    }
}
