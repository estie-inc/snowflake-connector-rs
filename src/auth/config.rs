use std::fmt;

#[cfg(feature = "external-browser-sso")]
use crate::auth::external_browser::ExternalBrowserConfig;

#[derive(Clone)]
pub struct AuthConfig {
    kind: AuthConfigKind,
}

#[derive(Clone)]
pub(crate) enum AuthConfigKind {
    Password(PasswordConfig),
    #[cfg(feature = "key-pair-auth")]
    KeyPair(KeyPairConfig),
    OAuth(OAuthConfig),
    #[cfg(feature = "external-browser-sso")]
    ExternalBrowser(ExternalBrowserConfig),
}

impl AuthConfig {
    /// Username/password authentication, optionally with an MFA passcode.
    ///
    /// Pass a `&str`/`String`, or a [`PasswordConfig`] to attach a TOTP passcode.
    pub fn password(config: impl Into<PasswordConfig>) -> Self {
        Self {
            kind: AuthConfigKind::Password(config.into()),
        }
    }

    #[cfg(feature = "key-pair-auth")]
    pub fn key_pair(config: KeyPairConfig) -> Self {
        Self {
            kind: AuthConfigKind::KeyPair(config),
        }
    }

    /// Authenticate with a Snowflake OAuth access token. Acquiring and refreshing the token is the caller's responsibility.
    pub fn oauth(token: impl Into<String>) -> Self {
        Self {
            kind: AuthConfigKind::OAuth(OAuthConfig::new(token)),
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
    /// use snowflake_connector_rs::{ExternalBrowserConfig, AuthConfig};
    ///
    /// let auth = AuthConfig::external_browser(ExternalBrowserConfig::default());
    /// ```
    ///
    /// ### Docker/container mode (manual open + explicit callback bind address/port)
    ///
    /// ```rust
    /// use std::net::Ipv4Addr;
    /// use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, AuthConfig};
    ///
    /// let external_browser = ExternalBrowserConfig::callback_listener(
    ///     BrowserLaunchMode::Manual,
    ///     Ipv4Addr::UNSPECIFIED.into(),
    ///     3037,
    /// );
    /// let auth = AuthConfig::external_browser(external_browser);
    /// ```
    ///
    /// ### Without callback listener mode (manual redirected-URL input)
    ///
    /// ```rust
    /// use std::num::NonZeroU16;
    /// use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, AuthConfig};
    ///
    /// let redirect_port = NonZeroU16::new(3037).unwrap();
    /// let external_browser =
    ///     ExternalBrowserConfig::manual_redirect(BrowserLaunchMode::Manual, redirect_port);
    /// let auth = AuthConfig::external_browser(external_browser);
    /// ```
    pub fn external_browser(config: ExternalBrowserConfig) -> Self {
        Self {
            kind: AuthConfigKind::ExternalBrowser(config),
        }
    }

    pub(crate) fn kind(&self) -> &AuthConfigKind {
        &self.kind
    }
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            AuthConfigKind::Password(config) => {
                f.debug_tuple("AuthConfig::Password").field(config).finish()
            }
            #[cfg(feature = "key-pair-auth")]
            AuthConfigKind::KeyPair(config) => {
                f.debug_tuple("AuthConfig::KeyPair").field(config).finish()
            }
            AuthConfigKind::OAuth(_) => f
                .debug_tuple("AuthConfig::OAuth")
                .field(&"<redacted>")
                .finish(),
            #[cfg(feature = "external-browser-sso")]
            AuthConfigKind::ExternalBrowser(config) => f
                .debug_tuple("AuthConfig::ExternalBrowser")
                .field(config)
                .finish(),
        }
    }
}

/// Password authentication, optionally carrying an MFA passcode.
#[derive(Clone)]
pub struct PasswordConfig {
    password: String,
    passcode: Option<PasscodeMode>,
}

#[derive(Clone)]
pub(crate) enum PasscodeMode {
    Separate(String),
    InPassword,
}

impl PasswordConfig {
    /// Password without an MFA passcode.
    pub fn new(password: impl Into<String>) -> Self {
        Self {
            password: password.into(),
            passcode: None,
        }
    }

    /// Attach a TOTP passcode, sent to Snowflake as a separate `PASSCODE`.
    pub fn with_passcode(mut self, passcode: impl Into<String>) -> Self {
        self.passcode = Some(PasscodeMode::Separate(passcode.into()));
        self
    }

    /// Signal that the passcode is already appended to the password passed to [`PasswordConfig::new`];
    /// no separate `PASSCODE` is sent.
    pub fn with_passcode_in_password(mut self) -> Self {
        self.passcode = Some(PasscodeMode::InPassword);
        self
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    pub(crate) fn passcode(&self) -> Option<&PasscodeMode> {
        self.passcode.as_ref()
    }
}

impl From<&str> for PasswordConfig {
    fn from(password: &str) -> Self {
        Self::new(password)
    }
}

impl From<String> for PasswordConfig {
    fn from(password: String) -> Self {
        Self::new(password)
    }
}

impl fmt::Debug for PasswordConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Expose only which passcode mode is set, not its value.
        let passcode = match self.passcode {
            None => "none",
            Some(PasscodeMode::Separate(_)) => "passcode",
            Some(PasscodeMode::InPassword) => "in-password",
        };
        f.debug_struct("PasswordConfig")
            .field("password", &"<redacted>")
            .field("passcode", &passcode)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct OAuthConfig {
    token: String,
}

impl OAuthConfig {
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
pub struct KeyPairConfig {
    pem: String,
    password: Option<Vec<u8>>,
}

#[cfg(feature = "key-pair-auth")]
impl KeyPairConfig {
    pub fn from_encrypted_pem(pem: impl Into<String>, password: impl Into<Vec<u8>>) -> Self {
        Self {
            pem: pem.into(),
            password: Some(password.into()),
        }
    }

    pub fn from_pem(pem: impl Into<String>) -> Self {
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
impl fmt::Debug for KeyPairConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyPairConfig")
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
        let debug = format!("{:?}", AuthConfig::password("secret"));
        assert!(debug.contains("AuthConfig::Password"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn password_debug_redacts_passcode() {
        let debug = format!(
            "{:?}",
            AuthConfig::password(PasswordConfig::new("secret").with_passcode("123456"))
        );
        assert!(debug.contains("AuthConfig::Password"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("123456"));
    }

    #[test]
    fn password_passcode_modes_are_mutually_exclusive_last_wins() {
        assert!(PasswordConfig::new("pw").passcode().is_none());
        assert!(matches!(
            PasswordConfig::new("pw").with_passcode("123456").passcode(),
            Some(PasscodeMode::Separate(code)) if code == "123456"
        ));
        assert!(matches!(
            PasswordConfig::new("pw")
                .with_passcode_in_password()
                .passcode(),
            Some(PasscodeMode::InPassword)
        ));
        // Setting one mode after another overwrites rather than accumulating.
        assert!(matches!(
            PasswordConfig::new("pw")
                .with_passcode("123456")
                .with_passcode_in_password()
                .passcode(),
            Some(PasscodeMode::InPassword)
        ));
    }

    #[test]
    fn oauth_debug_redacts_secret() {
        let debug = format!("{:?}", AuthConfig::oauth("oauth-token"));
        assert!(debug.contains("AuthConfig::OAuth"));
        assert!(!debug.contains("oauth-token"));
    }

    #[cfg(feature = "key-pair-auth")]
    #[test]
    fn key_pair_debug_redacts_secret() {
        let debug = format!(
            "{:?}",
            AuthConfig::key_pair(KeyPairConfig::from_encrypted_pem(
                "pem-body",
                b"super-secret".to_vec(),
            ))
        );
        assert!(debug.contains("AuthConfig::KeyPair"));
        assert!(!debug.contains("pem-body"));
        assert!(!debug.contains("super-secret"));
    }
}
