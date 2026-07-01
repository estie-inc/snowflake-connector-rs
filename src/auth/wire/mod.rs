pub(crate) mod login_request;
pub(crate) mod login_response;

#[cfg(feature = "external-browser-sso")]
pub(crate) mod authenticator_request;
#[cfg(feature = "external-browser-sso")]
pub(crate) mod authenticator_response;

#[cfg(feature = "external-browser-sso")]
pub(crate) use authenticator_request::{AuthenticatorRequest, ClientEnvironment};
#[cfg(feature = "external-browser-sso")]
pub(crate) use authenticator_response::{ExternalBrowserChallenge, parse_authenticator_response};
pub(crate) use login_request::{
    LoginBody, LoginCredentialWire, LoginData, LoginQuery, LoginRequest, PasscodeWire,
};
pub(crate) use login_response::{LoginSession, parse_login_response};
