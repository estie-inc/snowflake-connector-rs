use std::{
    net::{IpAddr, Ipv4Addr},
    num::NonZeroU16,
};

/// Controls how the SSO URL is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BrowserLaunchMode {
    /// Try to launch a local browser command first.
    Auto,
    /// Skip browser launch commands and only print the SSO URL.
    Manual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CallbackListenerConfig {
    browser_launch_mode: BrowserLaunchMode,
    callback_socket_addr: IpAddr,
    callback_socket_port: u16,
}

impl Default for CallbackListenerConfig {
    /// Returns the default callback-listener configuration.
    ///
    /// - `browser_launch_mode = BrowserLaunchMode::Auto`
    /// - `callback_socket_addr = 127.0.0.1`
    /// - `callback_socket_port = 0` (OS-selected ephemeral port)
    fn default() -> Self {
        Self {
            browser_launch_mode: BrowserLaunchMode::Auto,
            callback_socket_addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
            callback_socket_port: 0,
        }
    }
}

impl CallbackListenerConfig {
    pub(crate) fn new(
        browser_launch_mode: BrowserLaunchMode,
        callback_socket_addr: IpAddr,
        callback_socket_port: u16,
    ) -> Self {
        Self {
            browser_launch_mode,
            callback_socket_addr,
            callback_socket_port,
        }
    }

    pub(crate) fn browser_launch_mode(&self) -> BrowserLaunchMode {
        self.browser_launch_mode
    }

    pub(crate) fn callback_socket_addr(&self) -> IpAddr {
        self.callback_socket_addr
    }

    pub(crate) fn callback_socket_port(&self) -> u16 {
        self.callback_socket_port
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManualRedirectConfig {
    browser_launch_mode: BrowserLaunchMode,
    redirect_port: NonZeroU16,
}

impl ManualRedirectConfig {
    pub(crate) fn new(browser_launch_mode: BrowserLaunchMode, redirect_port: NonZeroU16) -> Self {
        Self {
            browser_launch_mode,
            redirect_port,
        }
    }

    pub(crate) fn browser_launch_mode(&self) -> BrowserLaunchMode {
        self.browser_launch_mode
    }

    pub(crate) fn redirect_port(&self) -> NonZeroU16 {
        self.redirect_port
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Configuration for [`AuthConfig::external_browser`](crate::AuthConfig::external_browser).
///
/// Use [`ExternalBrowserConfig::callback_listener`] or [`ExternalBrowserConfig::manual_redirect`] to choose the authentication mode.
/// For end-to-end setup examples, see [`AuthConfig::external_browser`](crate::AuthConfig::external_browser).
pub struct ExternalBrowserConfig {
    mode: ExternalBrowserMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ExternalBrowserMode {
    CallbackListener(CallbackListenerConfig),
    ManualRedirect(ManualRedirectConfig),
}

impl Default for ExternalBrowserConfig {
    /// Returns the default external-browser configuration.
    ///
    /// Equivalent to callback-listener mode with:
    /// - `browser_launch_mode = BrowserLaunchMode::Auto`
    /// - `callback_socket_addr = 127.0.0.1`
    /// - `callback_socket_port = 0` (OS-selected ephemeral port)
    fn default() -> Self {
        Self {
            mode: ExternalBrowserMode::CallbackListener(CallbackListenerConfig::default()),
        }
    }
}

impl ExternalBrowserConfig {
    /// Creates a callback-listener external-browser configuration.
    ///
    /// This mode starts a local HTTP listener and receives the token automatically from the redirected callback URL.
    ///
    /// - `browser_launch_mode`: controls whether the auth URL is opened automatically (`Auto`) or only printed for manual open (`Manual`).
    /// - `callback_socket_addr`: bind address for the local callback listener (for example `127.0.0.1` or `0.0.0.0`).
    /// - `callback_socket_port`: bind port for the callback listener. Use `0` to let the OS pick an available ephemeral port.
    pub fn callback_listener(
        browser_launch_mode: BrowserLaunchMode,
        callback_socket_addr: IpAddr,
        callback_socket_port: u16,
    ) -> Self {
        Self {
            mode: ExternalBrowserMode::CallbackListener(CallbackListenerConfig::new(
                browser_launch_mode,
                callback_socket_addr,
                callback_socket_port,
            )),
        }
    }

    /// Creates a manual-redirect external-browser configuration.
    ///
    /// This mode does not start a local listener. After login, paste the redirected URL shown by the browser into the terminal prompt.
    ///
    /// - `browser_launch_mode`: controls whether the auth URL is opened automatically (`Auto`) or only printed for manual open (`Manual`).
    /// - `redirect_port`: port embedded in `BROWSER_MODE_REDIRECT_PORT` for Snowflake. No local server is started in this mode,
    ///   so this port does not need to be actually listening. It is still required because Snowflake uses this value to construct
    ///   the browser redirect URL that you later paste into the terminal; the connector then extracts the token from that pasted URL.
    pub fn manual_redirect(
        browser_launch_mode: BrowserLaunchMode,
        redirect_port: NonZeroU16,
    ) -> Self {
        Self {
            mode: ExternalBrowserMode::ManualRedirect(ManualRedirectConfig::new(
                browser_launch_mode,
                redirect_port,
            )),
        }
    }

    pub(super) fn mode(&self) -> &ExternalBrowserMode {
        &self.mode
    }
}
