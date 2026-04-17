use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Controls how the SSO URL is opened.
pub enum BrowserLaunchMode {
    /// Try to launch a local browser command first.
    Auto,
    /// Skip browser launch commands and only print the SSO URL.
    Manual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WithCallbackListenerConfig {
    browser_launch_mode: BrowserLaunchMode,
    callback_socket_addr: IpAddr,
    callback_socket_port: u16,
}

impl Default for WithCallbackListenerConfig {
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

impl WithCallbackListenerConfig {
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
pub struct WithoutCallbackListenerConfig {
    browser_launch_mode: BrowserLaunchMode,
    redirect_port: NonZeroU16,
}

impl WithoutCallbackListenerConfig {
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
/// Configuration for `SnowflakeAuthMethod::ExternalBrowser`.
///
/// Use this type to choose one of two authentication modes:
/// `WithCallbackListener` or `WithoutCallbackListener`.
/// For end-to-end setup examples, see `SnowflakeAuthMethod::ExternalBrowser`.
pub enum ExternalBrowserConfig {
    WithCallbackListener(WithCallbackListenerConfig),
    WithoutCallbackListener(WithoutCallbackListenerConfig),
}

impl Default for ExternalBrowserConfig {
    /// Returns the default external-browser configuration.
    ///
    /// Equivalent to callback-listener mode with:
    /// - `browser_launch_mode = BrowserLaunchMode::Auto`
    /// - `callback_socket_addr = 127.0.0.1`
    /// - `callback_socket_port = 0` (OS-selected ephemeral port)
    fn default() -> Self {
        Self::WithCallbackListener(WithCallbackListenerConfig::default())
    }
}

impl ExternalBrowserConfig {
    /// Creates `ExternalBrowserConfig::WithCallbackListener`.
    ///
    /// This mode starts a local HTTP listener and receives the token automatically
    /// from the redirected callback URL.
    ///
    /// - `browser_launch_mode`: controls whether the auth URL is opened automatically (`Auto`)
    ///   or only printed for manual open (`Manual`).
    /// - `callback_socket_addr`: bind address for the local callback listener
    ///   (for example `127.0.0.1` or `0.0.0.0`).
    /// - `callback_socket_port`: bind port for the callback listener.
    ///   Use `0` to let the OS pick an available ephemeral port.
    pub fn with_callback_listener(
        browser_launch_mode: BrowserLaunchMode,
        callback_socket_addr: IpAddr,
        callback_socket_port: u16,
    ) -> Self {
        ExternalBrowserConfig::WithCallbackListener(WithCallbackListenerConfig::new(
            browser_launch_mode,
            callback_socket_addr,
            callback_socket_port,
        ))
    }

    /// Creates `ExternalBrowserConfig::WithoutCallbackListener`.
    ///
    /// This mode does not start a local listener. After login, paste the redirected
    /// URL shown by the browser into the terminal prompt.
    ///
    /// - `browser_launch_mode`: controls whether the auth URL is opened automatically (`Auto`)
    ///   or only printed for manual open (`Manual`).
    /// - `redirect_port`: port embedded in `BROWSER_MODE_REDIRECT_PORT` for Snowflake.
    ///   No local server is started in this mode, so this port does not need to be
    ///   actually listening.
    ///   It is still required because Snowflake uses this value to construct the
    ///   browser redirect URL that you later paste into the terminal; the connector
    ///   then extracts the token from that pasted URL.
    pub fn without_callback_listener(
        browser_launch_mode: BrowserLaunchMode,
        redirect_port: NonZeroU16,
    ) -> Self {
        ExternalBrowserConfig::WithoutCallbackListener(WithoutCallbackListenerConfig::new(
            browser_launch_mode,
            redirect_port,
        ))
    }
}
