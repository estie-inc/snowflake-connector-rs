use std::env;
use std::io;
use std::process::{Command, Stdio};

use indexmap::IndexSet;
use thiserror::Error;

/// Result of attempting to launch a browser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaunchOutcome {
    /// A browser command was successfully started.
    Opened,
    /// No browser command succeeded; caller should ask user to open the URL manually.
    ManualOpen { url: String },
}

#[derive(Debug, Error)]
pub(crate) enum BrowserError {
    #[error("URL must not be empty")]
    EmptyUrl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Platform {
    Windows,
    Mac,
    Unix,
}

impl Platform {
    fn current() -> Self {
        if cfg!(target_os = "windows") {
            Platform::Windows
        } else if cfg!(target_os = "macos") {
            Platform::Mac
        } else {
            Platform::Unix
        }
    }
}

/// Abstraction over spawning a process. Primarily used for testing.
pub(crate) trait CommandRunner: Send {
    fn spawn(&self, program: &str, args: &[String]) -> io::Result<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn spawn(&self, program: &str, args: &[String]) -> io::Result<()> {
        Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map(|_| ())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BrowserLauncher<R: CommandRunner = SystemCommandRunner> {
    runner: R,
    platform: Platform,
}

impl BrowserLauncher<SystemCommandRunner> {
    pub(crate) fn new() -> Self {
        Self {
            runner: SystemCommandRunner,
            platform: Platform::current(),
        }
    }
}

impl<R: CommandRunner> BrowserLauncher<R> {
    pub(crate) fn open(&self, url: &str) -> Result<LaunchOutcome, BrowserError> {
        if url.trim().is_empty() {
            return Err(BrowserError::EmptyUrl);
        }

        let env_browser = env::var("BROWSER").ok();
        let candidates = resolve_candidates(url, self.platform, env_browser.as_deref());

        for candidate in candidates {
            if self
                .runner
                .spawn(&candidate.program, &candidate.args)
                .is_ok()
            {
                return Ok(LaunchOutcome::Opened);
            }
        }

        Ok(LaunchOutcome::ManualOpen {
            url: url.to_string(),
        })
    }

    /// Message that can be shown to the user when manual opening is required.
    pub(crate) fn manual_open_message(url: &str) -> String {
        format!("Could not open a browser automatically. Please open this URL manually:\n{url}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CommandSpec {
    program: String,
    args: Vec<String>,
}

fn resolve_candidates(
    url: &str,
    platform: Platform,
    env_browser: Option<&str>,
) -> IndexSet<CommandSpec> {
    let mut candidates = IndexSet::new();

    if let Some(browser) = env_browser {
        let env_candidates = parse_browser_env(browser, url, platform);
        candidates.extend(env_candidates);
    }

    candidates.extend(default_commands(url, platform));
    candidates
}

fn default_commands(url: &str, platform: Platform) -> Vec<CommandSpec> {
    match platform {
        Platform::Windows => vec![
            CommandSpec {
                program: "cmd".into(),
                args: vec!["/C".into(), "start".into(), "".into(), url.into()],
            },
            CommandSpec {
                program: "rundll32".into(),
                args: vec!["url.dll,FileProtocolHandler".into(), url.into()],
            },
        ],
        Platform::Mac => vec![CommandSpec {
            program: "open".into(),
            args: vec![url.into()],
        }],
        Platform::Unix => vec![
            CommandSpec {
                program: "xdg-open".into(),
                args: vec![url.into()],
            },
            CommandSpec {
                program: "gio".into(),
                args: vec!["open".into(), "--".into(), url.into()],
            },
            CommandSpec {
                program: "sensible-browser".into(),
                args: vec![url.into()],
            },
            CommandSpec {
                program: "www-browser".into(),
                args: vec![url.into()],
            },
        ],
    }
}

fn parse_browser_env(value: &str, url: &str, platform: Platform) -> Vec<CommandSpec> {
    let separator = match platform {
        Platform::Windows => ';',
        _ => ':',
    };
    value
        .split(separator)
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| parse_browser_entry(entry.trim(), url))
        .collect()
}

fn parse_browser_entry(entry: &str, url: &str) -> CommandSpec {
    // Use shell-words to properly handle quoted paths like:
    // BROWSER="C:\\Program Files\\Browser\\browser.exe" %s
    let mut parts = shell_words::split(entry).unwrap_or_else(|_| {
        // Fallback to simple whitespace split if shell-words fails (e.g., unmatched quotes)
        entry.split_whitespace().map(|s| s.to_string()).collect()
    });
    if parts.is_empty() {
        return CommandSpec {
            program: entry.to_string(),
            args: vec![url.to_string()],
        };
    }

    let program = parts.remove(0);
    let mut args = Vec::new();
    let mut has_placeholder = false;

    for part in parts {
        if part.contains("%s") {
            has_placeholder = true;
            args.push(part.replace("%s", url));
        } else {
            args.push(part);
        }
    }

    if !has_placeholder {
        args.push(url.to_string());
    }

    CommandSpec { program, args }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    #[derive(Default)]
    struct TestRunner {
        succeed_on: RefCell<Option<String>>,
        calls: RefCell<Vec<CommandSpec>>,
    }

    impl TestRunner {
        fn succeed_on(program: &str) -> Self {
            Self {
                succeed_on: RefCell::new(Some(program.to_string())),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl CommandRunner for TestRunner {
        fn spawn(&self, program: &str, args: &[String]) -> io::Result<()> {
            self.calls.borrow_mut().push(CommandSpec {
                program: program.to_string(),
                args: args.to_vec(),
            });
            if self
                .succeed_on
                .borrow()
                .as_ref()
                .map(|target| target == program)
                .unwrap_or(false)
            {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "not found"))
            }
        }
    }

    #[test]
    fn parses_browser_env_with_placeholder() {
        let url = "https://example.com";
        let value = "firefox -new-window %s:lynx";
        let cmds = parse_browser_env(value, url, Platform::Unix);
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0].program, "firefox");
        assert_eq!(
            cmds[0].args,
            vec!["-new-window".to_string(), url.to_string()]
        );
        assert_eq!(cmds[1].program, "lynx");
        assert_eq!(cmds[1].args, vec![url.to_string()]);
    }

    #[test]
    fn picks_first_successful_command() {
        let runner = TestRunner::succeed_on("gio");
        let launcher = BrowserLauncher {
            runner,
            platform: Platform::Unix,
        };
        let outcome = launcher.open("https://example.com").unwrap();
        assert_eq!(outcome, LaunchOutcome::Opened);
    }

    #[test]
    fn falls_back_to_manual_when_all_fail() {
        let runner = TestRunner::default();
        let launcher = BrowserLauncher {
            runner,
            platform: Platform::Unix,
        };
        let outcome = launcher.open("https://example.com").unwrap();
        assert_eq!(
            outcome,
            LaunchOutcome::ManualOpen {
                url: "https://example.com".into()
            }
        );
    }

    #[test]
    fn rejects_empty_url() {
        let launcher = BrowserLauncher::new();
        let err = launcher.open(" ").unwrap_err();
        assert!(matches!(err, BrowserError::EmptyUrl));
    }
}
