use std::io::{self, IsTerminal, Read};
use std::ops::ControlFlow;

use nix::sys::termios::{self, LocalFlags, SetArg, SpecialCharacterIndices, Termios};

use super::validate_redirected_url_input;
use crate::{Error, Result};

pub(super) fn try_read_redirected_url_line_noncanonical() -> Option<Result<String>> {
    let stdin = io::stdin();

    // We intentionally use nix termios wrappers (instead of raw libc calls) so this crate
    // can avoid `unsafe` while still controlling TTY mode.
    // On macOS, canonical mode has a small line-length limit (MAX_CANON), so long redirected
    // URLs can trigger terminal bell/input rejection. Non-canonical mode avoids that limit.
    if !stdin.is_terminal() {
        // Only switch modes for interactive TTY. If stdin is redirected/piped, keep default read_line.
        return None;
    }

    let Ok(_guard) = NonCanonicalModeGuard::new() else {
        return None;
    };

    Some(read_line_noncanonical().and_then(validate_redirected_url_input))
}

fn read_line_noncanonical() -> Result<String> {
    let mut bytes = Vec::new();
    let mut buf = [0u8; 512];
    let stdin = io::stdin();
    let mut handle = stdin.lock();

    loop {
        let n = handle
            .read(&mut buf)
            .map_err(|e| Error::Communication(format!("failed to read input: {e}")))?;
        if n == 0 {
            break;
        }

        match apply_chunk_to_line(bytes, &buf[..n]) {
            LineChunkState::Continue(next_bytes) => bytes = next_bytes,
            LineChunkState::Complete(done_bytes) => return bytes_to_utf8(done_bytes),
        }
    }

    bytes_to_utf8(bytes)
}

enum LineChunkState {
    Continue(Vec<u8>),
    Complete(Vec<u8>),
}

fn apply_chunk_to_line(initial: Vec<u8>, chunk: &[u8]) -> LineChunkState {
    match chunk
        .iter()
        .copied()
        .try_fold(initial, |mut acc, byte| match byte {
            b'\n' | b'\r' => ControlFlow::Break(acc),
            0x08 | 0x7f => {
                // Known limitation: terminal echo behavior may differ by emulator/IME.
                // We only normalize the input buffer and keep terminal-driven rendering.
                let _ = acc.pop();
                ControlFlow::Continue(acc)
            }
            _ => {
                acc.push(byte);
                ControlFlow::Continue(acc)
            }
        }) {
        ControlFlow::Continue(bytes) => LineChunkState::Continue(bytes),
        ControlFlow::Break(done_bytes) => LineChunkState::Complete(done_bytes),
    }
}

fn bytes_to_utf8(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes)
        .map_err(|e| Error::Communication(format!("redirected URL is not valid UTF-8: {e}")))
}

struct NonCanonicalModeGuard {
    stdin: io::Stdin,
    original: Termios,
}

impl NonCanonicalModeGuard {
    fn new() -> io::Result<Self> {
        let stdin = io::stdin();
        let original = termios::tcgetattr(&stdin).map_err(nix_error_to_io)?;
        let mut modified = original.clone();
        modified.local_flags.remove(LocalFlags::ICANON);
        modified.control_chars[SpecialCharacterIndices::VMIN as usize] = 1;
        modified.control_chars[SpecialCharacterIndices::VTIME as usize] = 0;
        termios::tcsetattr(&stdin, SetArg::TCSANOW, &modified).map_err(nix_error_to_io)?;

        Ok(Self { stdin, original })
    }
}

impl Drop for NonCanonicalModeGuard {
    fn drop(&mut self) {
        let _ = termios::tcsetattr(&self.stdin, SetArg::TCSANOW, &self.original);
    }
}

fn nix_error_to_io(err: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(err as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_chunk_to_line_accumulates_without_newline() {
        let state = apply_chunk_to_line(b"abc".to_vec(), b"de");
        match state {
            LineChunkState::Continue(bytes) => assert_eq!(bytes, b"abcde"),
            LineChunkState::Complete(_) => panic!("expected continue state"),
        }
    }

    #[test]
    fn apply_chunk_to_line_stops_at_newline() {
        let state = apply_chunk_to_line(b"abc".to_vec(), b"de\nfg");
        match state {
            LineChunkState::Continue(_) => panic!("expected complete state"),
            LineChunkState::Complete(bytes) => assert_eq!(bytes, b"abcde"),
        }
    }

    #[test]
    fn apply_chunk_to_line_handles_backspace_and_delete() {
        let state = apply_chunk_to_line(Vec::new(), b"ab\x08c\x7fd");
        match state {
            LineChunkState::Continue(bytes) => assert_eq!(bytes, b"ad"),
            LineChunkState::Complete(_) => panic!("expected continue state"),
        }
    }

    #[test]
    fn bytes_to_utf8_accepts_valid_utf8() {
        let text = bytes_to_utf8(b"https://example.test/callback?token=abc".to_vec()).unwrap();
        assert_eq!(text, "https://example.test/callback?token=abc");
    }

    #[test]
    fn bytes_to_utf8_rejects_invalid_utf8() {
        let err = bytes_to_utf8(vec![0xff]).unwrap_err();
        assert!(format!("{err}").contains("not valid UTF-8"));
    }
}
