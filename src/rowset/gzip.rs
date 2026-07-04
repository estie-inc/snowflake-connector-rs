use std::io::Read;

use bytes::Bytes;
use flate2::bufread::GzDecoder;

use crate::error::ProtocolError;

const MAX_GZIP_PREALLOCATED_BYTES: usize = 128 * 1024 * 1024;

pub(crate) fn decode_gzip_chunk(
    body: Bytes,
    expected_uncompressed_size: Option<usize>,
) -> std::result::Result<Bytes, ProtocolError> {
    if body.is_empty() {
        return Ok(body);
    }

    if body.len() < 2 {
        return Err(ProtocolError::chunk_format("invalid chunk format"));
    }

    if body[0] == 0x1f && body[1] == 0x8b {
        let mut decoder = GzDecoder::new(&body[..]);
        let mut decoded =
            Vec::with_capacity(gzip_preallocated_capacity(expected_uncompressed_size));
        decoder
            .read_to_end(&mut decoded)
            .map_err(ProtocolError::gzip_decode)?;
        Ok(Bytes::from(decoded))
    } else {
        Ok(body)
    }
}

fn gzip_preallocated_capacity(expected_uncompressed_size: Option<usize>) -> usize {
    expected_uncompressed_size
        .filter(|size| *size <= MAX_GZIP_PREALLOCATED_BYTES)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;
    use crate::{Error, ErrorKind};

    #[test]
    fn malformed_gzip_is_protocol_error() {
        let err: Error = decode_gzip_chunk(Bytes::from_static(b"\x1f\x8bgarbage"), None)
            .unwrap_err()
            .into();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert!(err.to_string().contains("gzip decompression failed"));
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn gzip_capacity_hint_uses_bounded_expected_size() {
        assert_eq!(gzip_preallocated_capacity(Some(1024)), 1024);
        assert_eq!(
            gzip_preallocated_capacity(Some(MAX_GZIP_PREALLOCATED_BYTES)),
            MAX_GZIP_PREALLOCATED_BYTES
        );
        assert_eq!(
            gzip_preallocated_capacity(Some(MAX_GZIP_PREALLOCATED_BYTES + 1)),
            0
        );
        assert_eq!(gzip_preallocated_capacity(None), 0);
    }
}
