use std::io::Read;

use bytes::Bytes;
use flate2::bufread::GzDecoder;

use crate::error::ProtocolError;

pub(crate) fn decode_gzip_chunk(body: Bytes) -> std::result::Result<Bytes, ProtocolError> {
    if body.is_empty() {
        return Ok(body);
    }

    if body.len() < 2 {
        return Err(ProtocolError::chunk_format("invalid chunk format"));
    }

    if body[0] == 0x1f && body[1] == 0x8b {
        let mut decoder = GzDecoder::new(&body[..]);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .map_err(ProtocolError::gzip_decode)?;
        Ok(Bytes::from(decoded))
    } else {
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;
    use crate::{Error, ErrorKind};

    #[test]
    fn malformed_gzip_is_protocol_error() {
        let err: Error = decode_gzip_chunk(Bytes::from_static(b"\x1f\x8bgarbage"))
            .unwrap_err()
            .into();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert!(err.to_string().contains("gzip decompression failed"));
        assert!(StdError::source(&err).is_some());
    }
}
