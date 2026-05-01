use std::io::Read;

use bytes::Bytes;
use flate2::bufread::GzDecoder;

use crate::{Error, Result};

pub(crate) fn decode_gzip_chunk(body: Bytes) -> Result<Bytes> {
    if body.is_empty() {
        return Ok(body);
    }

    if body.len() < 2 {
        return Err(Error::ChunkDownload("invalid chunk format".into()));
    }

    if body[0] == 0x1f && body[1] == 0x8b {
        let mut decoder = GzDecoder::new(&body[..]);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded)?;
        Ok(Bytes::from(decoded))
    } else {
        Ok(body)
    }
}
