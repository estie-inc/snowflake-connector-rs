pub(crate) enum JsonStringFragment<'a> {
    Raw {
        bytes: &'a [u8],
        start_offset: usize,
    },
    DecodedByte(u8),
    Codepoint(u32),
}

pub(crate) enum JsonStringScanError<E> {
    UnterminatedString { offset: usize },
    TrailingBackslash { offset: usize },
    UnknownEscape { offset: usize, escape: u8 },
    ControlCharacter { offset: usize },
    InvalidUnicodeEscape { offset: usize },
    InvalidUnicodeSurrogatePair { offset: usize },
    Visitor(E),
}

pub(crate) fn scan_json_string_body<E>(
    bytes: &[u8],
    offset: &mut usize,
    mut visit: impl FnMut(JsonStringFragment<'_>) -> std::result::Result<(), E>,
) -> std::result::Result<(), JsonStringScanError<E>> {
    let mut segment_start = *offset;
    loop {
        let Some(&byte) = bytes.get(*offset) else {
            return Err(JsonStringScanError::UnterminatedString { offset: *offset });
        };

        match byte {
            b'"' => {
                if segment_start < *offset {
                    visit(JsonStringFragment::Raw {
                        bytes: &bytes[segment_start..*offset],
                        start_offset: segment_start,
                    })
                    .map_err(JsonStringScanError::Visitor)?;
                }
                *offset += 1;
                return Ok(());
            }
            b'\\' => {
                if segment_start < *offset {
                    visit(JsonStringFragment::Raw {
                        bytes: &bytes[segment_start..*offset],
                        start_offset: segment_start,
                    })
                    .map_err(JsonStringScanError::Visitor)?;
                }
                *offset += 1;
                let Some(&escape) = bytes.get(*offset) else {
                    return Err(JsonStringScanError::TrailingBackslash { offset: *offset });
                };
                *offset += 1;
                match escape {
                    b'"' => visit(JsonStringFragment::DecodedByte(b'"')),
                    b'\\' => visit(JsonStringFragment::DecodedByte(b'\\')),
                    b'/' => visit(JsonStringFragment::DecodedByte(b'/')),
                    b'b' => visit(JsonStringFragment::DecodedByte(0x08)),
                    b'f' => visit(JsonStringFragment::DecodedByte(0x0c)),
                    b'n' => visit(JsonStringFragment::DecodedByte(b'\n')),
                    b'r' => visit(JsonStringFragment::DecodedByte(b'\r')),
                    b't' => visit(JsonStringFragment::DecodedByte(b'\t')),
                    b'u' => {
                        let codepoint = read_unicode_escape::<E>(bytes, offset)?;
                        visit(JsonStringFragment::Codepoint(codepoint))
                    }
                    other => {
                        return Err(JsonStringScanError::UnknownEscape {
                            offset: *offset - 1,
                            escape: other,
                        });
                    }
                }
                .map_err(JsonStringScanError::Visitor)?;
                segment_start = *offset;
            }
            0x00..=0x1f => return Err(JsonStringScanError::ControlCharacter { offset: *offset }),
            _ => *offset += 1,
        }
    }
}

pub(crate) fn push_utf8(buf: &mut Vec<u8>, codepoint: u32) {
    if codepoint < 0x80 {
        buf.push(codepoint as u8);
    } else if codepoint < 0x800 {
        buf.push(0xc0 | ((codepoint >> 6) as u8));
        buf.push(0x80 | ((codepoint & 0x3f) as u8));
    } else if codepoint < 0x10000 {
        buf.push(0xe0 | ((codepoint >> 12) as u8));
        buf.push(0x80 | (((codepoint >> 6) & 0x3f) as u8));
        buf.push(0x80 | ((codepoint & 0x3f) as u8));
    } else {
        buf.push(0xf0 | ((codepoint >> 18) as u8));
        buf.push(0x80 | (((codepoint >> 12) & 0x3f) as u8));
        buf.push(0x80 | (((codepoint >> 6) & 0x3f) as u8));
        buf.push(0x80 | ((codepoint & 0x3f) as u8));
    }
}

fn read_unicode_escape<E>(
    bytes: &[u8],
    offset: &mut usize,
) -> std::result::Result<u32, JsonStringScanError<E>> {
    let codepoint = read_hex_quad::<E>(bytes, offset)?;
    if (0xd800..=0xdbff).contains(&codepoint) {
        if bytes.get(*offset).copied() != Some(b'\\')
            || bytes.get(*offset + 1).copied() != Some(b'u')
        {
            return Err(JsonStringScanError::InvalidUnicodeSurrogatePair { offset: *offset });
        }

        *offset += 2;
        let low = read_hex_quad::<E>(bytes, offset)?;
        if !(0xdc00..=0xdfff).contains(&low) {
            return Err(JsonStringScanError::InvalidUnicodeSurrogatePair { offset: *offset });
        }

        Ok(0x10000 + ((codepoint - 0xd800) << 10) + (low - 0xdc00))
    } else if (0xdc00..=0xdfff).contains(&codepoint) {
        Err(JsonStringScanError::InvalidUnicodeSurrogatePair { offset: *offset })
    } else {
        Ok(codepoint)
    }
}

fn read_hex_quad<E>(
    bytes: &[u8],
    offset: &mut usize,
) -> std::result::Result<u32, JsonStringScanError<E>> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(JsonStringScanError::InvalidUnicodeEscape { offset: *offset });
    }

    let mut value = 0u32;
    for _ in 0..4 {
        let byte = bytes[*offset];
        *offset += 1;
        let nibble = match byte {
            b'0'..=b'9' => (byte - b'0') as u32,
            b'a'..=b'f' => (byte - b'a' + 10) as u32,
            b'A'..=b'F' => (byte - b'A' + 10) as u32,
            _ => {
                return Err(JsonStringScanError::InvalidUnicodeEscape {
                    offset: *offset - 1,
                });
            }
        };
        value = (value << 4) | nibble;
    }

    Ok(value)
}
