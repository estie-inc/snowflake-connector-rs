use std::{io, net::SocketAddr};

use http::StatusCode;
use reqwest::Url;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub(crate) async fn read_http_request(stream: &mut TcpStream) -> io::Result<String> {
    let mut buf = Vec::new();
    let mut header_end = None;

    while header_end.is_none() {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream closed before headers completed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
    }

    let header_end = header_end.expect("header end must exist") + 4;
    let headers = String::from_utf8_lossy(&buf[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);

    while buf.len() < header_end + content_length {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream closed before body completed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    Ok(String::from_utf8_lossy(&buf).into_owned())
}

pub(crate) async fn write_json_response(
    stream: &mut TcpStream,
    status: StatusCode,
    body: &str,
) -> io::Result<()> {
    let reason = status.canonical_reason().unwrap_or("");
    stream
        .write_all(
            format!(
                "HTTP/1.1 {} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                status.as_u16(),
                body.len(),
            )
            .as_bytes(),
        )
        .await
}

pub(crate) fn base_url(addr: SocketAddr) -> Url {
    Url::parse(&format!("http://{addr}/")).expect("test base URL must parse")
}
