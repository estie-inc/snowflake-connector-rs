use reqwest::Url;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{ClientSharedPartial, session::SessionAuth, statement::api::QueryApiClient};

pub(super) fn test_query_api(base_url: Url) -> QueryApiClient {
    QueryApiClient::new(
        ClientSharedPartial::new().with_base_url(base_url).build(),
        SessionAuth::for_test("test-token"),
    )
}

pub(super) async fn read_http_request(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    let body_start;
    let body_length;

    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "connection closed before request completed");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            body_start = index + 4;
            let headers = String::from_utf8_lossy(&bytes[..index]);
            body_length = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .or_else(|| line.strip_prefix("Content-Length:"))
                })
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break;
        }
    }

    while bytes.len() < body_start + body_length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "connection closed before request body completed");
        bytes.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(bytes).unwrap()
}

pub(super) async fn write_http_response(socket: &mut TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
}

pub(super) async fn respond_once(listener: &TcpListener, status: u16, body: &str) {
    let (mut socket, _) = listener.accept().await.unwrap();
    read_http_request(&mut socket).await;
    write_http_response(&mut socket, status, body).await;
}
