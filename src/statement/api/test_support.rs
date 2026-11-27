use std::sync::Arc;

use http::StatusCode;
use reqwest::Url;
use tokio::net::TcpListener;

use crate::{
    ApiContext,
    auth::SessionAuth,
    statement::api::QueryApiClient,
    test_support::http::{read_http_request, write_json_response},
};

pub(super) fn test_query_api(base_url: Url) -> QueryApiClient {
    QueryApiClient::new(
        Arc::new(ApiContext::new(reqwest::Client::new(), base_url)),
        SessionAuth::for_test("test-token"),
    )
}

pub(super) fn renewable_query_api(base_url: Url) -> QueryApiClient {
    QueryApiClient::new(
        Arc::new(ApiContext::new(reqwest::Client::new(), base_url)),
        SessionAuth::for_test_with_master("test-token", "master-token"),
    )
}

pub(super) const SESSION_EXPIRED_BODY: &str =
    r#"{"success":false,"code":"390112","message":"session expired","data":null}"#;
pub(super) const RENEWED_BODY: &str =
    r#"{"success":true,"data":{"sessionToken":"renewed-token","masterToken":"renewed-master"}}"#;
pub(super) const QUERY_DONE_BODY: &str = r#"{"success":true,"data":{"queryId":"q","rowtype":[],"rowset":[],"queryResultFormat":"json"}}"#;

/// Answers one connection per scripted `200` body, then reports the requests it received and whether the client
/// stayed quiet afterwards.
pub(super) fn serve_script(
    listener: TcpListener,
    bodies: Vec<&'static str>,
) -> tokio::task::JoinHandle<(Vec<String>, bool)> {
    tokio::spawn(async move {
        let mut requests = Vec::new();
        for body in bodies {
            let (mut socket, _) = listener.accept().await.unwrap();
            requests.push(read_http_request(&mut socket).await.unwrap());
            write_json_response(&mut socket, StatusCode::OK, body)
                .await
                .unwrap();
        }
        let quiet = tokio::time::timeout(std::time::Duration::from_millis(200), listener.accept())
            .await
            .is_err();
        (requests, quiet)
    })
}

pub(super) fn request_target(request: &str) -> &str {
    request.split_whitespace().nth(1).unwrap()
}

pub(super) async fn respond_once(listener: &TcpListener, status: u16, body: &str) {
    let (mut socket, _) = listener.accept().await.unwrap();
    read_http_request(&mut socket).await.unwrap();
    write_json_response(&mut socket, StatusCode::from_u16(status).unwrap(), body)
        .await
        .unwrap();
}
