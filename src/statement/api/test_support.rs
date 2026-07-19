use std::sync::Arc;

use http::StatusCode;
use reqwest::Url;
use tokio::net::TcpListener;

use crate::{
    ApiContext,
    session::SessionAuth,
    statement::api::QueryApiClient,
    test_support::http::{read_http_request, write_json_response},
};

pub(super) fn test_query_api(base_url: Url) -> QueryApiClient {
    QueryApiClient::new(
        Arc::new(ApiContext::new(reqwest::Client::new(), base_url)),
        SessionAuth::for_test("test-token"),
    )
}

pub(super) async fn respond_once(listener: &TcpListener, status: u16, body: &str) {
    let (mut socket, _) = listener.accept().await.unwrap();
    read_http_request(&mut socket).await.unwrap();
    write_json_response(&mut socket, StatusCode::from_u16(status).unwrap(), body)
        .await
        .unwrap();
}
