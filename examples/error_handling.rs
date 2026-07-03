//! Inspecting [`snowflake_connector_rs::Error`] without a live Snowflake account.
//!
//! A minimal mock server stands in for Snowflake: it accepts the login request, then answers the statement request with a
//! canned failure. Each scenario drives the public client API to produce a real `Error`, and `describe` shows the recommended
//! way to read it — branch on `kind()`, then use the accessors.

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use url::Url;

use snowflake_connector_rs::{AuthConfig, Client, ClientConfig, EndpointConfig, Error, ErrorKind};

const LOGIN_OK: &str = r#"{"success":true,"data":{"token":"session-token"}}"#;
const SERVER_ERROR: &str = r#"{"code":"000904","message":"SQL compilation error: invalid identifier 'FOO'","success":false,"data":{"queryId":"01ab0000-demo-server"}}"#;
const SESSION_EXPIRED: &str = r#"{"code":"390112","message":"Your session has expired. Please login again.","success":false,"data":{"queryId":"01ab0001-demo-expired"}}"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("== scenario: Snowflake rejected the statement ==");
    describe(&provoke_error(SERVER_ERROR).await);

    println!("\n== scenario: session token expired ==");
    describe(&provoke_error(SESSION_EXPIRED).await);

    Ok(())
}

/// The reusable extraction pattern: categorize with `kind()`, then read the structured fields the category exposes.
fn describe(err: &Error) {
    println!("kind      = {:?}", err.kind());

    match err.kind() {
        ErrorKind::SessionExpired => {
            // Recover by creating a fresh session and retrying the statement.
            println!("action    = reconnect and retry");
        }
        ErrorKind::Server => {
            // The code/message/query id correlate the failure with query history.
            println!("action    = surface to the caller; inspect query history");
        }
        ErrorKind::Timeout => println!("action    = retry with backoff"),
        ErrorKind::Decode => {
            // A result column or cell did not match the requested Rust type.
            if let Some(schema) = err.as_schema_error() {
                println!("schema    = {schema}");
            }
        }
        _ => {}
    }

    // Snowflake-provided fields are available regardless of category; each is `None` when the failure path could not supply it.
    if let Some(code) = err.snowflake_code() {
        println!("code      = {code}");
    }
    if let Some(message) = err.snowflake_message() {
        println!("message   = {message}");
    }
    if let Some(query_id) = err.query_id() {
        println!("query_id  = {query_id}");
    }

    // The underlying cause chain is available for logging when one exists.
    if let Some(source) = std::error::Error::source(err) {
        println!("source    = {source}");
    }

    println!("display   = {err}");
}

/// Point a client at the mock server, run a query, and return the resulting error.
async fn provoke_error(query_response: &'static str) -> Error {
    let base_url = spawn_mock_snowflake(vec![LOGIN_OK, query_response]);

    let config = ClientConfig::new(
        "demo-user",
        "demo-account",
        AuthConfig::password("demo-secret"),
    )
    .with_endpoint(EndpointConfig::custom_base_url(
        Url::parse(&base_url).expect("valid mock URL"),
    ));

    let client = Client::new(config).expect("client config is valid");
    let session = client
        .create_session()
        .await
        .expect("mock accepts the login request");

    match session.query("SELECT * FROM demo").await {
        Ok(_) => panic!("mock is configured to return an error"),
        Err(err) => err,
    }
}

/// Serve `responses` in order, one per incoming connection, then stop.
///
/// This is deliberately just enough HTTP to satisfy the connector; a real test harness would use a proper mock-server crate.
fn spawn_mock_snowflake(responses: Vec<&'static str>) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
    let addr = listener.local_addr().expect("mock server address");

    thread::spawn(move || {
        for body in responses {
            let (mut stream, _) = listener.accept().expect("accept connection");
            // Read the request so the client can finish sending before we reply.
            let mut buf = [0_u8; 8192];
            let _ = stream.read(&mut buf);

            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        }
    });

    format!("http://{addr}")
}
