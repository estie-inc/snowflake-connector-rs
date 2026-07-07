//! The recommended way to handle [`snowflake_connector_rs::Error`]: branch on `kind()`, then read the structured fields
//! through the accessors.
//!
//! Each scenario produces a real `Error` through the public client API, and `describe` shows the extraction pattern.

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

/// Run a query against a server that fails it with `query_response`, and return the resulting error.
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

fn spawn_mock_snowflake(responses: Vec<&'static str>) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
    let addr = listener.local_addr().expect("mock server address");

    thread::spawn(move || {
        for body in responses {
            let (mut stream, _) = listener.accept().expect("accept connection");
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
