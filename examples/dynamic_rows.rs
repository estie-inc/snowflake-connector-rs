//! The dynamic typing path: decode query results into [`DynamicRow`] / [`CellValue`]
//! when the row shape is not known at compile time.

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use url::Url;

use snowflake_connector_rs::{
    AuthConfig, CellValue, Client, ClientConfig, DynamicRow, EndpointConfig, Result, Session,
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    run_schema_and_variants().await?;
    run_access_by_name_and_index().await?;
    run_take_values_out().await?;
    run_json_conversion().await?;
    Ok(())
}

const SAMPLE_QUERY: &str =
    "SELECT id, name, price, is_active, created_date, payload, note FROM products";

async fn run_schema_and_variants() -> Result<()> {
    println!("== scenario: schema inspection and CellValue variants ==");
    let session = connect(PRODUCT_RESULT).await;

    let table = session.query(SAMPLE_QUERY).await?.collect_table().await?;

    // The schema describes each result column: raw label, zero-based index, type, and nullability.
    for column in table.schema().columns() {
        println!(
            "column {}: {} {} nullable={}",
            column.index(),
            column.name(),
            column.ty(),
            column.is_nullable(),
        );
    }

    // `dynamic_rows()` decodes each row into the `CellValue` vocabulary.
    for row in table.dynamic_rows()? {
        let row = row?;
        for (column, value) in row.schema().columns().iter().zip(row.values()) {
            let rendered = match value {
                CellValue::Null => "NULL".to_string(),
                CellValue::Boolean(b) => format!("boolean {b}"),
                CellValue::Integer(i) => format!("integer {i}"),
                CellValue::Float(f) => format!("float {f}"),
                CellValue::Decimal(d) => format!("decimal {}", d.raw()),
                CellValue::String(s) => format!("string {s:?}"),
                CellValue::Date(d) => format!("date {d}"),
                CellValue::Json(json) => format!("json {json}"),
                other => format!("{other:?}"),
            };
            println!("  {} = {rendered}", column.name());
        }
    }

    Ok(())
}

async fn run_access_by_name_and_index() -> Result<()> {
    println!("\n== scenario: access by name and by index ==");
    let session = connect(PRODUCT_RESULT).await;

    let table = session.query(SAMPLE_QUERY).await?.collect_table().await?;

    // Resolve a label once, then reuse the index across rows; lookups are case-sensitive raw labels.
    let price_index = table.schema().column_index("PRICE")?;

    for row in table.dynamic_rows()? {
        let row = row?;

        // Positional access takes a plain zero-based index.
        assert_eq!(row.value_at(0)?, &CellValue::Integer(1));
        assert!(matches!(row.value_at(price_index)?, CellValue::Decimal(_)));

        // Name-based access resolves the label per call.
        assert_eq!(row.value("NAME")?, &CellValue::String("alice".to_string()));
        assert!(row.value("NOTE")?.is_null());

        println!(
            "id={:?} price={:?}",
            row.value_at(0)?,
            row.value_at(price_index)?
        );
    }

    // Lookup failures are structured `SchemaError`s.
    let row = table.dynamic_rows()?.next().expect("result has one row")?;
    let missing = row.value("no_such_column").unwrap_err();
    println!("lookup failure: {missing}");
    let out_of_range = row.value_at(99).unwrap_err();
    println!("lookup failure: {out_of_range}");

    Ok(())
}

async fn run_take_values_out() -> Result<()> {
    println!("\n== scenario: take values out of a row ==");
    let session = connect(PRODUCT_RESULT).await;

    // `collect()` gathers every row as an owned `DynamicRow`.
    let rows = session
        .query(SAMPLE_QUERY)
        .await?
        .collect::<Vec<DynamicRow>>()
        .await?;

    for mut row in rows {
        // `take` moves the value out without cloning and leaves NULL in the slot,
        // which is useful for large VARIANT / OBJECT payloads.
        let payload = row.take("PAYLOAD")?;
        assert!(matches!(payload, CellValue::Json(_)));
        assert!(row.value("PAYLOAD")?.is_null());

        let name_index = row.schema().column_index("NAME")?;
        let name = row.take_at(name_index)?;
        assert_eq!(name, CellValue::String("alice".to_string()));

        println!("payload={payload:?} name={name:?}");
    }

    Ok(())
}

async fn run_json_conversion() -> Result<()> {
    println!("\n== scenario: convert rows to JSON objects ==");
    let session = connect(PRODUCT_RESULT).await;

    let rows = session
        .query(SAMPLE_QUERY)
        .await?
        .collect::<Vec<DynamicRow>>()
        .await?;

    for row in rows {
        // Keyed by raw column label; fails if two columns share the same label.
        let object = row.into_json_object()?;
        assert_eq!(
            serde_json::Value::Object(object.clone()),
            serde_json::json!({
                "ID": 1,
                "NAME": "alice",
                "PRICE": "12.34",
                "IS_ACTIVE": true,
                "CREATED_DATE": "2026-07-08",
                "PAYLOAD": {"tags": ["a", "b"]},
                "NOTE": null,
            })
        );
        println!(
            "{}",
            serde_json::to_string(&object).expect("row JSON is serializable")
        );
    }

    Ok(())
}

async fn connect(query_response: &'static str) -> Session {
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
    client
        .create_session()
        .await
        .expect("mock accepts the login request")
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

const LOGIN_OK: &str = r#"{"success":true,"data":{"token":"session-token"}}"#;

// One product row mixing the common result types. On the wire, BOOLEAN is "1"/"0",
// DATE is days since the Unix epoch ("20642" = 2026-07-08), and VARIANT is JSON text.
const PRODUCT_RESULT: &str = r#"{"success":true,"data":{"queryId":"01ab-dynamic","rowset":[["1","alice","12.34","1","20642","{\"tags\": [\"a\", \"b\"]}",null]],"rowtype":[{"name":"ID","nullable":false,"precision":38,"scale":0,"type":"fixed"},{"name":"NAME","nullable":false,"length":16,"type":"text"},{"name":"PRICE","nullable":false,"precision":10,"scale":2,"type":"fixed"},{"name":"IS_ACTIVE","nullable":false,"type":"boolean"},{"name":"CREATED_DATE","nullable":false,"type":"date"},{"name":"PAYLOAD","nullable":true,"type":"variant"},{"name":"NOTE","nullable":true,"length":16,"type":"text"}],"queryResultFormat":"json"}}"#;
