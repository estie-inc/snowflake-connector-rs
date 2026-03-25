mod common;

use std::collections::HashMap;

use snowflake_connector_rs::Result;

#[tokio::test]
async fn test_session_parameter_chunk_size() -> Result<()> {
    let mut params = HashMap::new();
    params.insert(
        "CLIENT_RESULT_CHUNK_SIZE".to_string(),
        serde_json::json!(48),
    );

    let client = common::connect_with_session_parameters(params)?;
    let session = client.create_session().await?;

    let rows = session
        .query("SHOW PARAMETERS LIKE 'CLIENT_RESULT_CHUNK_SIZE' IN SESSION")
        .await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>("value")?, "48");

    Ok(())
}

#[tokio::test]
async fn test_multiple_session_parameters() -> Result<()> {
    let mut params = HashMap::new();
    params.insert(
        "CLIENT_RESULT_CHUNK_SIZE".to_string(),
        serde_json::json!(48),
    );
    params.insert("TIMEZONE".to_string(), serde_json::json!("Asia/Tokyo"));

    let client = common::connect_with_session_parameters(params)?;
    let session = client.create_session().await?;

    let chunk_rows = session
        .query("SHOW PARAMETERS LIKE 'CLIENT_RESULT_CHUNK_SIZE' IN SESSION")
        .await?;
    assert_eq!(chunk_rows[0].get::<String>("value")?, "48");

    let tz_rows = session
        .query("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION")
        .await?;
    assert_eq!(tz_rows[0].get::<String>("value")?, "Asia/Tokyo");

    Ok(())
}
