use super::common;

use snowflake_connector_rs::Result;

#[tokio::test]
async fn test_session_parameter_chunk_size() -> Result<()> {
    let client = common::connect_with_session(
        common::session_config()
            .with_session_parameter("CLIENT_RESULT_CHUNK_SIZE", serde_json::json!(48)),
    )?;
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
    let client = common::connect_with_session(
        common::session_config()
            .with_session_parameter("CLIENT_RESULT_CHUNK_SIZE", serde_json::json!(48))
            .with_session_parameter("TIMEZONE", serde_json::json!("Asia/Tokyo")),
    )?;
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
