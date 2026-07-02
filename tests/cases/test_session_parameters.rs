use std::collections::HashMap;

use super::common;

use snowflake_connector_rs::{CellValue, Result};

#[tokio::test]
async fn test_session_parameters_support_bulk_and_incremental_configuration() -> Result<()> {
    let params = HashMap::from([(
        "CLIENT_RESULT_CHUNK_SIZE".to_string(),
        serde_json::json!(48),
    )]);
    let client = common::connect_with_session(
        common::session_config()
            .with_session_parameters(params)
            .with_session_parameter("TIMEZONE", serde_json::json!("Asia/Tokyo")),
    )?;
    let session = client.create_session().await?;

    let rows = session
        .query("SHOW PARAMETERS LIKE 'CLIENT_RESULT_CHUNK_SIZE' IN SESSION")
        .await?
        .collect::<Vec<_>>()
        .await?;
    let value = rows[0].value("value").unwrap();
    assert_eq!(value, &CellValue::String("48".to_owned()));

    let rows = session
        .query("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION")
        .await?
        .collect::<Vec<_>>()
        .await?;
    let value = rows[0].value("value").unwrap();
    assert_eq!(value, &CellValue::String("Asia/Tokyo".to_owned()));

    Ok(())
}
