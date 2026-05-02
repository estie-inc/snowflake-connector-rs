use std::collections::HashMap;

use super::common;

use snowflake_connector_rs::{FromRow, Result, SnowflakeValue};

#[derive(Debug, FromRow, PartialEq)]
struct ShowParameterRow {
    value: String,
}

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
        .collect()
        .await?;
    let value = rows[0].value_by_label("value").unwrap();
    assert_eq!(value, &SnowflakeValue::String("48".to_owned()));

    let rows = session
        .query("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION")
        .await?
        .collect()
        .await?;
    let value = rows[0].value_by_label("value").unwrap();
    assert_eq!(value, &SnowflakeValue::String("Asia/Tokyo".to_owned()));

    Ok(())
}

#[tokio::test]
async fn derive_decodes_show_parameters_lowercase_label() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 48")
        .await?;
    let rows = session
        .query_as::<ShowParameterRow, _>(
            "SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS' IN SESSION",
        )
        .await?
        .collect()
        .await?;

    assert_eq!(
        rows,
        vec![ShowParameterRow {
            value: "48".to_string()
        }]
    );
    Ok(())
}
