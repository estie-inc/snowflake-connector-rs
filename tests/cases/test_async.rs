use std::time::Duration;

use super::common;

use snowflake_connector_rs::{Result, SnowflakeQueryConfig};

#[tokio::test]
async fn test_async_query_picked_up_in_final_polling_window() -> Result<()> {
    let query_config = SnowflakeQueryConfig::default()
        .with_async_query_completion_timeout(Duration::from_secs(25));
    let client = common::connect_with_query(query_config)?;
    let session = client.create_session().await?;

    let table = session
        .query(r#"CALL SYSTEM$WAIT(65)"#)
        .await?
        .collect_table()
        .await?;
    assert_eq!(table.row_count(), 1);

    let value = table.rows::<(String,)>()?.next().unwrap()?.0;
    assert_eq!(value, "waited 65 seconds");

    Ok(())
}
