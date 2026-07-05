use std::time::Duration;

use super::common;

use snowflake_connector_rs::{QueryConfig, Result};

#[tokio::test]
async fn test_async_query_picked_up_in_final_polling_window() -> Result<()> {
    let query_config =
        QueryConfig::default().with_async_query_completion_timeout(Duration::from_secs(15));
    let session = common::fresh_session_with_query(query_config).await?;

    let table = session
        .query(r#"CALL SYSTEM$WAIT(50)"#)
        .await?
        .collect_table()
        .await?;
    assert_eq!(table.row_count(), 1);

    let value = table.rows::<(String,)>()?.next().unwrap()?.0;
    assert_eq!(value, "waited 50 seconds");

    Ok(())
}
