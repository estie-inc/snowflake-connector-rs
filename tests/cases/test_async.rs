use std::time::Duration;

use super::common;

use snowflake_connector_rs::{Result, SnowflakeQueryConfig};

#[tokio::test]
async fn test_async_query() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    // Snowflake returns a handle for statements that take longer than 45 seconds
    // when `async=true` is not specified:
    // https://docs.snowflake.com/en/developer-guide/sql-api/reference
    let query = r#"CALL SYSTEM$WAIT(60)"#;
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>("SYSTEM$WAIT")?, "waited 60 seconds");

    Ok(())
}

/// Verify that a query completing during the final polling window is
/// picked up rather than dropped by a premature timeout check.
///
/// Timeline (POLL_INTERVAL = 10 s, timeout = 25 s):
///   - SYSTEM$WAIT(65) triggers async handoff at ~45 s
///   - Polling starts, deadline ≈ 45 + 25 = 70 s
///   - Poll 1 at ~45 s (immediate): in-progress
///   - sleep(10) → Poll 2 at ~55 s: in-progress (query finishes at 65 s)
///   - sleep(10) → Poll 3 at ~65 s: in-progress or just completed (race)
///   - remaining ≈ 5 s < POLL_INTERVAL → sleep(5 s) — final sub-interval
///   - Poll 4 at ~70 s: completed
#[tokio::test]
async fn test_async_query_picked_up_in_final_polling_window() -> Result<()> {
    let query_config = SnowflakeQueryConfig::default()
        .with_async_query_completion_timeout(Duration::from_secs(25));
    let client = common::connect_with_query(query_config)?;
    let session = client.create_session().await?;

    let rows = session.query(r#"CALL SYSTEM$WAIT(65)"#).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>("SYSTEM$WAIT")?, "waited 65 seconds");

    Ok(())
}
