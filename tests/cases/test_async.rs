use std::time::Duration;

use super::common;

use snowflake_connector_rs::{ErrorKind, QueryConfig, Result};

const QUERY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn test_async_query_succeeds_within_query_response_timeout() -> Result<()> {
    // The 50s wait is long enough to be served asynchronously, while the 60s response timeout still leaves room for the
    // poll path to return the completed result.
    let query_config = QueryConfig::default().with_query_response_timeout(QUERY_RESPONSE_TIMEOUT);
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

#[tokio::test]
async fn test_async_query_timeout_during_poll_preserves_query_id() -> Result<()> {
    // The timeout is above Snowflake's ~45s submit block, so the initial async response should include a query id
    // before the client stops waiting during polling.
    let query_config = QueryConfig::default().with_query_response_timeout(QUERY_RESPONSE_TIMEOUT);
    let session = common::fresh_session_with_query(query_config).await?;

    let err = session
        .query(r#"CALL SYSTEM$WAIT(65)"#)
        .await
        .expect_err("a query exceeding the response timeout must fail");
    assert_eq!(err.kind(), ErrorKind::Timeout);
    assert!(err.is_timeout());
    assert!(
        err.query_id().is_some(),
        "a poll-phase timeout must preserve the query id, got {err:?}"
    );

    Ok(())
}
