use std::time::Duration;

use snowflake_connector_rs::{Error, QueryCancelStatus, Result};

use super::common;

// The 1s sleep makes submission overwhelmingly likely, so `Accepted` is the expected outcome, but it is not guaranteed: if a
// network delay keeps the query POST from reaching Snowflake before the cancel, `NotSubmitted` is a legitimate result and this
// assertion can flake.

#[tokio::test]
async fn active_query_cancel_uses_request_id_abort() -> Result<()> {
    let session = common::default_session().await?;
    let query = session.query_handle("CALL SYSTEM$WAIT(120)")?;
    let canceller = query.canceller();
    let task = tokio::spawn(query.execute());

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(canceller.cancel().await?, QueryCancelStatus::Accepted);

    let result = task
        .await
        .map_err(|error| Error::other(error.to_string()))?;
    let error = result.expect_err("the long-running query should be cancelled");
    assert!(error.is_cancelled(), "unexpected query error: {error}");
    Ok(())
}

#[tokio::test]
async fn active_query_cancel_during_polling_uses_request_id_abort() -> Result<()> {
    let session = common::default_session().await?;
    let query = session.query_handle("CALL SYSTEM$WAIT(120)")?;
    let canceller = query.canceller();
    let task = tokio::spawn(query.execute());

    // The v1 submit request normally switches to the async response after about 45 seconds. By waiting beyond that
    // point, cancellation targets a query whose ID has already been recorded and whose result is being polled.
    tokio::time::sleep(Duration::from_secs(50)).await;
    assert_eq!(canceller.cancel().await?, QueryCancelStatus::Accepted);

    let result = task
        .await
        .map_err(|error| Error::other(error.to_string()))?;
    let error = result.expect_err("the long-running query should be cancelled");
    assert!(error.is_cancelled(), "unexpected query error: {error}");
    assert!(error.query_id().is_some());
    Ok(())
}

#[tokio::test]
async fn query_cancel_remains_available_after_execution_task_abort() -> Result<()> {
    let session = common::default_session().await?;
    let query = session.query_handle("CALL SYSTEM$WAIT(120)")?;
    let canceller = query.canceller();
    let task = tokio::spawn(query.execute());

    tokio::time::sleep(Duration::from_secs(1)).await;
    task.abort();
    let _ = task.await;

    assert_eq!(canceller.cancel().await?, QueryCancelStatus::Accepted);
    Ok(())
}
