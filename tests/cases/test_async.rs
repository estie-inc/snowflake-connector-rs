use super::common;

use snowflake_connector_rs::Result;

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
