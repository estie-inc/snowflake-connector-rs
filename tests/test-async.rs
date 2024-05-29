mod common;

use snowflake_connector_rs::Result;

#[tokio::test]
async fn test_async_query() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    let query = r#"CALL SYSTEM$WAIT(120)"#;
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>("SYSTEM$WAIT")?, "waited 120 seconds");

    Ok(())
}
