use snowflake_connector_rs::{Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

#[tokio::test]
async fn test_async_query() -> Result<()> {
    // Arrange
    let username = std::env::var("SNOWFLAKE_USERNAME").expect("set SNOWFLAKE_USERNAME for testing");
    let password = std::env::var("SNOWFLAKE_PASSWORD").expect("set SNOWFLAKE_PASSWORD for testing");
    let account = std::env::var("SNOWFLAKE_ACCOUNT").expect("set SNOWFLAKE_ACCOUNT for testing");

    let role = std::env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = std::env::var("SNOWFLAKE_DATABASE").ok();
    let schema = std::env::var("SNOWFLAKE_SCHEMA").ok();

    let client = SnowflakeClient::new(
        &username,
        SnowflakeAuthMethod::Password(password),
        SnowflakeClientConfig {
            account,
            warehouse,
            database,
            schema,
            role,
            timeout: None,
        },
    )?;

    // Act
    let session = client.create_session().await?;
    let query = r#"CALL SYSTEM$WAIT(120)"#;
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>("SYSTEM$WAIT")?, "waited 120 seconds");

    Ok(())
}
