use snowflake_connector_rs::{Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

#[tokio::test]
async fn test_download_chunked_results() -> Result<()> {
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
        },
    )?;

    // Act
    let session = client.create_session().await?;
    let query =
        "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>10000))";
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());

    Ok(())
}
