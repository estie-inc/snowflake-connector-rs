use snowflake_connector_rs::{Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

#[tokio::test]
async fn test_decode_naive_date() -> Result<()> {
    // Arrange
    let client = connect()?;
    let session = client.create_session().await?;

    // Act
    let query = "SELECT '2020-01-01'::DATE AS date";
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<chrono::NaiveDate>("DATE")?,
        chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
    );

    Ok(())
}

#[tokio::test]
async fn test_basic_operations() -> Result<()> {
    // Connect to Snowflake
    let client = connect()?;
    let session = client.create_session().await?;

    // Create a temporary table
    let query = "CREATE TEMPORARY TABLE example (id NUMBER, value STRING)";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<String>("STATUS")?,
        "Table EXAMPLE successfully created."
    );

    // Insert some data
    let query = "INSERT INTO example (id, value) VALUES (1, 'hello'), (2, 'world')";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("NUMBER OF ROWS INSERTED")?, 2);

    // Select the data back
    let query = "SELECT * FROM example ORDER BY id";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64>("ID")?, 1);
    assert_eq!(rows[0].get::<String>("VALUE")?, "hello");
    assert_eq!(rows[1].get::<i64>("ID")?, 2);
    assert_eq!(rows[1].get::<String>("VALUE")?, "world");

    Ok(())
}

fn connect() -> Result<SnowflakeClient> {
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

    Ok(client)
}
