use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let username = std::env::var("SNOWFLAKE_USERNAME")?;
    let account = std::env::var("SNOWFLAKE_ACCOUNT")?;
    let role = std::env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = std::env::var("SNOWFLAKE_DATABASE").ok();
    let schema = std::env::var("SNOWFLAKE_SCHEMA").ok();

    let client = SnowflakeClient::new(
        &username,
        SnowflakeAuthMethod::ExternalBrowser,
        SnowflakeClientConfig {
            account,
            warehouse,
            database,
            schema,
            role,
            timeout: Some(std::time::Duration::from_secs(90)),
        },
    )?;

    let session = client.create_session().await?;

    let query = "CREATE TEMPORARY TABLE example (
        id NUMBER, 
        value STRING,
        price DECIMAL(10,2),
        is_active BOOLEAN,
        created_date DATE,
        updated_at TIMESTAMP_NTZ
    )";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<String>("STATUS")?,
        "Table EXAMPLE successfully created."
    );

    // Insert some data
    let query = "INSERT INTO example (id, value, price, is_active, created_date, updated_at) 
                 VALUES (1, 'hello', 99.99, true, '2023-01-01', '2023-01-01 12:00:00'),
                        (2, 'world', 149.99, false, '2023-01-02', '2023-01-02 15:30:00')";
    println!("Executing query:\n{}", query);
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    let inserted: i64 = rows[0].get("NUMBER OF ROWS INSERTED")?;
    assert_eq!(inserted, 2);
    println!("Inserted {} rows.", inserted);

    // Select the data back
    let query = "SELECT * FROM example ORDER BY id";
    println!("Executing query:\n{}", query);
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 2);
    let row0_id: i64 = rows[0].get("ID")?;
    let row0_value: String = rows[0].get("VALUE")?;
    assert_eq!(row0_id, 1);
    assert_eq!(row0_value, "hello");
    println!("Row 0: id={}, value={}", row0_id, row0_value);
    let row1_id: i64 = rows[1].get("ID")?;
    let row1_value: String = rows[1].get("VALUE")?;
    assert_eq!(row1_id, 2);
    assert_eq!(row1_value, "world");
    println!("Row 1: id={}, value={}", row1_id, row1_value);

    Ok(())
}
