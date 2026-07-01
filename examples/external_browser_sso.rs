use std::env;

use snowflake_connector_rs::{
    ExternalBrowserConfig, SnowflakeAuthConfig, SnowflakeClient, SnowflakeClientConfig,
    SnowflakeSessionConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let username = env::var("SNOWFLAKE_USERNAME")?;
    let account = env::var("SNOWFLAKE_ACCOUNT")?;
    let role = env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = env::var("SNOWFLAKE_DATABASE").ok();
    let schema = env::var("SNOWFLAKE_SCHEMA").ok();

    let mut session_config = SnowflakeSessionConfig::default();
    if let Some(warehouse) = warehouse {
        session_config = session_config.with_warehouse(warehouse);
    }
    if let Some(database) = database {
        session_config = session_config.with_database(database);
    }
    if let Some(schema) = schema {
        session_config = session_config.with_schema(schema);
    }
    if let Some(role) = role {
        session_config = session_config.with_role(role);
    }

    let client = SnowflakeClient::new(
        SnowflakeClientConfig::new(
            &username,
            &account,
            SnowflakeAuthConfig::external_browser(ExternalBrowserConfig::default()),
        )
        .with_session(session_config),
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
    let table = session
        .query_as::<(String,), _>(query)
        .await?
        .collect_table()
        .await?;
    assert_eq!(table.row_count(), 1);
    let (status,) = table.rows().next().expect("status row")?;
    assert_eq!(status, "Table EXAMPLE successfully created.");

    let query = "
    INSERT INTO
        example (
            id,
            value,
            price,
            is_active,
            created_date,
            updated_at
        )
    VALUES
        (
            1,
            'hello',
            99.99,
            true,
            '2023-01-01',
            '2023-01-01 12:00:00'
        ),
        (
            2,
            'world',
            149.99,
            false,
            '2023-01-02',
            '2023-01-02 15:30:00'
        )
    ";
    println!("Executing query:\n{query}");

    let rows = session
        .query_as(query)
        .await?
        .collect::<Vec<(i64,)>>()
        .await?;
    let inserted = rows[0].0;
    assert_eq!(inserted, 2);
    println!("Inserted {inserted} rows.");

    let query = "SELECT * FROM example ORDER BY id";
    println!("Executing query:\n{query}");

    let rows = session
        .query_as("SELECT id, value FROM example ORDER BY id")
        .await?
        .collect::<Vec<(i64, String)>>()
        .await?;
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())],
    );
    for (id, value) in &rows {
        println!("id={id}, value={value}");
    }

    Ok(())
}
