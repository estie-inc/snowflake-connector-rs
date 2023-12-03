# Snowflake Connector for Rust

A Rust client for Snowflake, which enables you to connect to Snowflake and run queries.

```rust
let client = SnowflakeClient::new(
    "USERNAME",
    SnowflakeAuthMethod::Password("PASSWORD".to_string()),
    SnowflakeClientConfig {
        account: "ACCOUNT".to_string(),
        role: Some("ROLE".to_string()),
        warehouse: Some("WAREHOUSE".to_string()),
        database: Some("DATABASE".to_string()),
        schema: Some("SCHEMA".to_string()),
    },
)?;
let session = client.create_session().await?;
let query = "CREATE TEMPORARY TABLE example (id NUMBER, value STRING)";
session.query(query).await?;
let query = "INSERT INTO example (id, value) VALUES (1, 'hello'), (2, 'world')";
session.query(query).await?;
let query = "SELECT * FROM example ORDER BY id";
let rows = session.query(query).await?;
assert_eq!(rows.len(), 2);
assert_eq!(rows[0].get::<i64>("ID")?, 1);
assert_eq!(rows[0].get::<String>("VALUE")?, "hello");
```
