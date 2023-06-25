# Snowflake Connector for Rust

# Usage
## Connecting to Snowflake

```rust
use snowflake_connector::{QueryRequest, SnowflakeClient, SnowflakeClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let username = "...";
    let password = "...";

    let config = SnowflakeClientConfig {
        account: "your-account.ap-northeast-1.aws".into(),
        role: Some("YOUR_ROLE".into()),
        warehouse: Some("YOUR_WAREHOUSE".into()),
        database: Some("AMAZING_DATABASE".into()),
        schema: Some("COOL_SCHEMA".into()),
    };

    let client = SnowflakeClient::new(username, password, config)?;
    let session = client.create_session().await?;
    let response = session
        .query(&QueryRequest {
            sql_text: "SELECT * FROM YOUR_TABLE LIMIT 10".into(),
        })
        .await?;
    eprintln!("{:#?}", response);

    Ok(())
}
```
