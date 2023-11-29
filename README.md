# Snowflake Connector for Rust

# Usage
## Connecting to Snowflake

```rust
use snowflake_connector::{SnowflakeClient, SnowflakeClientConfig};

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

    let client = SnowflakeClient::new(username, SnowflakeAuthMethod::Password(password), config)?;
    let session = client.create_session().await?;
    let rows = session.query("SELECT * FROM your_table").await?;
    eprintln!("{}", rows.len());

    Ok(())
}
```
