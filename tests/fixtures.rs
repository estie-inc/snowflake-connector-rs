use rstest::*;
use snowflake_connector::{SnowflakeClient, SnowflakeClientConfig};

#[fixture]
pub fn client() -> SnowflakeClient {
    let username = std::env::var("SNOWFLAKE_USERNAME").unwrap();
    let password = std::env::var("SNOWFLAKE_PASSWORD").unwrap();
    let account = std::env::var("SNOWFLAKE_ACCOUNT").unwrap();
    let role = std::env::var("SNOWFLAKE_ROLE").unwrap();
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").unwrap();
    let database = std::env::var("SNOWFLAKE_DATABASE").unwrap();
    let schema = std::env::var("SNOWFLAKE_SCHEMA").unwrap();

    SnowflakeClient::new(
        &username,
        &password,
        SnowflakeClientConfig {
            account,
            warehouse: Some(warehouse),
            database: Some(database),
            schema: Some(schema),
            role: Some(role),
        },
    )
    .unwrap()
}
