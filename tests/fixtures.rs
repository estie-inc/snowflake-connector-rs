use rstest::*;
use snowflake_connector::{SnowflakeClient, SnowflakeClientConfig};

#[fixture]
pub fn client() -> SnowflakeClient {
    let username = std::env::var("SNOWFLAKE_USERNAME").expect("set SNOWFLAKE_USERNAME for testing");
    let password = std::env::var("SNOWFLAKE_PASSWORD").expect("set SNOWFLAKE_PASSWORD for testing");
    let account = std::env::var("SNOWFLAKE_ACCOUNT").expect("set SNOWFLAKE_ACCOUNT for testing");

    let role = std::env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = std::env::var("SNOWFLAKE_DATABASE").ok();
    let schema = std::env::var("SNOWFLAKE_SCHEMA").ok();

    SnowflakeClient::new(
        &username,
        &password,
        SnowflakeClientConfig {
            account,
            warehouse,
            database,
            schema,
            role,
        },
    )
    .unwrap()
}
