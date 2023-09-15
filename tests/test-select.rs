mod fixtures;

use fixtures::*;
use rstest::rstest;
use snowflake_connector::{Result, SnowflakeClient};

#[rstest]
#[tokio::test]
async fn test(client: SnowflakeClient) -> Result<()> {
    let session = client.create_session().await?;
    let response = session.query("SELECT COUNT(*) FROM ASKINGS").await?;
    eprintln!("{:#?}", response);
    Ok(())
}
