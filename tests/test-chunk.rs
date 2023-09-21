mod fixtures;

use fixtures::*;
use rstest::rstest;
use snowflake_connector::{Result, SnowflakeClient};

#[rstest]
#[tokio::test]
async fn test_download_chunked_results(client: SnowflakeClient) -> Result<()> {
    // Arrange
    let session = client.create_session().await?;

    // Act
    let query =
        "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>10000))";
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());

    Ok(())
}
