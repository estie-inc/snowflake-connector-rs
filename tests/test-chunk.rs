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
    let (row_types, rows) = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 10000);
    assert_eq!(row_types.len(), 2);
    assert_eq!(row_types[0].name, "SEQ");
    assert_eq!(row_types[0].data_type, "fixed");
    assert_eq!(row_types[0].nullable, false);
    assert_eq!(row_types[1].name, "RAND");
    assert_eq!(row_types[1].data_type, "text");
    assert_eq!(row_types[1].nullable, false);

    Ok(())
}
