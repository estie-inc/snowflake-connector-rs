mod common;

use snowflake_connector_rs::Result;

#[tokio::test]
async fn test_decode() -> Result<()> {
    // Connect to Snowflake
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Create a temporary table
    // DATETIME aliases TIMESTAMP_NTZ
    let query = "CREATE TEMPORARY TABLE example (n NUMBER, s STRING, b BOOLEAN, d DATE, tm TIME, ltz TIMESTAMP_LTZ, ntz TIMESTAMP_NTZ, tz TIMESTAMP_TZ, u UUID)";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<String>("STATUS")?,
        "Table EXAMPLE successfully created."
    );

    // Insert some data
    let query = "INSERT INTO example (n, s, b, d, tm, ltz, ntz, tz, u) VALUES (42,'hello',0,'2024-01-01','01:23:45','2024-01-01 00:00:00','2024-01-01 00:00:00','2024-01-01 00:00:00','2af9c142-ca9d-4d7b-bf2c-ae4458bae85f')";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("NUMBER OF ROWS INSERTED")?, 1);

    // Select the data back
    let query = "SELECT * FROM example";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("n")?, 42);
    assert_eq!(rows[0].get::<String>("s")?, "hello");
    assert!(!(rows[0].get::<bool>("b")?));
    assert_eq!(
        rows[0].get::<chrono::NaiveDate>("d")?,
        chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    );
    assert_eq!(
        rows[0].get::<chrono::NaiveTime>("tm")?,
        chrono::NaiveTime::from_hms_opt(1, 23, 45).unwrap()
    );
    assert_eq!(
        rows[0].get::<chrono::NaiveDateTime>("ltz")?,
        chrono::NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        rows[0].get::<chrono::NaiveDateTime>("ntz")?,
        chrono::NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        rows[0].get::<chrono::NaiveDateTime>("tz")?,
        chrono::NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        rows[0].get::<uuid::Uuid>("u")?,
        uuid::Uuid::parse_str("2af9c142-ca9d-4d7b-bf2c-ae4458bae85f").unwrap()
    );

    Ok(())
}
