mod common;

use snowflake_connector_rs::Result;

#[tokio::test]
async fn test_decode_naive_date() -> Result<()> {
    // Arrange
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Act
    let query = "SELECT '2020-01-01'::DATE AS date";
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<chrono::NaiveDate>("DATE")?,
        chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
    );

    Ok(())
}

#[tokio::test]
async fn test_basic_operations() -> Result<()> {
    // Connect to Snowflake
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Create a temporary table with various data types
    let query = "CREATE TEMPORARY TABLE example (
        id NUMBER, 
        value STRING,
        price DECIMAL(10,2),
        is_active BOOLEAN,
        created_date DATE,
        updated_at TIMESTAMP_NTZ
    )";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<String>("STATUS")?,
        "Table EXAMPLE successfully created."
    );

    // Insert some data
    let query = "INSERT INTO example (id, value, price, is_active, created_date, updated_at) 
                 VALUES (1, 'hello', 99.99, true, '2023-01-01', '2023-01-01 12:00:00'),
                        (2, 'world', 149.99, false, '2023-01-02', '2023-01-02 15:30:00')";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("NUMBER OF ROWS INSERTED")?, 2);

    // Select the data back
    let query = "SELECT * FROM example ORDER BY id";
    let rows = session.query(query).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64>("ID")?, 1);
    assert_eq!(rows[0].get::<String>("VALUE")?, "hello");
    assert_eq!(rows[1].get::<i64>("ID")?, 2);
    assert_eq!(rows[1].get::<String>("VALUE")?, "world");

    // Test column_types method with various data types
    let column_types = rows[0].column_types();
    assert_eq!(column_types.len(), 6);

    // Check ID column (NUMBER/FIXED)
    let id_column = &column_types[0];
    assert_eq!(id_column.name(), "ID");
    assert_eq!(id_column.index(), 0);
    assert_eq!(id_column.column_type().snowflake_type(), "fixed");
    assert!(id_column.column_type().nullable());

    // Check VALUE column (STRING/TEXT)
    let value_column = &column_types[1];
    assert_eq!(value_column.name(), "VALUE");
    assert_eq!(value_column.index(), 1);
    assert_eq!(value_column.column_type().snowflake_type(), "text");
    assert!(value_column.column_type().nullable());

    // Check PRICE column (DECIMAL)
    let price_column = &column_types[2];
    assert_eq!(price_column.name(), "PRICE");
    assert_eq!(price_column.index(), 2);
    assert_eq!(price_column.column_type().snowflake_type(), "fixed");
    assert!(price_column.column_type().nullable());
    assert_eq!(price_column.column_type().precision(), Some(10));
    assert_eq!(price_column.column_type().scale(), Some(2));

    // Check IS_ACTIVE column (BOOLEAN)
    let is_active_column = &column_types[3];
    assert_eq!(is_active_column.name(), "IS_ACTIVE");
    assert_eq!(is_active_column.index(), 3);
    assert_eq!(is_active_column.column_type().snowflake_type(), "boolean");
    assert!(is_active_column.column_type().nullable());

    // Check CREATED_DATE column (DATE)
    let date_column = &column_types[4];
    assert_eq!(date_column.name(), "CREATED_DATE");
    assert_eq!(date_column.index(), 4);
    assert_eq!(date_column.column_type().snowflake_type(), "date");
    assert!(date_column.column_type().nullable());

    // Check UPDATED_AT column (TIMESTAMP_NTZ)
    let timestamp_column = &column_types[5];
    assert_eq!(timestamp_column.name(), "UPDATED_AT");
    assert_eq!(timestamp_column.index(), 5);
    assert_eq!(
        timestamp_column.column_type().snowflake_type(),
        "timestamp_ntz"
    );
    assert!(timestamp_column.column_type().nullable());

    Ok(())
}
