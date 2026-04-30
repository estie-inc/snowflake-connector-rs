use super::common;

use chrono::{NaiveDate, NaiveDateTime};
use snowflake_connector_rs::{ColumnType, DecimalValue, Result};

#[tokio::test]
async fn test_basic_operations() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = "CREATE TEMPORARY TABLE example (
        id NUMBER,
        value STRING,
        price DECIMAL(10,2),
        is_active BOOLEAN,
        created_date DATE,
        updated_at TIMESTAMP_NTZ
    )";
    let table = session.query(query).await?.collect_table().await?;
    assert_eq!(table.row_count(), 1);

    let value = table.rows::<(String,)>()?.next().unwrap()?.0;
    assert_eq!(value, "Table EXAMPLE successfully created.");

    let query = "
    INSERT INTO
        example (
            id,
            value,
            price,
            is_active,
            created_date,
            updated_at
        )
    VALUES
        (
            1,
            'hello',
            99.99,
            true,
            '2023-01-01',
            '2023-01-01 12:00:00'
        ),
        (
            2,
            'world',
            149.99,
            false,
            '2023-01-02',
            '2023-01-02 15:30:00'
        )";
    let table = session.query(query).await?.collect_table().await?;
    assert_eq!(table.row_count(), 1);
    let value = table.rows::<(i64,)>()?.next().unwrap()?.0;
    assert_eq!(value, 2);

    let table = session
        .query("SELECT * FROM example ORDER BY id")
        .await?
        .collect_table()
        .await?;
    assert_eq!(table.row_count(), 2);

    let rows = table
        .rows::<(i64, String, DecimalValue, bool, NaiveDate, NaiveDateTime)>()?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        rows,
        vec![
            (
                1,
                "hello".to_string(),
                DecimalValue::new("99.99", Some(10), Some(2)),
                true,
                NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                NaiveDateTime::parse_from_str("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            (
                2,
                "world".to_string(),
                DecimalValue::new("149.99", Some(10), Some(2)),
                false,
                NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
                NaiveDateTime::parse_from_str("2023-01-02 15:30:00", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        ]
    );

    let columns = table.schema().columns();
    assert_eq!(columns.len(), 6);

    let id_column = &columns[0];
    assert_eq!(id_column.name(), "ID");
    assert_eq!(id_column.index().as_usize(), 0);
    assert!(matches!(id_column.ty(), ColumnType::Fixed { .. }));
    assert!(id_column.nullable());

    let value_column = &columns[1];
    assert_eq!(value_column.name(), "VALUE");
    assert_eq!(value_column.index().as_usize(), 1);
    assert!(matches!(value_column.ty(), ColumnType::Text { .. }));
    assert!(value_column.nullable());

    let price_column = &columns[2];
    assert_eq!(price_column.name(), "PRICE");
    assert!(matches!(price_column.ty(), ColumnType::Fixed { .. }));
    assert!(price_column.nullable());
    assert_eq!(price_column.ty().precision(), Some(10));
    assert_eq!(price_column.ty().scale(), Some(2));

    let is_active_column = &columns[3];
    assert_eq!(is_active_column.name(), "IS_ACTIVE");
    assert!(matches!(is_active_column.ty(), ColumnType::Boolean));
    assert!(is_active_column.nullable());

    let date_column = &columns[4];
    assert_eq!(date_column.name(), "CREATED_DATE");
    assert!(matches!(date_column.ty(), ColumnType::Date));

    let timestamp_column = &columns[5];
    assert_eq!(timestamp_column.name(), "UPDATED_AT");
    assert!(matches!(
        timestamp_column.ty(),
        ColumnType::TimestampNtz { .. }
    ));

    let price_rows = session
        .query_as::<(i64, DecimalValue), _>("SELECT id, price FROM example ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(
        price_rows,
        vec![
            (1, DecimalValue::new("99.99", Some(10), Some(2))),
            (2, DecimalValue::new("149.99", Some(10), Some(2))),
        ]
    );

    Ok(())
}
