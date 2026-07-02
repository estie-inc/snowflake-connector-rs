use super::common;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use snowflake_connector_rs::{CellValue, Result, SchemaError};

#[tokio::test]
async fn test_decode() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session.query("ALTER SESSION SET TIMEZONE = 'UTC'").await?;

    let query = "
    CREATE TEMPORARY TABLE example (
        n NUMBER,
        s STRING,
        b BOOLEAN,
        d DATE,
        tm TIME,
        tm3 TIME(3),
        tm9 TIME(9),
        ltz TIMESTAMP_LTZ,
        ntz TIMESTAMP_NTZ,
        tz TIMESTAMP_TZ
    )";
    let table = session.query(query).await?.collect_table().await?;
    assert_eq!(table.row_count(), 1);

    let row = table.dynamic_rows()?.next().unwrap()?;
    let status = row.value("status").unwrap();
    assert_eq!(
        status,
        &CellValue::String("Table EXAMPLE successfully created.".to_owned()),
    );

    let query = "
    INSERT INTO example (n, s, b, d, tm, tm3, tm9, ltz, ntz, tz) VALUES (
        42,
        'hello',
        0,
        '2024-01-01',
        '01:23:45',
        '01:23:45.123',
        '01:23:45.123456789',
        '2024-01-01 00:00:00',
        '2024-01-01 00:00:00',
        '2024-01-01 00:00:00'
    )";
    let table = session.query(query).await?.collect_table().await?;
    assert_eq!(table.row_count(), 1);

    let row = table.dynamic_rows()?.next().unwrap()?;
    let number_of_rows_inserted = row.value("number of rows inserted").unwrap();
    assert_eq!(number_of_rows_inserted, &CellValue::Integer(1));

    let table = session
        .query_as::<(
            i64,
            String,
            bool,
            NaiveDate,
            NaiveTime,
            NaiveTime,
            NaiveTime,
            DateTime<Utc>,
            NaiveDateTime,
            DateTime<Utc>,
        ), _>("SELECT * FROM example")
        .await?
        .collect_table()
        .await?;
    assert_eq!(table.row_count(), 1);

    let row = table.rows().next().unwrap()?;
    assert_eq!(row.0, 42);
    assert_eq!(row.1, "hello");
    assert!(!row.2);
    assert_eq!(row.3, NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    assert_eq!(row.4, NaiveTime::from_hms_opt(1, 23, 45).unwrap());
    assert_eq!(
        row.5,
        NaiveTime::from_hms_milli_opt(1, 23, 45, 123).unwrap()
    );
    assert_eq!(
        row.6,
        NaiveTime::from_hms_nano_opt(1, 23, 45, 123_456_789).unwrap()
    );
    assert_eq!(
        row.7,
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    );
    assert_eq!(
        row.8,
        NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        row.9,
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    );

    Ok(())
}

#[tokio::test]
async fn test_dynamic_row_value_resolves_escaped_identifiers() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let table = session
        .query(
            r#"SELECT
                1 AS id,
                2 AS "id",
                3 AS "my column",
                4 AS "MixedCase""#,
        )
        .await?
        .collect_table()
        .await?;

    let row = table.dynamic_rows()?.next().unwrap()?;

    assert_eq!(row.value("ID").unwrap(), &CellValue::Integer(1));
    assert_eq!(row.value("id").unwrap(), &CellValue::Integer(2));
    assert_eq!(row.value("my column").unwrap(), &CellValue::Integer(3));
    assert_eq!(row.value("MixedCase").unwrap(), &CellValue::Integer(4));

    assert!(matches!(
        row.value("MIXEDCASE"),
        Err(SchemaError::MissingColumn(_))
    ));
    assert!(matches!(
        row.value("MY COLUMN"),
        Err(SchemaError::MissingColumn(_))
    ));

    Ok(())
}

/// Verify TIMESTAMP_TZ decoding into `DateTime<FixedOffset>` and
/// `DateTime<Utc>` for both eastern (UTC+09:00) and western (UTC-05:00)
/// offsets.
#[tokio::test]
async fn test_decode_timestamp_tz_offsets() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Use a session timezone that is not UTC so any accidental
    // session-default leakage shows up in the failure mode.
    session
        .query("ALTER SESSION SET TIMEZONE = 'America/New_York'")
        .await?;

    let table = session
        .query(
            "SELECT
                '2024-06-15 12:30:45 +09:00'::TIMESTAMP_TZ AS east,
                '2024-06-15 12:30:45 -05:00'::TIMESTAMP_TZ AS west,
                '2024-06-15 12:30:45 +23:59'::TIMESTAMP_TZ AS far_east,
                '2024-06-15 12:30:45 -23:59'::TIMESTAMP_TZ AS far_west",
        )
        .await?
        .collect_table()
        .await?;

    let (east_utc, west_utc, far_east_utc, far_west_utc) = table
        .rows::<(DateTime<Utc>, DateTime<Utc>, DateTime<Utc>, DateTime<Utc>)>()?
        .next()
        .unwrap()?;
    let (east_off, west_off, far_east_off, far_west_off) = table
        .rows::<(
            DateTime<FixedOffset>,
            DateTime<FixedOffset>,
            DateTime<FixedOffset>,
            DateTime<FixedOffset>,
        )>()?
        .next()
        .unwrap()?;

    // East: 2024-06-15 12:30:45 +09:00 == 2024-06-15 03:30:45 UTC
    assert_eq!(
        east_utc,
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(3, 30, 45)
            .unwrap()
            .and_utc()
    );
    assert_eq!(east_off.offset().local_minus_utc(), 9 * 3600);
    assert_eq!(
        east_off.naive_local(),
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
    );

    // West: 2024-06-15 12:30:45 -05:00 == 2024-06-15 17:30:45 UTC
    assert_eq!(
        west_utc,
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(17, 30, 45)
            .unwrap()
            .and_utc()
    );
    assert_eq!(west_off.offset().local_minus_utc(), -5 * 3600);
    assert_eq!(
        west_off.naive_local(),
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
    );

    // Far east: 2024-06-15 12:30:45 +23:59 == 2024-06-14 12:31:45 UTC
    assert_eq!(
        far_east_utc,
        NaiveDate::from_ymd_opt(2024, 6, 14)
            .unwrap()
            .and_hms_opt(12, 31, 45)
            .unwrap()
            .and_utc()
    );
    assert_eq!(far_east_off.offset().local_minus_utc(), 23 * 3600 + 59 * 60);
    assert_eq!(
        far_east_off.naive_local(),
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
    );

    // Far west: 2024-06-15 12:30:45 -23:59 == 2024-06-16 12:29:45 UTC
    assert_eq!(
        far_west_utc,
        NaiveDate::from_ymd_opt(2024, 6, 16)
            .unwrap()
            .and_hms_opt(12, 29, 45)
            .unwrap()
            .and_utc()
    );
    assert_eq!(
        far_west_off.offset().local_minus_utc(),
        -(23 * 3600 + 59 * 60)
    );
    assert_eq!(
        far_west_off.naive_local(),
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
    );

    Ok(())
}
