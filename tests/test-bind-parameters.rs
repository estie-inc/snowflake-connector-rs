mod common;

use chrono::{NaiveDate, NaiveDateTime};
use snowflake_connector_rs::{Binding, BindingType, QueryRequest, Result};

#[tokio::test]
async fn test_bind_parameters_insert_and_select() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_test (id NUMBER, value STRING)")
        .await?;

    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_test (id, value) VALUES (?, ?)",
        vec![Binding::fixed(1), Binding::text("hello")],
    );
    session.query(insert).await?;

    let select = QueryRequest::with_bindings(
        "SELECT id, value FROM bind_test WHERE id = ?",
        vec![Binding::fixed(1)],
    );
    let rows = session.query(select).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("ID")?, 1);
    assert_eq!(rows[0].get::<String>("VALUE")?, "hello");

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_multiple_rows() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_multi (id NUMBER, name STRING)")
        .await?;

    for (id, name) in [(1, "alice"), (2, "bob"), (3, "charlie")] {
        let insert = QueryRequest::with_bindings(
            "INSERT INTO bind_multi (id, name) VALUES (?, ?)",
            vec![Binding::fixed(id), Binding::text(name)],
        );
        session.query(insert).await?;
    }

    let rows = session
        .query("SELECT * FROM bind_multi ORDER BY id")
        .await?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String>("NAME")?, "alice");
    assert_eq!(rows[2].get::<String>("NAME")?, "charlie");

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_boolean() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_bool (id NUMBER, flag BOOLEAN)")
        .await?;

    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_bool (id, flag) VALUES (?, ?)",
        vec![Binding::fixed(1), Binding::boolean(true)],
    );
    session.query(insert).await?;

    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_bool (id, flag) VALUES (?, ?)",
        vec![Binding::fixed(2), Binding::boolean(false)],
    );
    session.query(insert).await?;

    let rows = session.query("SELECT * FROM bind_bool ORDER BY id").await?;
    assert_eq!(rows.len(), 2);
    assert!(rows[0].get::<bool>("FLAG")?);
    assert!(!rows[1].get::<bool>("FLAG")?);

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_real() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_real (id NUMBER, val DOUBLE)")
        .await?;

    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_real (id, val) VALUES (?, ?)",
        vec![Binding::fixed(1), Binding::real(2.72_f64)],
    );
    session.query(insert).await?;

    let rows = session.query("SELECT * FROM bind_real").await?;
    assert_eq!(rows.len(), 1);
    let val = rows[0].get::<f64>("VAL")?;
    assert!((val - 2.72).abs() < 1e-10);

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_date_and_timestamp() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_temporal (id NUMBER, d DATE, ts TIMESTAMP_NTZ)")
        .await?;

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let ts = date.and_hms_opt(12, 30, 45).unwrap();

    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_temporal (id, d, ts) VALUES (?, ?, ?)",
        vec![
            Binding::fixed(1),
            Binding::date(date),
            Binding::timestamp_ntz(ts),
        ],
    );
    session.query(insert).await?;

    let rows = session.query("SELECT * FROM bind_temporal").await?;
    assert_eq!(rows.len(), 1);
    let d = rows[0].get::<NaiveDate>("D")?;
    assert_eq!(d, NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());

    let ts = rows[0].get::<NaiveDateTime>("TS")?;
    assert_eq!(
        ts,
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
    );

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_null() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_null (id NUMBER, val STRING)")
        .await?;

    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_null (id, val) VALUES (?, ?)",
        vec![Binding::fixed(1), Binding::null(BindingType::Text)],
    );
    session.query(insert).await?;

    let rows = session.query("SELECT * FROM bind_null").await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("ID")?, 1);
    let val: Option<String> = rows[0].get("VAL")?;
    assert!(val.is_none(), "expected NULL but got: {val:?}");

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_unicode_and_empty_string() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_unicode (id NUMBER, val STRING)")
        .await?;

    let cases = [(1, ""), (2, "日本語テスト"), (3, "emoji: 🚀✨")];
    for (id, val) in &cases {
        let insert = QueryRequest::with_bindings(
            "INSERT INTO bind_unicode (id, val) VALUES (?, ?)",
            vec![Binding::fixed(*id), Binding::text(*val)],
        );
        session.query(insert).await?;
    }

    let rows = session
        .query("SELECT * FROM bind_unicode ORDER BY id")
        .await?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String>("VAL")?, "");
    assert_eq!(rows[1].get::<String>("VAL")?, "日本語テスト");
    assert_eq!(rows[2].get::<String>("VAL")?, "emoji: 🚀✨");

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_large_integer() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_bigint (id NUMBER, val NUMBER(18,0))")
        .await?;

    let large: i64 = 9_999_999_999_999_999;
    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_bigint (id, val) VALUES (?, ?)",
        vec![Binding::fixed(1_i64), Binding::fixed(large)],
    );
    session.query(insert).await?;

    let rows = session.query("SELECT * FROM bind_bigint").await?;
    assert_eq!(rows[0].get::<i64>("VAL")?, large);

    Ok(())
}

#[tokio::test]
async fn test_bind_parameters_binary() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_bin (id NUMBER, val BINARY)")
        .await?;

    // "48656C6C6F" is hex for "Hello"
    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_bin (id, val) VALUES (?, ?)",
        vec![Binding::fixed(1), Binding::binary("48656C6C6F")],
    );
    session.query(insert).await?;

    let rows = session.query("SELECT * FROM bind_bin").await?;
    assert_eq!(rows.len(), 1);
    let val = rows[0].get::<String>("VAL")?;
    assert_eq!(val, "48656C6C6F");

    Ok(())
}
