use chrono::NaiveDateTime;

use snowflake_connector_rs::{DecimalValue, FromRow, Result, SchemaError};

use super::common;

#[derive(Debug, FromRow, PartialEq)]
struct UserRow {
    id: i64,
    #[snowflake(rename = "USER_NAME")]
    name: String,
    is_active: bool,
    bio: Option<String>,
}

#[derive(Debug, FromRow, PartialEq)]
struct RequiredNoteRow {
    id: i64,
    note: String,
}

#[derive(Debug, FromRow, PartialEq)]
struct DefaultCaseValueRow {
    value: i64,
}

#[derive(Debug, FromRow, PartialEq)]
struct AmbiguousIdRow {
    id: i64,
}

#[derive(Debug, FromRow, PartialEq)]
#[snowflake(positional)]
struct CountAndName(i64, String);

#[derive(Debug, FromRow, PartialEq)]
struct EventRow {
    id: i64,
    /// SCREAMING_SNAKE_CASE is the default — `created_at` maps to `CREATED_AT`.
    created_at: NaiveDateTime,
}

#[derive(Debug, FromRow, PartialEq)]
struct PriceRow {
    id: i64,
    price: DecimalValue,
}

#[derive(Debug, FromRow, PartialEq)]
struct KeywordRow {
    r#type: i64,
}

#[derive(Debug, FromRow, PartialEq)]
struct QuotedAliasRow {
    #[snowflake(rename = "value")]
    v: i64,
}

#[derive(Debug, FromRow, PartialEq)]
#[snowflake(rename_all = "none")]
struct SessionParameterRow {
    key: String,
    value: String,
    level: String,
}

#[derive(Debug, FromRow, PartialEq)]
#[snowflake(positional)]
struct NamedByPosition {
    id: i64,
    label: String,
}

#[tokio::test]
async fn derive_named_lookup_variants_decode_expected_rows() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let users = session
        .query_as(
            "SELECT 1 AS id, 'alice' AS user_name, TRUE AS is_active, NULL::STRING AS bio
             UNION ALL
             SELECT 2 AS id, 'bob' AS user_name, FALSE AS is_active, 'hello' AS bio
             ORDER BY 1",
        )
        .await?
        .collect::<Vec<UserRow>>()
        .await?;

    assert_eq!(
        users,
        vec![
            UserRow {
                id: 1,
                name: "alice".to_string(),
                is_active: true,
                bio: None,
            },
            UserRow {
                id: 2,
                name: "bob".to_string(),
                is_active: false,
                bio: Some("hello".to_string()),
            },
        ]
    );

    session.query("ALTER SESSION SET TIMEZONE = 'UTC'").await?;

    let events = session
        .query_as("SELECT 1 AS id, '2024-01-01 00:00:00'::TIMESTAMP_NTZ AS created_at")
        .await?
        .collect::<Vec<EventRow>>()
        .await?;

    assert_eq!(
        events,
        vec![EventRow {
            id: 1,
            created_at: NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
        }]
    );

    let prices = session
        .query_as(
            "SELECT 1 AS id, 99.99::DECIMAL(10,2) AS price
             UNION ALL
             SELECT 2 AS id, 149.99::DECIMAL(10,2) AS price
             ORDER BY 1",
        )
        .await?
        .collect::<Vec<PriceRow>>()
        .await?;

    let expected_prices = [(1_i64, "99.99"), (2, "149.99")];
    assert_eq!(prices.len(), expected_prices.len());

    for (row, (exp_id, exp_raw)) in prices.iter().zip(expected_prices) {
        assert_eq!(row.id, exp_id);
        assert_eq!(row.price.raw(), exp_raw);
    }

    let keywords = session
        .query_as("SELECT 1 AS TYPE")
        .await?
        .collect::<Vec<KeywordRow>>()
        .await?;

    assert_eq!(keywords, vec![KeywordRow { r#type: 1 }]);

    let renamed = session
        .query_as(r#"SELECT 1 AS "value""#)
        .await?
        .collect::<Vec<QuotedAliasRow>>()
        .await?;

    assert_eq!(renamed, vec![QuotedAliasRow { v: 1 }]);

    Ok(())
}

#[tokio::test]
async fn derive_required_fields_surface_schema_and_decode_errors() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let err = session
        .query_as::<UserRow, _>("SELECT 1 AS id, 'alice' AS user_name, TRUE AS is_active")
        .await
        .expect_err("missing named column should fail during plan building");

    match err.as_schema_error() {
        Some(SchemaError::MissingColumn(error)) => assert_eq!(error.name(), "BIO"),
        other => panic!("expected MissingColumn, got: {other:?}"),
    }

    let err = session
        .query_as::<DefaultCaseValueRow, _>(r#"SELECT 1 AS "value""#)
        .await
        .expect_err("default lookup should not match lowercase quoted labels");

    match err.as_schema_error() {
        Some(SchemaError::MissingColumn(error)) => assert_eq!(error.name(), "VALUE"),
        other => panic!("expected MissingColumn, got: {other:?}"),
    }

    let err = session
        .query_as::<CountAndName, _>("SELECT 42")
        .await
        .expect_err("short positional row should fail during plan building");

    match err.as_schema_error() {
        Some(SchemaError::ColumnCountMismatch(error)) => {
            assert_eq!(error.expected(), 2);
            assert_eq!(error.actual(), 1);
        }
        other => panic!("expected ColumnCountMismatch, got: {other:?}"),
    }

    let err = session
        .query_as("SELECT 1 AS id, NULL::STRING AS note")
        .await?
        .collect::<Vec<RequiredNoteRow>>()
        .await
        .expect_err("non-Option NULL should surface the decode error");

    match err.as_cell_decode_error() {
        Some(err) => {
            assert_eq!(err.column_name(), "NOTE");
            assert_eq!(err.conversion_error().reason(), "value is NULL");
        }
        other => panic!("expected Decode error, got: {other:?}"),
    }

    let err = session
        .query_as::<AmbiguousIdRow, _>("SELECT 1 AS id, 2 AS ID")
        .await
        .expect_err("duplicate raw labels should fail during plan building");

    assert!(matches!(
        err.as_schema_error(),
        Some(SchemaError::AmbiguousColumn(_))
    ));

    Ok(())
}

#[tokio::test]
async fn derive_nondefault_lookup_modes_cover_metadata_and_named_position_paths() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("ALTER SESSION SET TIMEZONE = 'Asia/Tokyo'")
        .await?;

    let rows = session
        .query_as("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION")
        .await?
        .collect::<Vec<SessionParameterRow>>()
        .await?;

    assert_eq!(
        rows,
        vec![SessionParameterRow {
            key: "TIMEZONE".to_string(),
            value: "Asia/Tokyo".to_string(),
            level: "SESSION".to_string(),
        }]
    );

    let rows = session
        .query_as("SELECT 42, 'hi' UNION ALL SELECT 7, 'lo' ORDER BY 1 DESC")
        .await?
        .collect::<Vec<CountAndName>>()
        .await?;

    assert_eq!(
        rows,
        vec![
            CountAndName(42, "hi".to_string()),
            CountAndName(7, "lo".to_string()),
        ]
    );

    let rows = session
        .query_as(r#"SELECT 9, 'named row'"#)
        .await?
        .collect::<Vec<NamedByPosition>>()
        .await?;

    assert_eq!(
        rows,
        vec![NamedByPosition {
            id: 9,
            label: "named row".to_string(),
        }]
    );
    Ok(())
}
