use chrono::NaiveDateTime;
use snowflake_connector_rs::{DecimalValue, Error, FromRow, Result, SchemaError};

use super::common;

#[derive(Debug, FromRow, PartialEq)]
struct UserRow {
    id: i64,
    #[snowflake(rename = "USER_NAME")]
    name: String,
    is_active: bool,
}

#[derive(Debug, FromRow, PartialEq)]
struct UserWithDefault {
    id: i64,
    #[snowflake(rename = "USER_NAME")]
    name: String,
    /// `bio` doesn't exist in the result; `default` lets it materialise as None.
    #[snowflake(default)]
    bio: Option<String>,
}

#[derive(Debug, FromRow, PartialEq)]
struct UserWithNullDefault {
    id: i64,
    #[snowflake(default)]
    bio: String,
}

#[derive(Debug, FromRow, PartialEq)]
#[snowflake(by_position)]
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
struct IdentifierLookupRow {
    id: i64,
}

#[tokio::test]
async fn derive_named_struct_decodes_query_results() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query(
            "CREATE TEMPORARY TABLE derive_users (
                id NUMBER,
                user_name STRING,
                is_active BOOLEAN
            )",
        )
        .await?;
    session
        .query(
            "INSERT INTO derive_users VALUES
                (1, 'alice', TRUE),
                (2, 'bob',   FALSE)",
        )
        .await?;

    let users = session
        .query_as::<UserRow, _>("SELECT id, user_name, is_active FROM derive_users ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(
        users,
        vec![
            UserRow {
                id: 1,
                name: "alice".to_string(),
                is_active: true,
            },
            UserRow {
                id: 2,
                name: "bob".to_string(),
                is_active: false,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn derive_default_attribute_handles_missing_column() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE derive_users_no_bio (id NUMBER, user_name STRING)")
        .await?;
    session
        .query("INSERT INTO derive_users_no_bio VALUES (1, 'alice')")
        .await?;

    // `bio` is absent from the result schema; `#[snowflake(default)]` makes
    // the field default to `None` instead of erroring.
    let users = session
        .query_as::<UserWithDefault, _>("SELECT id, user_name FROM derive_users_no_bio")
        .await?
        .collect()
        .await?;

    assert_eq!(
        users,
        vec![UserWithDefault {
            id: 1,
            name: "alice".to_string(),
            bio: None,
        }]
    );
    Ok(())
}

#[tokio::test]
async fn derive_default_attribute_handles_null_column() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let users = session
        .query_as::<UserWithNullDefault, _>("SELECT 1 AS id, NULL::STRING AS bio")
        .await?
        .collect()
        .await?;

    assert_eq!(
        users,
        vec![UserWithNullDefault {
            id: 1,
            bio: String::new(),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn derive_by_position_tuple_struct() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let rows = session
        .query_as::<CountAndName, _>("SELECT 42, 'hi' UNION ALL SELECT 7, 'lo' ORDER BY 1 DESC")
        .await?
        .collect()
        .await?;

    assert_eq!(
        rows,
        vec![
            CountAndName(42, "hi".to_string()),
            CountAndName(7, "lo".to_string()),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn derive_default_screaming_snake_case_mapping() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session.query("ALTER SESSION SET TIMEZONE = 'UTC'").await?;
    session
        .query(
            "CREATE TEMPORARY TABLE derive_events (
                id NUMBER,
                created_at TIMESTAMP_NTZ
            )",
        )
        .await?;
    session
        .query(
            "INSERT INTO derive_events VALUES
                (1, '2024-01-01 00:00:00')",
        )
        .await?;

    let events = session
        .query_as::<EventRow, _>("SELECT id, created_at FROM derive_events")
        .await?
        .collect()
        .await?;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, 1);
    assert_eq!(
        events[0].created_at,
        NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    Ok(())
}

#[tokio::test]
async fn derive_decimal_value_field_decodes_query_results() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE derive_prices (id NUMBER, price DECIMAL(10,2))")
        .await?;
    session
        .query(
            "INSERT INTO derive_prices VALUES
                (1, 99.99),
                (2, 149.99)",
        )
        .await?;

    let rows = session
        .query_as::<PriceRow, _>("SELECT id, price FROM derive_prices ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(
        rows,
        vec![
            PriceRow {
                id: 1,
                price: DecimalValue::new("99.99", Some(10), Some(2)),
            },
            PriceRow {
                id: 2,
                price: DecimalValue::new("149.99", Some(10), Some(2)),
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn derive_raw_identifier_uses_logical_field_name() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let rows = session
        .query_as::<KeywordRow, _>("SELECT 1 AS TYPE")
        .await?
        .collect()
        .await?;

    assert_eq!(rows, vec![KeywordRow { r#type: 1 }]);
    Ok(())
}

#[tokio::test]
async fn derive_identifier_lookup_distinguishes_quoted_labels() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let rows = session
        .query_as::<IdentifierLookupRow, _>(r#"SELECT 1 AS id, 2 AS "id""#)
        .await?
        .collect()
        .await?;

    assert_eq!(rows, vec![IdentifierLookupRow { id: 1 }]);
    Ok(())
}

#[derive(Debug, FromRow, PartialEq)]
struct AmbiguousIdWithDefault {
    /// `id` resolves through unquoted identifier lookup. If the result schema
    /// contains two raw `ID` labels, the identifier namespace is ambiguous;
    /// `#[snowflake(default)]` must NOT swallow that.
    #[snowflake(default)]
    id: Option<i64>,
}

#[tokio::test]
async fn derive_default_does_not_swallow_ambiguous_column() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let err = session
        .query_as::<AmbiguousIdWithDefault, _>("SELECT 1 AS id, 2 AS ID")
        .await
        .err()
        .expect("ambiguous identifier lookup should not silently default");

    match err {
        Error::Schema(SchemaError::AmbiguousColumn { .. }) => {}
        other => panic!("expected AmbiguousColumn, got: {other:?}"),
    }
    Ok(())
}
