use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use snowflake_connector_rs::{Binding, BindingType, QueryRequest, Result};

use super::common;

struct BindingRoundTripCase {
    id: i64,
    text: &'static str,
    flag: bool,
    real: f64,
    date: NaiveDate,
    time: NaiveTime,
    ts_ntz: NaiveDateTime,
    ts_ltz_utc: NaiveDateTime,
    nullable_text: Option<&'static str>,
    bigint: i64,
    binary: &'static [u8],
    expected_hex: &'static str,
}

#[tokio::test]
async fn test_bind_parameters_round_trip_common_types_and_where_clause() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query(
            "CREATE TEMPORARY TABLE bind_round_trip (
                id NUMBER,
                text_val STRING,
                flag BOOLEAN,
                real_val DOUBLE,
                date_val DATE,
                time_val TIME,
                ts_ntz TIMESTAMP_NTZ,
                ts_ltz TIMESTAMP_LTZ,
                nullable_text STRING,
                bigint_val NUMBER(18,0),
                binary_val BINARY
            )",
        )
        .await?;

    let cases = [
        BindingRoundTripCase {
            id: 1,
            text: "",
            flag: true,
            real: 2.72,
            date: NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            time: NaiveTime::from_hms_nano_opt(12, 34, 56, 123_000_000).unwrap(),
            ts_ntz: NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(12, 30, 45)
                .unwrap(),
            ts_ltz_utc: NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(12, 30, 45)
                .unwrap(),
            nullable_text: None,
            bigint: 9_999_999_999_999_999,
            binary: b"Hello",
            expected_hex: "48656C6C6F",
        },
        BindingRoundTripCase {
            id: 2,
            text: "日本語テスト",
            flag: false,
            real: -42.5,
            date: NaiveDate::from_ymd_opt(2024, 6, 16).unwrap(),
            time: NaiveTime::from_hms_nano_opt(1, 2, 3, 456_000_000).unwrap(),
            ts_ntz: NaiveDate::from_ymd_opt(2024, 6, 16)
                .unwrap()
                .and_hms_opt(1, 2, 3)
                .unwrap(),
            ts_ltz_utc: NaiveDate::from_ymd_opt(2024, 6, 16)
                .unwrap()
                .and_hms_opt(1, 2, 3)
                .unwrap(),
            nullable_text: Some("emoji: 🚀✨"),
            bigint: 9_999_999_999_999_998,
            binary: b"Rust",
            expected_hex: "52757374",
        },
    ];

    for case in &cases {
        let insert = QueryRequest::with_bindings(
            "INSERT INTO bind_round_trip (
                id,
                text_val,
                flag,
                real_val,
                date_val,
                time_val,
                ts_ntz,
                ts_ltz,
                nullable_text,
                bigint_val,
                binary_val
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            vec![
                Binding::fixed(case.id),
                Binding::text(case.text),
                Binding::boolean(case.flag),
                Binding::real(case.real),
                Binding::date(case.date),
                Binding::time(case.time),
                Binding::timestamp_ntz(case.ts_ntz),
                Binding::timestamp_ltz(case.ts_ltz_utc.and_utc()),
                match case.nullable_text {
                    Some(value) => Binding::text(value),
                    None => Binding::null(BindingType::Text),
                },
                Binding::fixed(case.bigint),
                Binding::binary(case.binary),
            ],
        );
        session.query(insert).await?;
    }

    let rows = session
        .query_as::<(
            i64,
            String,
            bool,
            f64,
            NaiveDate,
            NaiveTime,
            NaiveDateTime,
            DateTime<Utc>,
            Option<String>,
            i64,
            String,
        ), _>(
            "SELECT
                id,
                text_val,
                flag,
                real_val,
                date_val,
                time_val,
                ts_ntz,
                ts_ltz,
                nullable_text,
                bigint_val,
                binary_val
            FROM
                bind_round_trip
            ORDER BY
                id",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(rows.len(), cases.len());

    for (row, case) in rows.iter().zip(cases.iter()) {
        assert_eq!(row.0, case.id);
        assert_eq!(row.1, case.text);
        assert_eq!(row.2, case.flag);
        assert!(
            (row.3 - case.real).abs() < 1e-10,
            "expected REAL {} but got {} for id {}",
            case.real,
            row.3,
            case.id
        );
        assert_eq!(row.4, case.date);
        assert_eq!(row.5, case.time);
        assert_eq!(row.6, case.ts_ntz);
        assert_eq!(row.7, case.ts_ltz_utc.and_utc());
        assert_eq!(row.8.as_deref(), case.nullable_text);
        assert_eq!(row.9, case.bigint);
        assert_eq!(row.10, case.expected_hex);
    }

    let filtered = session
        .query_as::<(String, Option<String>), _>(QueryRequest::with_bindings(
            "SELECT text_val, nullable_text FROM bind_round_trip WHERE id = ?",
            vec![Binding::fixed(2_i64)],
        ))
        .await?
        .collect()
        .await?;
    assert_eq!(
        filtered,
        vec![("日本語テスト".to_string(), Some("emoji: 🚀✨".to_string()))]
    );

    Ok(())
}

fn timestamp_tz_probe_value(offset: FixedOffset) -> DateTime<FixedOffset> {
    NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(12, 30, 45)
        .unwrap()
        .and_local_timezone(offset)
        .unwrap()
}

async fn render_bound_timestamp_tz(
    session: &snowflake_connector_rs::SnowflakeSession,
    offset: FixedOffset,
) -> Result<String> {
    let dt = timestamp_tz_probe_value(offset);
    let query = QueryRequest::with_bindings(
        "SELECT TO_CHAR(?::TIMESTAMP_TZ, 'YYYY-MM-DD HH24:MI:SS TZHTZM') AS ts_str",
        vec![Binding::timestamp_tz(dt)],
    );
    let rows = session
        .query_as::<(String,), _>(query)
        .await?
        .collect()
        .await?;
    Ok(rows.into_iter().next().unwrap().0)
}

fn format_tzhtzm(offset: FixedOffset) -> String {
    let total_minutes = offset.local_minus_utc() / 60;
    let sign = if total_minutes < 0 { '-' } else { '+' };
    let abs = total_minutes.abs();
    let hours = abs / 60;
    let minutes = abs % 60;
    format!("{sign}{hours:02}{minutes:02}")
}

#[tokio::test]
async fn test_bind_parameters_timestamp_tz_round_trips_control_and_extreme_offsets() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    session
        .query("CREATE TEMPORARY TABLE bind_ts_tz (id NUMBER, ts TIMESTAMP_TZ)")
        .await?;

    let control_offset = FixedOffset::east_opt(9 * 3600).unwrap();
    let control_insert = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(21, 30, 45)
        .unwrap()
        .and_local_timezone(control_offset)
        .unwrap();
    let insert = QueryRequest::with_bindings(
        "INSERT INTO bind_ts_tz (id, ts) VALUES (?, ?)",
        vec![Binding::fixed(1), Binding::timestamp_tz(control_insert)],
    );
    session.query(insert).await?;
    session
        .query("INSERT INTO bind_ts_tz VALUES (2, '2024-06-15 21:30:45 +09:00'::TIMESTAMP_TZ)")
        .await?;

    let inserted_rows = session
        .query_as::<(String,), _>(
            "SELECT
                TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS TZHTZM') AS ts_str
            FROM
                bind_ts_tz
            ORDER BY
                id",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(inserted_rows.len(), 2);
    assert_eq!(inserted_rows[0].0, inserted_rows[1].0);

    let control_literal = session
        .query_as::<(String,), _>(
            "SELECT TO_CHAR('2024-06-15 12:30:45 +09:00'::TIMESTAMP_TZ, 'YYYY-MM-DD HH24:MI:SS TZHTZM') AS ts_str",
        )
        .await?
        .collect()
        .await?;
    let control_bound = render_bound_timestamp_tz(&session, control_offset).await?;
    assert_eq!(control_bound, control_literal.into_iter().next().unwrap().0);

    let candidates = [
        ("plus_14_01", FixedOffset::east_opt(14 * 3600 + 60).unwrap()),
        (
            "plus_14_30",
            FixedOffset::east_opt(14 * 3600 + 30 * 60).unwrap(),
        ),
        ("plus_15_00", FixedOffset::east_opt(15 * 3600).unwrap()),
        (
            "plus_23_59",
            FixedOffset::east_opt(23 * 3600 + 59 * 60).unwrap(),
        ),
        (
            "minus_12_01",
            FixedOffset::west_opt(12 * 3600 + 60).unwrap(),
        ),
        ("minus_13_00", FixedOffset::west_opt(13 * 3600).unwrap()),
        (
            "minus_23_59",
            FixedOffset::west_opt(23 * 3600 + 59 * 60).unwrap(),
        ),
    ];

    for (label, offset) in candidates {
        let rendered = render_bound_timestamp_tz(&session, offset)
            .await
            .map_err(|err| {
                snowflake_connector_rs::Error::other(format!(
                    "{label} bind unexpectedly failed: {err}"
                ))
            })?;
        assert_eq!(
            rendered,
            format!("2024-06-15 12:30:45 {}", format_tzhtzm(offset)),
            "{label} should round-trip through TIMESTAMP_TZ bind"
        );
    }

    Ok(())
}
