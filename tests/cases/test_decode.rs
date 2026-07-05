use super::common;

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use snowflake_connector_rs::{
    BinaryValue, CellValue, ColumnType, Json, Result, SchemaError, Vector, VectorValue,
};

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

    let table = session
        .query(
            "SELECT
                '1969-12-31 23:59:58.900000000'::TIMESTAMP_NTZ(9) AS ntz,
                '1969-12-31 23:59:58.900000000'::TIMESTAMP_LTZ(9) AS ltz,
                '1969-12-31 23:59:58.900000000 +09:00'::TIMESTAMP_TZ(9) AS tz",
        )
        .await?
        .collect_table()
        .await?;

    let expected_local = NaiveDate::from_ymd_opt(1969, 12, 31)
        .unwrap()
        .and_hms_nano_opt(23, 59, 58, 900_000_000)
        .unwrap();
    let expected_tz_utc = NaiveDate::from_ymd_opt(1969, 12, 31)
        .unwrap()
        .and_hms_nano_opt(14, 59, 58, 900_000_000)
        .unwrap()
        .and_utc();

    let (ntz, ltz, tz_utc) = table
        .rows::<(NaiveDateTime, DateTime<Utc>, DateTime<Utc>)>()?
        .next()
        .unwrap()?;
    assert_eq!(ntz, expected_local);
    assert_eq!(ltz, expected_local.and_utc());
    assert_eq!(tz_utc, expected_tz_utc);

    let (_, _, tz_offset) = table
        .rows::<(NaiveDateTime, DateTime<Utc>, DateTime<FixedOffset>)>()?
        .next()
        .unwrap()?;
    assert_eq!(tz_offset.offset().local_minus_utc(), 9 * 3600);
    assert_eq!(tz_offset.naive_local(), expected_local);

    let table = session
        .query(
            "SELECT
                PARSE_JSON('{\"a\":1,\"b\":\"x\"}')                             AS variant_obj,
                ARRAY_CONSTRUCT(1, NULL, 3)                                     AS arr_with_null,
                TO_BINARY('48656C6C6F', 'HEX')                                  AS bin,
                ARRAY_CONSTRUCT(1, 2, 3)::ARRAY(INTEGER)                        AS structured_array,
                OBJECT_CONSTRUCT('name', 'x', 'age', 1)::OBJECT(name VARCHAR, age INTEGER) AS structured_object,
                OBJECT_CONSTRUCT('1', 'a', '2', 'b')::MAP(NUMBER, VARCHAR)      AS number_map",
        )
        .await?
        .collect_table()
        .await?;
    assert_eq!(table.row_count(), 1);

    // Dynamic path: semi-structured columns land in `CellValue::Json`, BINARY in `CellValue::Binary`.
    // The NULL array element surfaces as JSON `null`.
    let row = table.dynamic_rows()?.next().unwrap()?;
    assert_eq!(
        row.value("VARIANT_OBJ").unwrap(),
        &CellValue::Json(serde_json::json!({"a": 1, "b": "x"}))
    );
    assert_eq!(
        row.value("ARR_WITH_NULL").unwrap(),
        &CellValue::Json(serde_json::json!([1, null, 3]))
    );
    assert!(matches!(
        row.value("BIN").unwrap(),
        CellValue::Binary(bin) if bin.as_bytes() == b"Hello"
    ));
    assert_eq!(
        row.value("STRUCTURED_ARRAY").unwrap(),
        &CellValue::Json(serde_json::json!([1, 2, 3]))
    );
    assert_eq!(
        row.value("STRUCTURED_OBJECT").unwrap(),
        &CellValue::Json(serde_json::json!({"name": "x", "age": 1}))
    );
    assert_eq!(
        row.value("NUMBER_MAP").unwrap(),
        &CellValue::Json(serde_json::json!({"1": "a", "2": "b"}))
    );

    // Typed path: `serde_json::Value`, `Json<T>` for concrete Rust shapes, and `BinaryValue` for BINARY.
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Person {
        name: String,
        age: i64,
    }

    let (variant_obj, arr_with_null, bin, structured_array, structured_object, number_map) = table
        .rows::<(
            serde_json::Value,
            Json<Vec<Option<i64>>>,
            BinaryValue,
            Json<Vec<i64>>,
            Json<Person>,
            Json<BTreeMap<i64, String>>,
        )>()?
        .next()
        .unwrap()?;

    assert_eq!(variant_obj, serde_json::json!({"a": 1, "b": "x"}));
    assert_eq!(arr_with_null.into_inner(), vec![Some(1), None, Some(3)]);
    assert_eq!(bin.as_bytes(), b"Hello");
    assert_eq!(structured_array.into_inner(), vec![1, 2, 3]);
    assert_eq!(
        structured_object.into_inner(),
        Person {
            name: "x".to_owned(),
            age: 1
        }
    );
    assert_eq!(
        number_map.into_inner(),
        BTreeMap::from([(1, "a".to_owned()), (2, "b".to_owned())])
    );

    // GeoJSON: reported as `Object`, decodes as `serde_json::Value`.
    session
        .query("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='GeoJSON'")
        .await?;
    session
        .query("ALTER SESSION SET GEOMETRY_OUTPUT_FORMAT='GeoJSON'")
        .await?;
    let table = session
        .query_as::<(serde_json::Value, serde_json::Value), _>(
            "SELECT
                TO_GEOGRAPHY('POINT(-122.35 37.55)') AS geog,
                TO_GEOMETRY('POINT(1 2)') AS geom",
        )
        .await?
        .collect_table()
        .await?;
    let columns = table.schema().columns();
    assert!(matches!(columns[0].ty(), ColumnType::Object));
    assert!(matches!(columns[1].ty(), ColumnType::Object));
    let (geog, geom) = table.rows().next().unwrap()?;
    assert_eq!(
        geog,
        serde_json::json!({"type": "Point", "coordinates": [-122.35, 37.55]})
    );
    // GEOMETRY GeoJSON renders coordinates in scientific notation, but they parse to plain JSON numbers.
    assert_eq!(
        geom,
        serde_json::json!({"type": "Point", "coordinates": [1.0, 2.0]})
    );

    // WKT / EWKT: reported as `Text`, decodes as `String`; EWKT prefixes the value with `SRID=<n>;`.
    session
        .query("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT'")
        .await?;
    session
        .query("ALTER SESSION SET GEOMETRY_OUTPUT_FORMAT='EWKT'")
        .await?;
    let table = session
        .query_as::<(String, String), _>(
            "SELECT
                TO_GEOGRAPHY('POINT(-122.35 37.55)') AS geog,
                ST_GEOMFROMWKT('LINESTRING(0 0, 10 10)', 3857) AS geom",
        )
        .await?
        .collect_table()
        .await?;
    let columns = table.schema().columns();
    assert!(matches!(columns[0].ty(), ColumnType::Text { .. }));
    assert!(matches!(columns[1].ty(), ColumnType::Text { .. }));
    let (wkt, ewkt) = table.rows().next().unwrap()?;
    assert_eq!(wkt, "POINT(-122.35 37.55)");
    assert_eq!(ewkt, "SRID=3857;LINESTRING(0 0,10 10)");

    // WKB / EWKB: reported as `Binary`, decodes as `BinaryValue`; EWKB carries the SRID flag bit and SRID bytes.
    session
        .query("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKB'")
        .await?;
    session
        .query("ALTER SESSION SET GEOMETRY_OUTPUT_FORMAT='EWKB'")
        .await?;
    let table = session
        .query_as::<(BinaryValue, BinaryValue), _>(
            "SELECT
                TO_GEOGRAPHY('POINT(-122.35 37.55)') AS geog,
                ST_GEOMFROMWKT('LINESTRING(0 0, 10 10)', 3857) AS geom",
        )
        .await?
        .collect_table()
        .await?;
    let columns = table.schema().columns();
    assert!(matches!(columns[0].ty(), ColumnType::Binary { .. }));
    assert!(matches!(columns[1].ty(), ColumnType::Binary { .. }));
    let (wkb, ewkb) = table.rows().next().unwrap()?;
    // WKB: little-endian byte order marker, then geometry type 1 (Point) with no SRID flag.
    let wkb = wkb.as_bytes();
    assert_eq!(wkb[0], 0x01);
    assert_eq!(&wkb[1..5], &[0x01, 0x00, 0x00, 0x00]);
    // EWKB: little-endian marker, geometry type 2 (LineString) with the SRID flag bit (0x20000000) set,
    // followed by the SRID as a 4-byte little-endian integer (3857 = 0x0F11).
    let ewkb = ewkb.as_bytes();
    assert_eq!(ewkb[0], 0x01);
    assert_eq!(&ewkb[1..5], &[0x02, 0x00, 0x00, 0x20]);
    assert_eq!(
        u32::from_le_bytes([ewkb[5], ewkb[6], ewkb[7], ewkb[8]]),
        3857
    );

    // A geo cell that is SQL NULL decodes to `None`.
    let table = session
        .query_as::<(Option<BinaryValue>,), _>(
            "SELECT IFF(1 = 0, TO_GEOGRAPHY('POINT(0 0)'), NULL) AS geog",
        )
        .await?
        .collect_table()
        .await?;
    let (geog_null,) = table.rows().next().unwrap()?;
    assert!(geog_null.is_none());

    // Snowflake serializes non-finite floats in VARIANT / OBJECT / ARRAY payloads as bare `Infinity` / `-Infinity` /
    // `NaN`, which are not valid JSON. The decode error should call out the Snowflake token.
    let err = session
        .query_as::<(serde_json::Value,), _>("SELECT TO_VARIANT('inf'::DOUBLE) AS v")
        .await?
        .collect_table()
        .await?
        .rows()
        .next()
        .unwrap()
        .expect_err("non-finite float must not decode as JSON");
    match err.as_cell_decode_error() {
        Some(decode) => assert!(
            decode
                .conversion_error()
                .reason()
                .contains("non-finite float token `Infinity`"),
            "actual: {}",
            decode.conversion_error().reason()
        ),
        other => panic!("expected Decode error, got: {other:?}"),
    }

    // VECTOR: reported as `ColumnType::Vector` with no element-type or dimension metadata; the element type is
    // recovered from the payload. `Vector<i32>` reads VECTOR(INT); `Vector<f32>` reads VECTOR(FLOAT) and restores
    // Snowflake's lowercase `inf` / `-inf` / `nan` tokens.
    let table = session
        .query(
            "SELECT
                [1, 2, 3]::VECTOR(INT, 3)                                     AS vec_int,
                [1.5, -2.25, 0]::VECTOR(FLOAT, 3)                             AS vec_float,
                ['inf'::FLOAT, '-inf'::FLOAT, 'NaN'::FLOAT]::VECTOR(FLOAT, 3) AS vec_nonfinite",
        )
        .await?
        .collect_table()
        .await?;
    for column in table.schema().columns() {
        assert_eq!(column.ty(), &ColumnType::Vector);
    }

    // Dynamic path: element type inferred from the payload.
    let row = table.dynamic_rows()?.next().unwrap()?;
    match row.value("VEC_INT").unwrap() {
        CellValue::Vector(VectorValue::Int(v)) => assert_eq!(v.as_slice(), &[1, 2, 3]),
        other => panic!("expected Vector(Int), got {other:?}"),
    }
    match row.value("VEC_FLOAT").unwrap() {
        CellValue::Vector(VectorValue::Float(v)) => assert_eq!(v.as_slice(), &[1.5f32, -2.25, 0.0]),
        other => panic!("expected Vector(Float), got {other:?}"),
    }

    // Typed path: non-finite tokens restore to the corresponding `f32` values.
    let (vec_int, vec_float, vec_nonfinite) = table
        .rows::<(Vector<i32>, Vector<f32>, Vector<f32>)>()?
        .next()
        .unwrap()?;
    assert_eq!(vec_int.as_slice(), &[1, 2, 3]);
    assert_eq!(vec_float.as_slice(), &[1.5f32, -2.25, 0.0]);
    let nf = vec_nonfinite.as_slice();
    assert!(nf[0].is_infinite() && nf[0].is_sign_positive());
    assert!(nf[1].is_infinite() && nf[1].is_sign_negative());
    assert!(nf[2].is_nan());

    // Element-type mismatch is a per-cell decode error: the INT column rejects float tokens, the FLOAT column
    // rejects integer tokens. The first offending column drives the row error.
    let err = table
        .rows::<(Vector<f32>, Vector<f32>, Vector<f32>)>()?
        .next()
        .unwrap()
        .expect_err("VECTOR(INT) must not decode as Vector<f32>");
    assert!(
        err.as_cell_decode_error().is_some(),
        "unexpected error: {err:?}"
    );
    let err = table
        .rows::<(Vector<i32>, Vector<i32>, Vector<i32>)>()?
        .next()
        .unwrap()
        .expect_err("VECTOR(FLOAT) must not decode as Vector<i32>");
    assert!(
        err.as_cell_decode_error().is_some(),
        "unexpected error: {err:?}"
    );

    // SQL NULL vector decodes to `None`.
    let table = session
        .query("SELECT IFF(1 = 0, [1, 2, 3]::VECTOR(INT, 3), NULL) AS v")
        .await?
        .collect_table()
        .await?;
    let (vec_null,) = table.rows::<(Option<Vector<i32>>,)>()?.next().unwrap()?;
    assert!(vec_null.is_none());

    // VECTOR(FLOAT) renders every element as a fixed decimal (always a `.`, never exponent notation) across the full
    // f32 magnitude range: extremes render as e.g. `3.4e38 -> 339...704.000000` and tiny values as `0.000000`.
    // `Vector<f32>` decoding relies on this, since an integer-only token would be rejected as a VECTOR(INT) payload.
    let table = session
        .query(
            "SELECT
                [3.4e38, 1e-30]::VECTOR(FLOAT, 2)               AS huge_and_tiny,
                [3.4028235e38, -3.4028235e38]::VECTOR(FLOAT, 2) AS f32_max,
                [1.4e-45, 9.9e37]::VECTOR(FLOAT, 2)             AS subnormal_and_large",
        )
        .await?
        .collect_table()
        .await?;
    let (a, b, c) = table.rows::<(String, String, String)>()?.next().unwrap()?;
    for raw in [&a, &b, &c] {
        let body = raw
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
            .unwrap_or_else(|| panic!("vector wire is not a bracketed list: {raw}"));
        for token in body.split(',') {
            let token = token.trim();
            assert!(
                token.contains('.'),
                "element `{token}` lacks a decimal point: {raw}"
            );
            assert!(
                !token.contains(['e', 'E']),
                "element `{token}` uses exponent notation: {raw}"
            );
        }
    }
    // The fixed-decimal wire always decodes as `Vector<f32>`.
    table
        .rows::<(Vector<f32>, Vector<f32>, Vector<f32>)>()?
        .next()
        .unwrap()?;

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

/// Verify TIMESTAMP_TZ decoding into `DateTime<FixedOffset>` and `DateTime<Utc>` for both eastern (UTC+09:00) and
/// western (UTC-05:00) offsets.
#[tokio::test]
async fn test_decode_timestamp_tz_offsets() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Use a session timezone that is not UTC so any accidental session-default leakage shows up in the failure mode.
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
