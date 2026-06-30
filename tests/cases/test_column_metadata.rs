use super::common;

use snowflake_connector_rs::{
    Result,
    result::{ColumnType, Schema},
};

// Verifies the wire -> ColumnType metadata mapping against live Snowflake: the declared precision / scale / length land
// in the expected variants and widths. `CAST(NULL AS T)` carries the declared type's metadata without needing rows.
#[tokio::test]
async fn column_type_metadata_maps_declared_types() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let sql = "
    SELECT
        CAST(NULL AS NUMBER(38,0))      AS num_38_0,
        CAST(NULL AS NUMBER(10,2))      AS num_10_2,
        CAST(NULL AS INT)               AS int_col,
        CAST(NULL AS FLOAT)             AS float_col,
        CAST(NULL AS VARCHAR(255))      AS vc_255,
        CAST(NULL AS CHAR(10))          AS char_10,
        CAST(NULL AS BOOLEAN)           AS bool_col,
        CAST(NULL AS DATE)              AS date_col,
        CAST(NULL AS TIME(3))           AS time_3,
        CAST(NULL AS TIMESTAMP_NTZ(9))  AS ts_ntz_9,
        CAST(NULL AS TIMESTAMP_LTZ(3))  AS ts_ltz_3,
        CAST(NULL AS TIMESTAMP_TZ(0))   AS ts_tz_0,
        CAST(NULL AS BINARY)            AS bin_col,
        CAST(NULL AS VARIANT)           AS variant_col,
        CAST(NULL AS OBJECT)            AS object_col,
        CAST(NULL AS ARRAY)             AS array_col
    ";
    let result = session.query(sql).await?;
    let schema = result.schema();

    assert!(matches!(
        ty(schema, "NUM_38_0"),
        ColumnType::Fixed {
            precision: Some(38),
            scale: Some(0),
            ..
        }
    ));
    assert!(matches!(
        ty(schema, "NUM_10_2"),
        ColumnType::Fixed {
            precision: Some(10),
            scale: Some(2),
            ..
        }
    ));
    assert!(matches!(
        ty(schema, "INT_COL"),
        ColumnType::Fixed {
            precision: Some(38),
            scale: Some(0),
            ..
        }
    ));
    assert!(matches!(ty(schema, "FLOAT_COL"), ColumnType::Real));
    assert!(matches!(
        ty(schema, "VC_255"),
        ColumnType::Text {
            length: Some(255),
            ..
        }
    ));
    assert!(matches!(
        ty(schema, "CHAR_10"),
        ColumnType::Text {
            length: Some(10),
            ..
        }
    ));
    assert!(matches!(ty(schema, "BOOL_COL"), ColumnType::Boolean));
    assert!(matches!(ty(schema, "DATE_COL"), ColumnType::Date));
    assert!(matches!(
        ty(schema, "TIME_3"),
        ColumnType::Time { scale: Some(3), .. }
    ));
    assert!(matches!(
        ty(schema, "TS_NTZ_9"),
        ColumnType::TimestampNtz { scale: Some(9), .. }
    ));
    assert!(matches!(
        ty(schema, "TS_LTZ_3"),
        ColumnType::TimestampLtz { scale: Some(3), .. }
    ));
    assert!(matches!(
        ty(schema, "TS_TZ_0"),
        ColumnType::TimestampTz { scale: Some(0), .. }
    ));
    // Binary reports a default max byte length; assert the variant and that a length is present,
    // not the exact value (it is not declared here).
    assert!(matches!(
        ty(schema, "BIN_COL"),
        ColumnType::Binary {
            length: Some(_),
            ..
        }
    ));
    assert!(matches!(ty(schema, "VARIANT_COL"), ColumnType::Variant));
    assert!(matches!(ty(schema, "OBJECT_COL"), ColumnType::Object));
    assert!(matches!(ty(schema, "ARRAY_COL"), ColumnType::Array));

    Ok(())
}

fn ty<'a>(schema: &'a Schema, name: &str) -> &'a ColumnType {
    schema
        .columns()
        .iter()
        .find(|c| c.name() == name)
        .unwrap_or_else(|| panic!("column {name} not found in schema"))
        .ty()
}
