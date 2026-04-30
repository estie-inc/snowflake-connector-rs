use super::common;

use snowflake_connector_rs::{ColumnType, Result};

#[tokio::test]
async fn result_set_query_id_and_schema_survive_inline_empty_and_chunked_paths() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let mut inline = session.query("SELECT 1 AS X, 2 AS Y").await?;
    let inline_query_id = inline.query_id().to_string();
    assert!(
        !inline.query_id().is_empty(),
        "session.query() must expose a non-empty query_id before collection"
    );
    let inline_columns = inline.schema().columns();
    assert_eq!(inline_columns.len(), 2);
    assert_eq!(inline_columns[0].name(), "X");
    assert!(matches!(inline_columns[0].ty(), ColumnType::Fixed { .. }));
    assert_eq!(inline_columns[1].name(), "Y");
    assert!(matches!(inline_columns[1].ty(), ColumnType::Fixed { .. }));

    let inline_table = inline
        .next_table()
        .await?
        .expect("inline partition should be present");
    assert_eq!(inline_table.query_id(), inline_query_id.as_str());
    assert!(inline.next_table().await?.is_none());

    let mut empty = session
        .query("SELECT 1 AS ID, 'hello' AS NAME LIMIT 0")
        .await?;
    let empty_query_id = empty.query_id().to_string();
    let empty_columns = empty.schema().columns();
    assert_eq!(empty_columns.len(), 2);
    assert_eq!(empty_columns[0].name(), "ID");
    assert!(matches!(empty_columns[0].ty(), ColumnType::Fixed { .. }));
    assert_eq!(empty_columns[1].name(), "NAME");
    assert!(matches!(empty_columns[1].ty(), ColumnType::Text { .. }));
    assert!(empty.is_exhausted());
    if let Some(table) = empty.next_table().await? {
        assert_eq!(table.query_id(), empty_query_id.as_str());
    }

    let query = r#"
        SELECT
            SEQ8() AS SEQ,
            RANDSTR(1000, RANDOM()) AS PAD
        FROM
            TABLE(GENERATOR(ROWCOUNT=>200000))
    "#;
    let chunked = session.query(query).await?;
    let chunked_query_id = chunked.query_id().to_string();
    let collected = chunked.collect_table().await?;
    assert_eq!(collected.row_count(), 200_000);
    assert_eq!(collected.query_id(), chunked_query_id.as_str());

    Ok(())
}
