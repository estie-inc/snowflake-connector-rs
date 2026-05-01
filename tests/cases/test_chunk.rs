use std::num::NonZeroUsize;

use snowflake_connector_rs::{CollectOptions, ColumnType, Result};

use super::common;

fn chunked_ordered_query() -> &'static str {
    r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS PAD
    FROM
        TABLE(GENERATOR(ROWCOUNT=>200000))
    ORDER BY
        SEQ
    "#
}

fn assert_rows_are_contiguous(rows: &[(u64, String)], expected_start: u64) {
    for (offset, (seq, _)) in rows.iter().enumerate() {
        let expected = expected_start + offset as u64;
        assert_eq!(
            *seq, expected,
            "expected SEQ {expected} but got {seq} (duplicate or gap)"
        );
    }
}

#[tokio::test]
async fn test_chunked_typed_streaming_preserves_order() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let mut result = session
        .query_as::<(u64, String), _>(chunked_ordered_query())
        .await?;
    let query_id = result.query_id().to_string();
    assert!(!query_id.is_empty());

    let mut expected_seq = 0u64;
    let mut chunk_count = 0u64;
    while let Some(table) = result.next_table().await? {
        chunk_count += 1;
        assert_eq!(table.query_id(), query_id.as_str());

        let rows = table.rows().collect::<Result<Vec<_>>>()?;
        assert_rows_are_contiguous(&rows, expected_seq);
        expected_seq += rows.len() as u64;
    }

    assert_eq!(expected_seq, 200_000);
    assert!(
        chunk_count >= 3,
        "expected multiple chunks but got {chunk_count}"
    );
    Ok(())
}

#[tokio::test]
async fn test_partial_typed_stream_then_collect_preserves_order() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    for use_options in [false, true] {
        let mut result = session
            .query_as::<(u64, String), _>(chunked_ordered_query())
            .await?;
        let query_id = result.query_id().to_string();

        let columns = result.schema().columns();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name(), "SEQ");
        assert!(matches!(columns[0].ty(), ColumnType::Fixed { .. }));
        assert!(!columns[0].nullable());
        assert_eq!(columns[1].name(), "PAD");
        assert!(matches!(columns[1].ty(), ColumnType::Text { .. }));
        assert!(!columns[1].nullable());

        let first_batch = result
            .next_table()
            .await?
            .expect("should have at least one batch");
        assert_eq!(first_batch.query_id(), query_id.as_str());

        let first_rows = first_batch.rows().collect::<Result<Vec<_>>>()?;
        assert!(!first_rows.is_empty());
        assert_rows_are_contiguous(&first_rows, 0);
        assert!(!result.is_exhausted());

        let first_batch_len = first_rows.len();
        let remaining = if use_options {
            let options =
                CollectOptions::default().with_prefetch_concurrency(NonZeroUsize::new(2).unwrap());
            result.collect_table_with_options(options).await?
        } else {
            result.collect_table().await?
        };
        assert_eq!(remaining.query_id(), query_id.as_str());
        assert_eq!(first_batch_len + remaining.row_count(), 200_000);

        let remaining_rows = remaining.rows().collect::<Result<Vec<_>>>()?;
        assert_rows_are_contiguous(&remaining_rows, first_batch_len as u64);
    }

    Ok(())
}
