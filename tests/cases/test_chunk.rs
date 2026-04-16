use std::num::NonZeroUsize;

use super::common;

use snowflake_connector_rs::{CollectOptions, Result};

/// Verify that chunks are yielded in the order Snowflake returns them,
/// so that ORDER BY is preserved across chunk boundaries.
/// Uses RANDSTR to pad each row, making the result large enough to span
/// multiple S3 chunks (the threshold is ~48 MB compressed).
#[tokio::test]
async fn test_chunk_order_preserved_with_order_by() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS PAD
    FROM
        TABLE(GENERATOR(ROWCOUNT=>200000))
    ORDER BY
        SEQ
    "#;
    let mut result = session.execute(query).await?;

    let mut expected_seq = 0u64;
    let mut chunk_count = 0u64;
    while let Some(rows) = result.next_batch().await? {
        chunk_count += 1;
        for row in &rows {
            let seq = row.get::<u64>("SEQ")?;
            assert_eq!(
                seq, expected_seq,
                "expected SEQ {expected_seq} but got {seq} (duplicate or gap)"
            );
            expected_seq += 1;
        }
    }
    assert_eq!(expected_seq, 200_000);
    assert!(
        chunk_count >= 3,
        "expected multiple chunks but got {chunk_count}; \
         increase ROWCOUNT or PAD size to trigger chunking"
    );
    Ok(())
}

/// Same verification using `query()` (which calls `collect_all` internally).
#[tokio::test]
async fn test_chunk_order_preserved_with_query() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS PAD
    FROM
        TABLE(GENERATOR(ROWCOUNT=>200000))
    ORDER BY
        SEQ
    "#;
    let rows = session.query(query).await?;

    assert_eq!(rows.len(), 200_000);
    for (i, row) in rows.iter().enumerate() {
        let seq = row.get::<u64>("SEQ")?;
        assert_eq!(
            seq, i as u64,
            "expected SEQ {i} but got {seq} (duplicate or gap)"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_download_chunked_results() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    let query = r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS RAND
    FROM
        TABLE(GENERATOR(ROWCOUNT=>10000))
    "#;
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());
    assert!(rows[0].column_names().contains(&"SEQ"));
    assert!(rows[0].column_names().contains(&"RAND"));

    let columns = rows[0].columns();
    assert_eq!(
        columns[0]
            .column_type()
            .snowflake_type()
            .to_ascii_uppercase(),
        "FIXED"
    );
    assert!(!columns[0].column_type().nullable());
    assert_eq!(columns[0].index(), 0);
    assert_eq!(
        columns[1]
            .column_type()
            .snowflake_type()
            .to_ascii_uppercase(),
        "TEXT"
    );
    assert!(!columns[1].column_type().nullable());
    assert_eq!(columns[1].index(), 1);

    Ok(())
}

/// After consuming one or more batches via `next_batch()`, `collect_all()`
/// must return only the remaining rows — no duplicates, no gaps.
/// Uses 200 000 rows with 1 000-char padding to guarantee multiple S3 chunks.
#[tokio::test]
async fn test_partial_next_batch_then_collect_all() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS PAD
    FROM
        TABLE(GENERATOR(ROWCOUNT=>200000))
    ORDER BY
        SEQ
    "#;
    let mut result = session.execute(query).await?;

    let first_batch = result
        .next_batch()
        .await?
        .expect("should have at least one batch");
    let first_batch_len = first_batch.len();
    assert!(first_batch_len > 0);
    assert!(
        !result.is_exhausted(),
        "expected remote partitions remaining after first batch"
    );

    // Verify first batch is an exact prefix 0..first_batch_len
    for (i, row) in first_batch.iter().enumerate() {
        let seq = row.get::<u64>("SEQ")?;
        assert_eq!(seq, i as u64, "first_batch: expected SEQ {i} but got {seq}");
    }

    let remaining = result.collect_all().await?;
    assert_eq!(first_batch_len + remaining.len(), 200_000);

    // Verify remaining continues exactly where first_batch left off
    for (i, row) in remaining.iter().enumerate() {
        let expected = (first_batch_len + i) as u64;
        let seq = row.get::<u64>("SEQ")?;
        assert_eq!(
            seq, expected,
            "remaining: expected SEQ {expected} but got {seq}"
        );
    }

    Ok(())
}

/// `collect_all_with_options` with a custom concurrency override must
/// preserve partition order and remaining-only semantics.
/// Uses 200 000 rows with 1 000-char padding to guarantee multiple S3 chunks.
#[tokio::test]
async fn test_partial_next_batch_then_collect_all_with_options() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS PAD
    FROM
        TABLE(GENERATOR(ROWCOUNT=>200000))
    ORDER BY
        SEQ
    "#;
    let mut result = session.execute(query).await?;

    let first_batch = result
        .next_batch()
        .await?
        .expect("should have at least one batch");
    let first_batch_len = first_batch.len();
    assert!(
        !result.is_exhausted(),
        "expected remote partitions remaining after first batch"
    );

    for (i, row) in first_batch.iter().enumerate() {
        let seq = row.get::<u64>("SEQ")?;
        assert_eq!(seq, i as u64, "first_batch: expected SEQ {i} but got {seq}");
    }

    let options =
        CollectOptions::default().with_prefetch_concurrency(NonZeroUsize::new(2).unwrap());
    let remaining = result.collect_all_with_options(options).await?;

    assert_eq!(first_batch_len + remaining.len(), 200_000);

    for (i, row) in remaining.iter().enumerate() {
        let expected = (first_batch_len + i) as u64;
        let seq = row.get::<u64>("SEQ")?;
        assert_eq!(
            seq, expected,
            "remaining: expected SEQ {expected} but got {seq}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_result_set_streaming() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    let query = r#"
    SELECT
        SEQ8() AS SEQ,
        RANDSTR(1000, RANDOM()) AS RAND
    FROM
        TABLE(GENERATOR(ROWCOUNT=>10000))
    "#;

    let mut result = session.execute(query).await?;
    let mut rows = Vec::with_capacity(10000);
    while let Some(mut r) = result.next_batch().await? {
        rows.append(&mut r);
    }

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());
    assert!(rows[0].column_names().contains(&"SEQ"));
    assert!(rows[0].column_names().contains(&"RAND"));

    Ok(())
}
