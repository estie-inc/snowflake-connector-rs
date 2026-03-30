use super::common;

use snowflake_connector_rs::Result;

/// Verify that chunks are yielded in the order Snowflake returns them,
/// so that ORDER BY is preserved across chunk boundaries.
/// Uses RANDSTR to pad each row, making the result large enough to span
/// multiple S3 chunks (the threshold is ~48 MB compressed).
#[tokio::test]
async fn test_chunk_order_preserved_with_order_by() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS PAD \
                 FROM TABLE(GENERATOR(ROWCOUNT=>200000)) ORDER BY SEQ";
    let executor = session.execute(query).await?;

    let mut prev: Option<u64> = None;
    let mut total_rows = 0u64;
    let mut chunk_count = 0u64;
    while let Some(rows) = executor.fetch_next_chunk().await? {
        chunk_count += 1;
        for row in &rows {
            let seq = row.get::<u64>("SEQ")?;
            if let Some(p) = prev {
                assert!(
                    seq >= p,
                    "ordering broken: {p} followed by {seq} (row {total_rows})"
                );
            }
            prev = Some(seq);
            total_rows += 1;
        }
    }
    assert_eq!(total_rows, 200_000);
    assert!(
        chunk_count >= 3,
        "expected multiple chunks but got {chunk_count}; \
         increase ROWCOUNT or PAD size to trigger chunking"
    );
    Ok(())
}

/// Same verification using `query()` (which calls `fetch_all` internally).
#[tokio::test]
async fn test_chunk_order_preserved_with_query() -> Result<()> {
    let client = common::connect()?;
    let session = client.create_session().await?;

    let query = "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS PAD \
                 FROM TABLE(GENERATOR(ROWCOUNT=>200000)) ORDER BY SEQ";
    let rows = session.query(query).await?;

    assert_eq!(rows.len(), 200_000);
    for i in 1..rows.len() {
        let prev = rows[i - 1].get::<u64>("SEQ")?;
        let curr = rows[i].get::<u64>("SEQ")?;
        assert!(
            curr >= prev,
            "ordering broken at index {i}: {prev} followed by {curr}"
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
    let query = "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>10000))";
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());
    assert!(rows[0].column_names().contains(&"SEQ"));
    assert!(rows[0].column_names().contains(&"RAND"));

    let columns = rows[0].column_types();
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

#[tokio::test]
async fn test_query_executor() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    let query = "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>10000))";

    let executor = session.execute(query).await?;
    let mut rows = Vec::with_capacity(10000);
    while let Some(mut r) = executor.fetch_next_chunk().await? {
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
