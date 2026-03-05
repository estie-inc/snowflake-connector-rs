mod common;

use snowflake_connector_rs::{Binding, QueryRequest, Result};

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
