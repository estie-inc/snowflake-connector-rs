use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
pub(crate) struct CrateRow {
    value: i64,
}

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = CrateRow { value: 1 };
    assert_from_row::<CrateRow>();
}
