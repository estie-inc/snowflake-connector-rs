use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
pub struct PubRow {
    pub value: i64,
}

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = PubRow { value: 1 };
    assert_from_row::<PubRow>();
}
