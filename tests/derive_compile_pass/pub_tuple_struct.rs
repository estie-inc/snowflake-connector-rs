use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
pub struct PubTuple(pub i64, pub String);

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = PubTuple(1, "alice".to_string());
    assert_from_row::<PubTuple>();
}
