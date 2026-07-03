use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(positional)]
pub struct PubPositional {
    pub id: i64,
    pub name: String,
}

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = PubPositional {
        id: 1,
        name: "alice".to_string(),
    };
    assert_from_row::<PubPositional>();
}
