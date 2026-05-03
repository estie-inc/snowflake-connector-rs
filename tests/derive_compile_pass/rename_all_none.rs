use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(rename_all = "none")]
struct LowercaseRow {
    value: i64,
}

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = LowercaseRow { value: 1 };
    assert_from_row::<LowercaseRow>();
}
