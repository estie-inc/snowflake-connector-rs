use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(by_position, rename_all = "none")]
struct Bad(i64, String);

fn main() {}
