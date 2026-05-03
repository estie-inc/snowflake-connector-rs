use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(rename_all = "none")]
struct Bad(i64, String);

fn main() {}
