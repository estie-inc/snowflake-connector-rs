use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(positional, rename_all = "SCREAMING_SNAKE_CASE")]
struct Bad(i64, String);

fn main() {}
