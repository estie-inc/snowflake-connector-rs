use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(rename_all = "SCREAMING_SNAKE_CASE", rename_all = "SCREAMING_SNAKE_CASE")]
struct Bad {
    x: i64,
}

fn main() {}
