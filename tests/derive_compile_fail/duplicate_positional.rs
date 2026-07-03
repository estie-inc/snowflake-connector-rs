use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(positional, positional)]
struct Bad {
    id: i64,
}

fn main() {}
