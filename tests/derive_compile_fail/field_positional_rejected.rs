use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {
    #[snowflake(positional)]
    id: i64,
}

fn main() {}
