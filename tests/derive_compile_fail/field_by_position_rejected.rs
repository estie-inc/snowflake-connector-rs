use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {
    #[snowflake(by_position)]
    id: i64,
}

fn main() {}
