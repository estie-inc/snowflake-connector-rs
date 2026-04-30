use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {
    #[snowflake(unknown = "value")]
    value: i64,
}

fn main() {}
