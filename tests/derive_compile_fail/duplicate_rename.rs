use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {
    #[snowflake(rename = "A", rename = "B")]
    x: i64,
}

fn main() {}
