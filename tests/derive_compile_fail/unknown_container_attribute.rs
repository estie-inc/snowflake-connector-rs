use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(unknown = "value")]
struct Bad {
    value: i64,
}

fn main() {}
