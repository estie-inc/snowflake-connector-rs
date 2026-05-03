use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(rename_all = "snake_case")]
struct Bad {
    value: i64,
}

fn main() {}
