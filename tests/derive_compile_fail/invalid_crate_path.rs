use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(crate = "not a valid path")]
struct Bad {
    id: i64,
}

fn main() {}
