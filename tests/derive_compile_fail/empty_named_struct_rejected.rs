use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {}

fn main() {}
