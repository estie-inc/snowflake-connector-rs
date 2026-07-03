use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(crate = "")]
struct Bad {
    id: i64,
}

fn main() {}
