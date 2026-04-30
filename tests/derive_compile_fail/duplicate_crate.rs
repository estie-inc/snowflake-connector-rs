use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(crate = "::snowflake_connector_rs", crate = "::other")]
struct Bad {
    x: i64,
}

fn main() {}
