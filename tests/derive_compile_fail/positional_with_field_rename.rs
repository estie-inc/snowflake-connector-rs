use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
#[snowflake(positional)]
struct Bad {
    #[snowflake(rename = "ID")]
    id: i64,
}

fn main() {}
