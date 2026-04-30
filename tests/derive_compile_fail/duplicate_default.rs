use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {
    #[snowflake(default, default)]
    x: Option<String>,
}

fn main() {}
