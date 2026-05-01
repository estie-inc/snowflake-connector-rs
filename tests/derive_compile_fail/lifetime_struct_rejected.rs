use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad<'a> {
    value: &'a str,
}

fn main() {}
