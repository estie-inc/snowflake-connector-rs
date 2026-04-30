use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad<T> {
    value: T,
}

fn main() {}
