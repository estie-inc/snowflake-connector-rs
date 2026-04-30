use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
enum Bad {
    A,
    B,
}

fn main() {}
