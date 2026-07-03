use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
union Bad {
    id: i64,
}

fn main() {}
