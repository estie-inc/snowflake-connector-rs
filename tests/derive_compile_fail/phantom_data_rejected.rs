use std::marker::PhantomData;

use snowflake_connector_rs::FromRow;

#[derive(FromRow)]
struct Bad {
    id: i64,
    marker: PhantomData<()>,
}

fn main() {}
