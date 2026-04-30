extern crate snowflake_connector_rs as renamed;

#[derive(renamed::FromRow)]
#[snowflake(crate = "::renamed")]
struct User {
    id: i64,
}

fn assert_from_row<T: renamed::FromRow>() {}

fn main() {
    let _ = User { id: 1 };
    assert_from_row::<User>();
}
