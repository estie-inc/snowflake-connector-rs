use snowflake_connector_rs::FromRow;

struct __UserPlan;

#[derive(FromRow)]
struct User {
    id: i64,
}

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = __UserPlan;
    let _ = User { id: 1 };
    assert_from_row::<User>();
}
