extern crate snowflake_connector_rs as renamed;

#[derive(renamed::FromRow)]
#[snowflake(crate = "::renamed")]
pub struct PubAliased {
    pub id: i64,
}

fn assert_from_row<T: renamed::FromRow>() {}

fn main() {
    let _ = PubAliased { id: 1 };
    assert_from_row::<PubAliased>();
}
