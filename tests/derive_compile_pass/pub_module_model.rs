pub mod entity {
    use snowflake_connector_rs::FromRow;

    #[derive(FromRow)]
    pub struct Model {
        pub id: i64,
        pub name: String,
    }
}

pub use entity::Model;

fn assert_from_row<T: snowflake_connector_rs::FromRow>() {}

fn main() {
    let _ = Model {
        id: 1,
        name: "alice".to_string(),
    };
    assert_from_row::<Model>();
}
