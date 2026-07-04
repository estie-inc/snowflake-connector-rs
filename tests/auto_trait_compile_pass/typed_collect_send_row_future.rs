// Pins that typed futures are `Send` for `T: Send`: both the table-producing futures and the streaming collect futures.

use snowflake_connector_rs::{CollectOptions, FromRow, TypedResultCursor};

fn typed_cursor<T: FromRow>() -> TypedResultCursor<T> {
    unreachable!()
}

fn assert_send<T: Send>(_: T) {}

type SendRow = (String,);

#[allow(dead_code)]
fn checks() {
    // Table-producing futures are Send when `T::Plan: Send + Sync`.
    assert_send(typed_cursor::<SendRow>().collect_table());
    assert_send(typed_cursor::<SendRow>().collect_table_with_options(CollectOptions::default()));

    // Streaming collect futures are Send when `T: Send` (the accumulated `Vec<T>` crosses awaits).
    assert_send(typed_cursor::<SendRow>().collect::<Vec<SendRow>>());
    assert_send(
        typed_cursor::<SendRow>().collect_with_options::<Vec<SendRow>>(CollectOptions::default()),
    );
}

fn main() {}
