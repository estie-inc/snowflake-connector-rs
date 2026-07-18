// Pins that public non-generic and untyped async futures are `Send`, plus that representative `Session` query futures are `Send`.

use std::rc::Rc;

use snowflake_connector_rs::{
    Client, CollectOptions, DynamicRow, FromRow, QueryCanceller, QueryHandle, ResultCursor,
    RowPlanContext, RowRef, Session,
    decode::{PlanBuildResult, RowDecodeResult},
};

fn client() -> Client {
    unreachable!()
}

fn session() -> Session {
    unreachable!()
}

fn result_cursor() -> ResultCursor {
    unreachable!()
}

fn query_handle() -> QueryHandle {
    unreachable!()
}

fn query_canceller() -> QueryCanceller {
    unreachable!()
}

fn assert_send<T: Send>(_: T) {}

// `Send` row type: only `T::Plan` matters for the typed table/plan APIs.
type SendRow = (String,);

// `!Send` row type: `T` body holds an `Rc`, but `T::Plan` stays `Send + Sync`.
struct LocalRow(Rc<()>);

impl FromRow for LocalRow {
    type Plan = ();

    fn build_plan(_: RowPlanContext<'_>) -> PlanBuildResult<Self::Plan> {
        Ok(())
    }

    fn from_row_with_plan(_: RowRef<'_>, _: &Self::Plan) -> RowDecodeResult<Self> {
        Ok(Self(Rc::new(())))
    }
}

#[allow(dead_code)]
fn checks() {
    // Public non-generic async futures are Send.
    let client = client();
    assert_send(client.create_session());
    assert_send(result_cursor().next_table());
    assert_send(result_cursor().collect_table());
    assert_send(result_cursor().collect_table_with_options(CollectOptions::default()));

    // Public untyped collect futures are Send.
    assert_send(result_cursor().collect::<Vec<DynamicRow>>());
    assert_send(result_cursor().collect_with_options::<Vec<DynamicRow>>(CollectOptions::default()));

    // Active query-cancel futures are Send, and their owning handles meet the public auto-trait contract.
    assert_send(query_handle().execute());
    assert_send(query_handle().execute_as::<SendRow>());
    assert_send(query_canceller().cancel());

    // Session query futures are Send for representative statements.
    let session = session();
    assert_send(session.query("SELECT 1"));
    assert_send(session.query_as::<SendRow, _>("SELECT 1"));

    // `query_as` with a `!Send` row type is allowed: it does not build a `T`.
    let fut = session.query_as::<LocalRow, _>("SELECT 1");
    drop(fut);
}

fn main() {}
