// Pins that the typed streaming collect future is NOT `Send` for `T: !Send`.

use std::rc::Rc;

use snowflake_connector_rs::{
    FromRow, RowPlanContext, RowRef, TypedResultCursor,
    decode::{PlanBuildResult, RowDecodeResult},
};

fn typed_cursor<T: FromRow>() -> TypedResultCursor<T> {
    unreachable!()
}

fn assert_send<T: Send>(_: T) {}

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

fn main() {
    assert_send(typed_cursor::<LocalRow>().collect::<Vec<LocalRow>>());
}
