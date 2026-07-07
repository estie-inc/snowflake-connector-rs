// Pins that typed table-producing futures allow `!Send` row types, and that `collect` allows `!Send` row types for future creation.

use std::rc::Rc;

use snowflake_connector_rs::{
    CollectOptions, FromRow, RowPlanContext, RowRef, TypedResultCursor,
    decode::{PlanBuildResult, RowDecodeResult},
};

fn typed_cursor<T: FromRow>() -> TypedResultCursor<T> {
    unreachable!()
}

// `!Send` row type whose `Plan` is `Send + Sync`.
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
    // Table-producing futures allow `!Send` row types: they carry only a decode plan, never a `T`.
    let fut = typed_cursor::<LocalRow>().collect_table();
    drop(fut);

    let fut = typed_cursor::<LocalRow>().collect_table_with_options(CollectOptions::default());
    drop(fut);

    let mut cursor = typed_cursor::<LocalRow>();
    let fut = cursor.next_table();
    drop(fut);

    // `collect` allows `!Send` row types when the future is only created: adding `where T: Send` would break these calls.
    let fut = typed_cursor::<LocalRow>().collect::<Vec<LocalRow>>();
    drop(fut);

    let fut =
        typed_cursor::<LocalRow>().collect_with_options::<Vec<LocalRow>>(CollectOptions::default());
    drop(fut);
}

fn main() {}
