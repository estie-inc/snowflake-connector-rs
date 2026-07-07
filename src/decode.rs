//! Traits and helpers for decoding query results into Rust types.
//!
//! Most callers decode with `#[derive(FromRow)]` or [`query_as`](crate::Session::query_as) and never touch this module.
//! For hand-written decoders, implement [`FromCell`] for a single column or [`FromRow`] for a whole row.
//! [`Json`] and [`Vector`] are the wrappers for decoding semi-structured and `VECTOR` columns.
//!
//! Hand-written decoders report their own validation failures with one of three errors. The connector attaches
//! position context where it can, so implementations provide only the reason and optional source:
//!
//! - [`CustomPlanError`] — a plan-time failure from `build_plan`, before any row is read.
//! - [`RowConversionError`] — a row-level failure from [`FromRow::from_row_with_plan`], after cells decode successfully.
//! - [`CellConversionError`] — a cell-local failure from [`FromCell::from_cell_with_plan`].
//!
//! See `examples/hand_written_decode.rs` for fuller hand-written [`FromCell`] and [`FromRow`] implementations that
//! exercise these errors end to end.
//!
//! # Example
//!
//! A newtype over `f64` reuses `f64`'s plan, inheriting its column-type validation:
//!
//! ```
//! use snowflake_connector_rs::{
//!     CellPlanContext, FromCell,
//!     decode::{CellDecodeResult, PlanBuildResult},
//! };
//!
//! struct CelsiusTemp(f64);
//!
//! impl FromCell for CelsiusTemp {
//!     type Plan = <f64 as FromCell>::Plan;
//!
//!     fn build_plan(ctx: CellPlanContext<'_>) -> PlanBuildResult<Self::Plan> {
//!         f64::build_plan(ctx)
//!     }
//!
//!     fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
//!         f64::from_cell_with_plan(raw, plan).map(CelsiusTemp)
//!     }
//! }
//! ```

pub use crate::error::{
    CellConversionError, CellConversionErrorBuilder, CellDecodeResult, CustomPlanError,
    CustomPlanErrorBuilder, PlanBuildError, PlanBuildResult, RowConversionError,
    RowConversionErrorBuilder, RowDecodeError, RowDecodeResult,
};
pub use crate::result_table::{
    CellPlan, CellPlanContext, FromCell, FromRow, Json, RowPlanContext, TimePlan, TimestampPlan,
    UtcTimestampPlan, Vector,
};
