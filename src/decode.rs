//! Traits and helpers for decoding query results into Rust types.
//!
//! Most callers decode with `#[derive(FromRow)]` or [`query_as`](crate::Session::query_as) and never touch this module.
//! Reach here to hand-write a decoder: implement [`FromCell`] for a single column or [`FromRow`] for a whole row,
//! reporting cell-local failures with [`CellConversionError`].
//! [`Json`] and [`Vector`] are the wrappers for decoding semi-structured and `VECTOR` columns.
//!
//! # Example
//!
//! A newtype over `f64` reuses `f64`'s plan, inheriting its column-type validation:
//!
//! ```
//! use snowflake_connector_rs::{decode::CellDecodeResult, CellPlanContext, FromCell, Result};
//!
//! struct CelsiusTemp(f64);
//!
//! impl FromCell for CelsiusTemp {
//!     type Plan = <f64 as FromCell>::Plan;
//!
//!     fn build_plan(ctx: CellPlanContext<'_>) -> Result<Self::Plan> {
//!         f64::build_plan(ctx)
//!     }
//!
//!     fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
//!         f64::from_cell_with_plan(raw, plan).map(CelsiusTemp)
//!     }
//! }
//! ```

pub use crate::error::{CellConversionError, CellConversionErrorBuilder, CellDecodeResult};
pub use crate::result_table::{
    CellPlan, CellPlanContext, FromCell, FromRow, Json, RowPlanContext, TimePlan, TimestampPlan,
    UtcTimestampPlan, Vector,
};
