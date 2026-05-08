//! Types passed to [`Statement::bind`](crate::Statement::bind) and
//! [`Statement::bind_named`](crate::Statement::bind_named).
//!
//! Most callers reach for one of the typed wrappers.
//! [`RawBind`] is the escape hatch when none of those fit (e.g. binding a `DECFLOAT`).
//!
//! # Example
//!
//! ```
//! use snowflake_connector_rs::{
//!     Statement,
//!     bind::{Binary, Integer, RawBind, SnowflakeBindType},
//! };
//!
//! let stmt = Statement::new("INSERT INTO t (id, payload, big_num) SELECT ?, ?, ?")
//!     .bind(1_i64)
//!     .bind(Binary::new(b"hello".as_slice()))
//!     .bind(Integer::try_from(1_000_000_000_000_000_i128).unwrap());
//!
//! // Bind a DECFLOAT via the escape hatch.
//! let _ = Statement::new("SELECT ?")
//!     .bind(RawBind::new(SnowflakeBindType::DecFloat, "1.23e-40"));
//! # let _ = stmt;
//! ```

pub use crate::statement::bind::{
    Binary, BindName, Integer, IntoBind, IntoBindNullable, SnowflakeBindType, Time, TimestampLtz,
    TimestampNtz, TimestampTz,
};
pub use crate::statement::raw::RawBind;
