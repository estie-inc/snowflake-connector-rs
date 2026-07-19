//! # Snowflake Connector
//!
//! A Rust client for Snowflake.
//!
//! ```rust,no_run
//! # use snowflake_connector_rs::{
//! #     Result, AuthConfig, Client, ClientConfig,
//! # };
//! # async fn run() -> Result<()> {
//! let client = Client::new(ClientConfig::new(
//!     "USERNAME",
//!     "ACCOUNT",
//!     AuthConfig::password("PASSWORD"),
//! ))?;
//! let session = client.create_session().await?;
//!
//! // Collect dynamic or typed rows in one shot.
//! let dynamic_rows = session
//!     .query("SELECT 1 AS id, 'hi' AS name")
//!     .await?
//!     .collect::<Vec<_>>()
//!     .await?;
//! assert_eq!(dynamic_rows.len(), 1);
//!
//! let typed_rows = session
//!     .query_as("SELECT 1 AS id, 'hi' AS name")
//!     .await?
//!     .collect::<Vec<(i64, String)>>()
//!     .await?;
//! assert_eq!(typed_rows, vec![(1, "hi".to_string())]);
//!
//! // Collect materialized storage as a ResultTable or TypedResultTable.
//! let table = session
//!     .query("SELECT 1 AS id")
//!     .await?
//!     .collect_table()
//!     .await?;
//! assert_eq!(table.row_count(), 1);
//!
//! let typed_table = session
//!     .query_as::<(i64,), _>("SELECT 1 AS id")
//!     .await?
//!     .collect_table()
//!     .await?;
//! assert_eq!(typed_table.row_count(), 1);
//!
//! // Or stream typed partitions for large results.
//! let mut result = session
//!     .query_as::<(i64, String), _>("SELECT 1 AS id, 'hi' AS name")
//!     .await?;
//! while let Some(table) = result.next_table().await? {
//!     for row in table.rows() {
//!         let (id, name) = row?;
//!         println!("{id} {name}");
//!     }
//! }
//!
//! // Keep the untyped streaming path when you only need materialized storage.
//! let result = session.query("SELECT 1 AS id").await?;
//! assert_eq!(result.schema().len(), 1);
//! # Ok(())
//! # }
//! ```

mod auth;
pub mod bind;
mod client;
mod config;
pub mod decode;
pub mod error;
mod result_cursor;
mod result_table;
mod rowset;
mod runtime;
mod session;
mod statement;

pub use auth::config::{AuthConfig, PasswordConfig};
pub use client::Client;
pub use config::{
    ClientConfig, EndpointConfig, ProxyConfig, QueryConfig, SessionConfig, TransportConfig,
};
pub use decode::{CellPlan, CellPlanContext, FromCell, FromRow, RowPlanContext};
pub use error::{Error, ErrorKind, Result};
pub use result_cursor::{CollectOptions, ResultCursor, TypedResultCursor};
pub use result_table::{
    BinaryValue, CellRef, CellValue, Column, ColumnType, DecimalValue, DynamicRow, ResultTable,
    RowRef, Rows, Schema, TypedResultTable, VectorValue,
};
pub use session::{QueryOptions, Session};
pub use statement::builder::{IntoStatement, NamedBinds, PositionalBinds, Statement, UnboundBinds};
pub use statement::{QueryCancelStatus, QueryCanceller, QueryHandle};

#[cfg(feature = "external-browser-sso")]
pub use auth::external_browser::{BrowserLaunchMode, ExternalBrowserConfig};

#[cfg(feature = "key-pair-auth")]
pub use auth::config::KeyPairConfig;

#[cfg(feature = "derive")]
pub use snowflake_connector_rs_derive::FromRow;

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;

pub(crate) use client::ClientShared;
pub(crate) use config::{
    ClientLoginConfig, InitialSessionConfig, QueryExecutionPolicy, QueryExecutionSettings,
};

#[cfg(test)]
pub(crate) use client::ClientSharedPartial;
