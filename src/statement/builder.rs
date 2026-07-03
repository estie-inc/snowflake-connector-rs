// `pub(crate)` `StatementParts` is exposed through the sealed (unnameable) `IntoStatement` supertrait; callers cannot name it,
// so this leak is intentional.
#![allow(private_interfaces)]

use std::borrow::Cow;

use indexmap::IndexMap;

use super::bind::{Bind, BindName, IntoBind, encode_bind};

/// Typestate marker for a [`Statement`] with no binds attached yet.
///
/// The first call to [`Statement::bind`] transitions to [`PositionalBinds`];
/// the first call to [`Statement::bind_named`] transitions to [`NamedBinds`].
#[derive(Debug, Clone)]
pub struct UnboundBinds;

/// Typestate marker for a [`Statement`] using positional (`?`) binds.
///
/// While in this state, only [`Statement::bind`] is callable — [`Statement::bind_named`] is rejected at compile time.
#[derive(Debug, Clone)]
pub struct PositionalBinds(Vec<Bind>);

/// Typestate marker for a [`Statement`] using named (`:name` / `:1`) binds.
///
/// While in this state, only [`Statement::bind_named`] is callable. Re-binding the same name is last-wins.
#[derive(Debug, Clone)]
pub struct NamedBinds(IndexMap<BindName, Bind>);

/// Builder for a SQL statement and its bind values, accepted by
/// [`Session::query`](crate::Session::query) / [`Session::query_as`](crate::Session::query_as).
///
/// A `Statement` is in one of three bind modes captured by the typestate
/// parameter `M`:
///
/// | Constructor | `M` | Bind method |
/// |-------------|-----|-------------|
/// | [`Statement::new`] | [`UnboundBinds`] | [`bind`](Self::bind) (→ [`PositionalBinds`]) or [`bind_named`](Self::bind_named) (→ [`NamedBinds`]) |
/// | [`Statement::new_positional`] | [`PositionalBinds`] | [`bind`](Self::bind) |
/// | [`Statement::new_named`] | [`NamedBinds`] | [`bind_named`](Self::bind_named) |
///
/// Mixing positional and named binds in one statement is a compile error.
///
/// # Examples
///
/// Positional binds:
///
/// ```
/// use snowflake_connector_rs::Statement;
///
/// let stmt = Statement::new("SELECT ?, ?")
///     .bind(1_i64)
///     .bind("hello");
/// assert_eq!(stmt.sql(), "SELECT ?, ?");
/// ```
///
/// Named binds:
///
/// ```
/// use snowflake_connector_rs::Statement;
///
/// let stmt = Statement::new("SELECT * FROM t WHERE id = :id")
///     .bind_named("id", 42_i64);
/// assert_eq!(stmt.sql(), "SELECT * FROM t WHERE id = :id");
/// ```
///
/// Mixing modes does not compile:
///
/// ```compile_fail
/// use snowflake_connector_rs::Statement;
///
/// let _ = Statement::new("SELECT ?")
///     .bind(1_i64)
///     .bind_named("id", 1_i64);
/// ```
#[derive(Debug, Clone)]
pub struct Statement<M = UnboundBinds> {
    sql: Cow<'static, str>,
    bindings: M,
}

impl Statement<UnboundBinds> {
    /// Builds a new statement from SQL text.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new("SELECT 1");
    /// assert_eq!(stmt.sql(), "SELECT 1");
    /// ```
    pub fn new(sql: impl Into<Cow<'static, str>>) -> Self {
        Self {
            sql: sql.into(),
            bindings: UnboundBinds,
        }
    }

    /// Adds a positional bind, transitioning to [`PositionalBinds`] mode.
    ///
    /// `T` is any [`IntoBind`] value — primary scalars, the typed wrappers, [`RawBind`](crate::bind::RawBind), or
    /// `Option<T>` where `T:` [`IntoBindNullable`](crate::bind::IntoBindNullable).
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new("SELECT ?, ?")
    ///     .bind(1_i64)
    ///     .bind("hello");
    /// assert_eq!(stmt.sql(), "SELECT ?, ?");
    /// ```
    pub fn bind<T>(self, value: T) -> Statement<PositionalBinds>
    where
        T: IntoBind,
    {
        Statement {
            sql: self.sql,
            bindings: PositionalBinds(vec![encode_bind(value)]),
        }
    }

    /// Adds a named bind, transitioning to [`NamedBinds`] mode.
    ///
    /// Pass the bare placeholder name (no leading `:`): `"id"` matches `:id`, `"1"` matches `:1`. The name is sent verbatim.
    /// Empty names are rejected when the statement is submitted.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new("SELECT * FROM t WHERE id = :id OR id = :1")
    ///     .bind_named("id", 1_i64)
    ///     .bind_named("1", 999_i64);
    /// assert_eq!(stmt.sql(), "SELECT * FROM t WHERE id = :id OR id = :1");
    /// ```
    pub fn bind_named<T>(self, name: impl Into<BindName>, value: T) -> Statement<NamedBinds>
    where
        T: IntoBind,
    {
        let mut bindings = IndexMap::new();
        bindings.insert(name.into(), encode_bind(value));
        Statement {
            sql: self.sql,
            bindings: NamedBinds(bindings),
        }
    }
}

impl Statement<PositionalBinds> {
    /// Builds an empty positional-mode statement.
    ///
    /// Use this when the bind mode must be fixed before the first bind (e.g. in generic helpers that return
    /// `Statement<PositionalBinds>`). Otherwise [`Statement::new`] followed by [`bind`](Self::bind) is equivalent.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new_positional("INSERT INTO t VALUES (?, ?)")
    ///     .bind(1_i64)
    ///     .bind("hello");
    /// assert_eq!(stmt.sql(), "INSERT INTO t VALUES (?, ?)");
    /// ```
    pub fn new_positional(sql: impl Into<Cow<'static, str>>) -> Self {
        Self {
            sql: sql.into(),
            bindings: PositionalBinds(Vec::new()),
        }
    }

    /// Appends a positional bind.
    ///
    /// `T` may be any [`IntoBind`] value.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new_positional("SELECT ?, ?, ?")
    ///     .bind(1_i64)
    ///     .bind("hello")
    ///     .bind(true);
    /// assert_eq!(stmt.sql(), "SELECT ?, ?, ?");
    /// ```
    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: IntoBind,
    {
        self.bindings.0.push(encode_bind(value));
        self
    }
}

impl Statement<NamedBinds> {
    /// Builds an empty named-mode statement.
    ///
    /// Use this when the bind mode must be fixed before the first bind (e.g. in generic helpers that return
    /// `Statement<NamedBinds>`). Otherwise [`Statement::new`] followed by [`bind_named`](Self::bind_named) is equivalent.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new_named("SELECT * FROM t WHERE id = :id")
    ///     .bind_named("id", 1_i64);
    /// assert_eq!(stmt.sql(), "SELECT * FROM t WHERE id = :id");
    /// ```
    pub fn new_named(sql: impl Into<Cow<'static, str>>) -> Self {
        Self {
            sql: sql.into(),
            bindings: NamedBinds(IndexMap::new()),
        }
    }

    /// Inserts a named bind, last-wins for duplicated names.
    ///
    /// `T` may be any [`IntoBind`] value. Pass the bare placeholder name (no leading `:`).
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// // Last-wins overwrite for a duplicated name.
    /// let stmt = Statement::new_named("SELECT * FROM t WHERE id = :id")
    ///     .bind_named("id", 1_i64)
    ///     .bind_named("id", 2_i64);
    /// assert_eq!(stmt.sql(), "SELECT * FROM t WHERE id = :id");
    /// ```
    pub fn bind_named<T>(mut self, name: impl Into<BindName>, value: T) -> Self
    where
        T: IntoBind,
    {
        self.bindings.0.insert(name.into(), encode_bind(value));
        self
    }
}

impl<M> Statement<M> {
    /// Returns the SQL text the statement was constructed with.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::Statement;
    ///
    /// let stmt = Statement::new("SELECT 1");
    /// assert_eq!(stmt.sql(), "SELECT 1");
    /// ```
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl From<&'static str> for Statement<UnboundBinds> {
    fn from(sql: &'static str) -> Self {
        Self::new(sql)
    }
}

impl From<String> for Statement<UnboundBinds> {
    fn from(sql: String) -> Self {
        Self::new(sql)
    }
}

/// Marker trait for values accepted by [`Session::query`](crate::Session::query) / [`Session::query_as`](crate::Session::query_as).
///
/// # Example
///
/// ```no_run
/// use snowflake_connector_rs::{
///     Result, AuthConfig, Client, ClientConfig, Statement,
/// };
/// # async fn run() -> Result<()> {
/// # let client = Client::new(ClientConfig::new(
/// #     "USER",
/// #     "ACCOUNT",
/// #     AuthConfig::password("PW"),
/// # ))?;
/// let session = client.create_session().await?;
///
/// // Bare SQL string.
/// let _ = session.query("SELECT 1").await?;
///
/// // Statement with binds.
/// let stmt = Statement::new("SELECT ?").bind(42_i64);
/// let _ = session.query(stmt).await?;
/// # Ok(())
/// # }
/// ```
pub trait IntoStatement: into_statement_sealed::Sealed {}

mod into_statement_sealed {
    pub trait Sealed {
        fn into_statement_parts(self) -> super::StatementParts;
    }
}

pub(crate) fn into_statement_parts<S>(statement: S) -> StatementParts
where
    S: IntoStatement,
{
    <S as into_statement_sealed::Sealed>::into_statement_parts(statement)
}

#[doc(hidden)]
#[derive(Debug)]
pub(crate) struct StatementParts(StatementPartsRepr);

#[derive(Debug)]
pub(crate) enum StatementPartsRepr {
    Unbound {
        sql: Cow<'static, str>,
    },
    Positional {
        sql: Cow<'static, str>,
        bindings: Vec<Bind>,
    },
    Named {
        sql: Cow<'static, str>,
        bindings: IndexMap<BindName, Bind>,
    },
}

impl StatementParts {
    pub(crate) fn sql(&self) -> &str {
        match &self.0 {
            StatementPartsRepr::Unbound { sql }
            | StatementPartsRepr::Positional { sql, .. }
            | StatementPartsRepr::Named { sql, .. } => sql,
        }
    }

    pub(crate) fn repr(&self) -> &StatementPartsRepr {
        &self.0
    }
}

impl IntoStatement for &'static str {}

impl into_statement_sealed::Sealed for &'static str {
    fn into_statement_parts(self) -> StatementParts {
        StatementParts(StatementPartsRepr::Unbound { sql: self.into() })
    }
}

impl IntoStatement for String {}

impl into_statement_sealed::Sealed for String {
    fn into_statement_parts(self) -> StatementParts {
        StatementParts(StatementPartsRepr::Unbound { sql: self.into() })
    }
}

impl IntoStatement for Statement<UnboundBinds> {}

impl into_statement_sealed::Sealed for Statement<UnboundBinds> {
    fn into_statement_parts(self) -> StatementParts {
        StatementParts(StatementPartsRepr::Unbound { sql: self.sql })
    }
}

impl IntoStatement for Statement<PositionalBinds> {}

impl into_statement_sealed::Sealed for Statement<PositionalBinds> {
    fn into_statement_parts(self) -> StatementParts {
        StatementParts(StatementPartsRepr::Positional {
            sql: self.sql,
            bindings: self.bindings.0,
        })
    }
}

impl IntoStatement for Statement<NamedBinds> {}

impl into_statement_sealed::Sealed for Statement<NamedBinds> {
    fn into_statement_parts(self) -> StatementParts {
        StatementParts(StatementPartsRepr::Named {
            sql: self.sql,
            bindings: self.bindings.0,
        })
    }
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<Statement<UnboundBinds>>();
    assert_send_sync::<Statement<PositionalBinds>>();
    assert_send_sync::<Statement<NamedBinds>>();
    assert_send_sync::<StatementParts>();
};

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::statement::bind::BindValue;

    use super::*;

    #[test]
    fn unbound_statement_has_no_bindings() {
        let statement = Statement::from("SELECT 1");
        assert_eq!(statement.sql(), "SELECT 1");

        match into_statement_parts(statement).repr() {
            StatementPartsRepr::Unbound { sql } => {
                assert!(matches!(sql, Cow::Borrowed("SELECT 1")));
            }
            _ => panic!("expected unbound statement"),
        }
    }

    #[test]
    fn positional_statement_parts_preserve_bind_order_and_typed_values() {
        let statement = Statement::new("SELECT ?, ?").bind(1_i64).bind("hi");

        match into_statement_parts(statement).repr() {
            StatementPartsRepr::Positional { sql, bindings } => {
                assert!(matches!(sql, Cow::Borrowed("SELECT ?, ?")));
                assert_eq!(bindings.len(), 2);
                assert_eq!(bindings[0].value(), Some(&BindValue::Fixed(1)));
                match bindings[1].value() {
                    Some(BindValue::Text(text)) => assert!(matches!(text, Cow::Borrowed("hi"))),
                    other => panic!("expected borrowed text bind, got {other:?}"),
                }
            }
            _ => panic!("expected positional statement"),
        }
    }

    #[test]
    fn named_statement_last_wins_for_duplicate_keys() {
        let statement = Statement::new("SELECT :id")
            .bind_named("id", 1_i64)
            .bind_named("id", 2_i64);

        match into_statement_parts(statement).repr() {
            StatementPartsRepr::Named { sql, bindings } => {
                assert!(matches!(sql, Cow::Borrowed("SELECT :id")));
                assert_eq!(bindings.len(), 1);
                let (name, bind) = bindings.iter().next().expect("one entry");
                assert_eq!(name.as_str(), "id");
                assert_eq!(bind.value(), Some(&BindValue::Fixed(2)));
            }
            _ => panic!("expected named statement"),
        }
    }

    #[test]
    fn statement_clone_allows_reuse() {
        let statement = Statement::new("SELECT ?").bind(1_i64);
        let first = into_statement_parts(statement.clone());
        let second = into_statement_parts(statement);

        for parts in [&first, &second] {
            match parts.repr() {
                StatementPartsRepr::Positional { sql, bindings } => {
                    assert_eq!(sql, "SELECT ?");
                    assert_eq!(bindings.len(), 1);
                }
                _ => panic!("expected positional statement"),
            }
        }
    }

    #[test]
    fn statement_debug_redacts_bound_values() {
        let email = "alice@example.com";
        let statement = Statement::new("SELECT ?, ?")
            .bind(email)
            .bind(crate::bind::Binary::new(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        let rendered = format!("{statement:?}");

        // The SQL text stays visible; the bound values do not.
        assert!(
            rendered.contains("SELECT ?, ?"),
            "sql should be visible: {rendered}"
        );
        assert!(!rendered.contains(email), "text bind leaked: {rendered}");
        assert!(!rendered.contains("222"), "binary bytes leaked: {rendered}");
        assert!(
            rendered.contains("<redacted>"),
            "expected redaction marker: {rendered}"
        );
    }
}
