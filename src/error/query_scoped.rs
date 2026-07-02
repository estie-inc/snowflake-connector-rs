use std::sync::Arc;

use super::{
    Error, InternalError, NetworkError, ProtocolError, RowsetParseError, ServerError,
    SessionExpiredError, TimeoutError, repr::Repr,
};

#[derive(Debug)]
pub(crate) struct QueryScopedError {
    query_id: Arc<str>,
    repr: QueryScopedRepr,
}

#[derive(Debug)]
pub(crate) enum QueryScopedRepr {
    Network(NetworkError),
    Timeout(TimeoutError),
    Protocol(ProtocolError),
    Internal(InternalError),
    Server(ServerError),
    SessionExpired(SessionExpiredError),
}

pub(crate) type QueryScopedResult<T> = std::result::Result<T, QueryScopedError>;

impl QueryScopedError {
    pub(crate) fn new(query_id: Arc<str>, inner: impl Into<QueryScopedRepr>) -> Self {
        Self {
            query_id,
            repr: inner.into(),
        }
    }

    fn into_parts(self) -> (Arc<str>, QueryScopedRepr) {
        (self.query_id, self.repr)
    }
}

impl From<QueryScopedError> for Error {
    fn from(error: QueryScopedError) -> Self {
        let (query_id, inner) = error.into_parts();
        let repr = match inner {
            QueryScopedRepr::Network(error) => Repr::Network {
                error,
                query_id: Some(query_id),
            },
            QueryScopedRepr::Timeout(error) => Repr::Timeout {
                error,
                query_id: Some(query_id),
            },
            QueryScopedRepr::Protocol(error) => Repr::Protocol {
                error,
                query_id: Some(query_id),
            },
            QueryScopedRepr::Internal(error) => Repr::Internal {
                error,
                query_id: Some(query_id),
            },
            QueryScopedRepr::Server(mut error) => {
                if error.query_id.is_none() {
                    error.query_id = Some(query_id);
                }
                Repr::Server(error)
            }
            QueryScopedRepr::SessionExpired(mut error) => {
                if error.query_id.is_none() {
                    error.query_id = Some(query_id);
                }
                Repr::SessionExpired(error)
            }
        };
        Error::new(repr)
    }
}

impl From<NetworkError> for QueryScopedRepr {
    fn from(error: NetworkError) -> Self {
        Self::Network(error)
    }
}

impl From<TimeoutError> for QueryScopedRepr {
    fn from(error: TimeoutError) -> Self {
        Self::Timeout(error)
    }
}

impl From<ProtocolError> for QueryScopedRepr {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<InternalError> for QueryScopedRepr {
    fn from(error: InternalError) -> Self {
        Self::Internal(error)
    }
}

impl From<ServerError> for QueryScopedRepr {
    fn from(error: ServerError) -> Self {
        Self::Server(error)
    }
}

impl From<SessionExpiredError> for QueryScopedRepr {
    fn from(error: SessionExpiredError) -> Self {
        Self::SessionExpired(error)
    }
}

impl From<RowsetParseError> for QueryScopedRepr {
    fn from(error: RowsetParseError) -> Self {
        Self::Protocol(ProtocolError::RowsetParse(error))
    }
}
