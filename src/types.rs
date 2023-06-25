use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub(crate) struct SnowflakeResponse<T> {
    pub(crate) data: T,
    pub(crate) message: Option<String>,
    pub(crate) success: bool,
}
