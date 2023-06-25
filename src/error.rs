#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP client error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Communication error: {0}")]
    Communication(String),
}

pub type Result<T> = std::result::Result<T, Error>;
