use std::time::{Duration, Instant};

use http::header::{ACCEPT, AUTHORIZATION};
use reqwest::{Client, Url};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{Error, Result, query::QueryRequest};

use super::response::{
    QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE, SnowflakeResponse, parse_response,
};

pub(crate) struct StatementApiClient {
    http: Client,
    base_url: Url,
    session_token: String,
}

impl StatementApiClient {
    pub(crate) fn new(http: Client, base_url: Url, session_token: String) -> Self {
        Self {
            http,
            base_url,
            session_token,
        }
    }

    pub(crate) fn http_client(&self) -> Client {
        self.http.clone()
    }

    pub(crate) async fn submit(&self, request: &QueryRequest) -> Result<SnowflakeResponse> {
        let request_id = Uuid::new_v4();
        let mut url = self.base_url.join("queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &request_id.to_string());

        let response = self
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, self.session_token),
            )
            .json(request)
            .send()
            .await?;

        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&body[..]).into_owned();
            return Err(Error::Communication(format!("HTTP {status}: {body_text}")));
        }

        parse_response(body)
    }

    pub(crate) async fn poll_async_results(
        &self,
        poll_relative_url: &str,
        timeout: Duration,
    ) -> Result<SnowflakeResponse> {
        const POLL_INTERVAL: Duration = Duration::from_secs(10);

        let poll_url = self.base_url.join(poll_relative_url)?;
        let deadline = Instant::now() + timeout;

        loop {
            let resp = self
                .http
                .get(poll_url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(
                    AUTHORIZATION,
                    format!(r#"Snowflake Token="{}""#, self.session_token),
                )
                .send()
                .await?;

            let status = resp.status();
            let body = resp.bytes().await?;
            if !status.is_success() {
                let body_text = String::from_utf8_lossy(&body[..]).into_owned();
                return Err(Error::Communication(format!("HTTP {status}: {body_text}")));
            }

            let response = parse_response(body)?;
            if response.code.as_deref() != Some(QUERY_IN_PROGRESS_ASYNC_CODE)
                && response.code.as_deref() != Some(QUERY_IN_PROGRESS_CODE)
            {
                return Ok(response);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::TimedOut);
            }
            sleep(remaining.min(POLL_INTERVAL)).await;
        }
    }
}
