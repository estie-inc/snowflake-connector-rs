use reqwest::header::{ACCEPT, AUTHORIZATION};

use crate::{types::SnowflakeResponse, Error, Result};

pub struct SnowflakeSession {
    pub(crate) http: reqwest::Client,
    pub(crate) account: String,
    pub(crate) session_token: String,
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<QueryResponse> {
        let request_id = uuid::Uuid::new_v4();
        let url = format!(
            r"https://{account}.snowflakecomputing.com/queries/v1/query-request?requestId={request_id}",
            account = self.account
        );

        let request: QueryRequest = request.into();
        let response = self
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, self.session_token),
            )
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(Error::Communication(body));
        }
        let response: SnowflakeResponse<QueryResponse> =
            serde_json::from_str(&body).map_err(|_| Error::Communication(body))?;
        if !response.success {
            return Err(Error::Communication(response.message.unwrap_or_default()));
        }

        Ok(response.data)
    }
}

impl Into<QueryRequest> for &QueryRequest {
    fn into(self) -> QueryRequest {
        self.clone()
    }
}
impl Into<QueryRequest> for &str {
    fn into(self) -> QueryRequest {
        QueryRequest {
            sql_text: self.to_string(),
        }
    }
}

#[derive(Debug, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub sql_text: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub parameters: Vec<QueryResponseParameter>,
    pub query_id: String,
    pub returned: i64,
    pub total: i64,

    #[serde(rename = "rowset")]
    pub row_set: Vec<Vec<Option<String>>>,

    #[serde(rename = "rowtype")]
    pub row_types: Vec<QueryResponseRowType>,
}
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponseRowType {
    pub database: String,
    pub name: String,
    pub nullable: bool,
    pub schema: String,
    pub table: String,

    #[serde(rename = "type")]
    pub data_type: String,
}
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponseParameter {
    pub name: String,
    pub value: serde_json::Value,
}
