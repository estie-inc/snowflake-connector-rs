use reqwest::{Client, Url};
use uuid::Uuid;

use crate::{
    Result, SnowflakeClientConfig, SnowflakeSessionConfig,
    auth::credential::{LoginCredentialProvider, PreparedLoginCredential},
};

use super::{
    client::AuthApiClient,
    credential::LoginContext,
    wire::{LoginBody, LoginData, LoginQuery, LoginRequest},
};

/// Login to Snowflake and return a session token.
pub(crate) async fn login(
    http: &Client,
    config: &SnowflakeClientConfig,
    base_url: &Url,
) -> Result<String> {
    let client = AuthApiClient::new(http.clone(), base_url.clone());
    let context = LoginContext {
        username: config.username(),
        account: config.account(),
    };
    let credential = config.auth().prepare(&client, context).await?;
    let request = build_login_request(context, config.session(), &credential);

    let session = client.login(request).await?;
    Ok(session.token)
}

fn build_login_request<'a>(
    context: LoginContext<'a>,
    session_config: &'a SnowflakeSessionConfig,
    credential: &'a PreparedLoginCredential,
) -> LoginRequest<'a> {
    let session_parameters = session_config.session_parameters();

    LoginRequest {
        query: LoginQuery {
            warehouse: session_config.warehouse(),
            database_name: session_config.database(),
            schema_name: session_config.schema(),
            role_name: session_config.role(),
            request_id: credential.requires_request_id().then(Uuid::new_v4),
        },
        body: LoginBody {
            data: LoginData {
                account_name: context.account,
                login_name: context.username,
                credential: credential.as_wire(),
                session_parameters: (!session_parameters.is_empty()).then_some(session_parameters),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Url;

    use super::*;

    fn query_string(query: &LoginQuery<'_>) -> Option<String> {
        reqwest::Client::new()
            .get(Url::parse("https://example.com").unwrap())
            .query(query)
            .build()
            .unwrap()
            .url()
            .query()
            .map(str::to_owned)
    }

    #[test]
    fn password_login_request_omits_request_id() {
        let session = SnowflakeSessionConfig::default().with_warehouse("warehouse");
        let credential = PreparedLoginCredential::Password("secret".to_string());
        let request = build_login_request(
            LoginContext {
                username: "user",
                account: "account",
            },
            &session,
            &credential,
        );

        assert_eq!(request.query.request_id, None);
        assert_eq!(
            query_string(&request.query).as_deref(),
            Some("warehouse=warehouse")
        );
    }

    #[cfg(feature = "external-browser-sso")]
    #[test]
    fn external_browser_login_request_includes_request_id() {
        let session = SnowflakeSessionConfig::default().with_warehouse("warehouse");
        let credential = PreparedLoginCredential::ExternalBrowser {
            token: "browser-token".to_string(),
            proof_key: None,
        };
        let request = build_login_request(
            LoginContext {
                username: "user",
                account: "account",
            },
            &session,
            &credential,
        );

        assert!(request.query.request_id.is_some());
        assert!(
            query_string(&request.query)
                .as_deref()
                .is_some_and(|query| query.starts_with("warehouse=warehouse&request_id="))
        );
    }
}
