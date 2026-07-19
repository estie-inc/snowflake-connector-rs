use std::sync::Arc;

use uuid::Uuid;

use crate::{
    ApiContext, ClientLoginConfig, InitialSessionConfig, Result,
    auth::credential::{LoginCredentialProvider, PreparedLoginCredential},
};

use super::{
    api::AuthApiClient,
    credential::LoginContext,
    wire::{LoginBody, LoginData, LoginQuery, LoginRequest},
};

/// Login to Snowflake and return a session token.
pub(crate) async fn login(login: &ClientLoginConfig, api: Arc<ApiContext>) -> Result<String> {
    let client = AuthApiClient::new(api);
    let context = LoginContext {
        username: login.username(),
        account: login.account(),
    };
    let credential = login.auth().prepare(&client, context).await?;
    let request = build_login_request(context, login.initial_session(), &credential);

    let session = client.login(request).await?;
    Ok(session.token)
}

fn build_login_request<'a>(
    context: LoginContext<'a>,
    initial_session: &'a InitialSessionConfig,
    credential: &'a PreparedLoginCredential,
) -> LoginRequest<'a> {
    let session_parameters = initial_session.session_parameters();

    LoginRequest {
        query: LoginQuery {
            warehouse: initial_session.warehouse(),
            database_name: initial_session.database(),
            schema_name: initial_session.schema(),
            role_name: initial_session.role(),
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
    use crate::{SessionConfig, auth::credential::PreparedPasscode};

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
        let session =
            InitialSessionConfig::from(SessionConfig::default().with_warehouse("warehouse"));
        let credential = PreparedLoginCredential::Password {
            password: "secret".to_string(),
            passcode: None,
        };
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

    #[test]
    fn password_mfa_login_request_includes_request_id() {
        let session =
            InitialSessionConfig::from(SessionConfig::default().with_warehouse("warehouse"));
        let credential = PreparedLoginCredential::Password {
            password: "secret".to_string(),
            passcode: Some(PreparedPasscode::Separate("123456".to_string())),
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

    #[cfg(feature = "external-browser-sso")]
    #[test]
    fn external_browser_login_request_includes_request_id() {
        let session =
            InitialSessionConfig::from(SessionConfig::default().with_warehouse("warehouse"));
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
