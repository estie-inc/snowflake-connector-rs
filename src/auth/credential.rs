use chrono::Utc;

use crate::{
    Result, SnowflakeAuthMethod,
    auth::{
        client::AuthApiClient, key_pair::generate_jwt_from_key_pair, wire::LoginCredentialWire,
    },
};

#[cfg(feature = "external-browser-sso")]
use crate::auth::external_browser::acquire_external_browser_credential;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedLoginCredential {
    Password(String),
    SnowflakeJwt(String),
    OAuth(String),
    #[cfg(feature = "external-browser-sso")]
    ExternalBrowser {
        token: String,
        proof_key: Option<String>,
    },
}

impl PreparedLoginCredential {
    pub(crate) fn as_wire(&self) -> LoginCredentialWire<'_> {
        match self {
            Self::Password(password) => LoginCredentialWire::Password { password },
            Self::SnowflakeJwt(token) => LoginCredentialWire::SnowflakeJwt { token },
            Self::OAuth(token) => LoginCredentialWire::OAuth { token },
            #[cfg(feature = "external-browser-sso")]
            Self::ExternalBrowser { token, proof_key } => LoginCredentialWire::ExternalBrowser {
                token,
                proof_key: proof_key.as_deref(),
            },
        }
    }

    pub(crate) fn requires_request_id(&self) -> bool {
        #[cfg(feature = "external-browser-sso")]
        if matches!(self, Self::ExternalBrowser { .. }) {
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LoginContext<'a> {
    pub(crate) username: &'a str,
    pub(crate) account: &'a str,
}

pub(crate) trait LoginCredentialProvider {
    async fn prepare<'a>(
        &'a self,
        _client: &'a AuthApiClient,
        context: LoginContext<'a>,
    ) -> Result<PreparedLoginCredential>;
}

impl LoginCredentialProvider for SnowflakeAuthMethod {
    async fn prepare<'a>(
        &'a self,
        _client: &'a AuthApiClient,
        context: LoginContext<'a>,
    ) -> Result<PreparedLoginCredential> {
        match self {
            SnowflakeAuthMethod::Password(password) => {
                Ok(PreparedLoginCredential::Password(password.clone()))
            }
            SnowflakeAuthMethod::KeyPair {
                encrypted_pem,
                password,
            } => prepare_jwt_credential(
                encrypted_pem,
                Some(password.as_slice()),
                context.username,
                context.account,
                Utc::now().timestamp(),
            ),
            SnowflakeAuthMethod::KeyPairUnencrypted { pem } => prepare_jwt_credential(
                pem,
                None,
                context.username,
                context.account,
                Utc::now().timestamp(),
            ),
            SnowflakeAuthMethod::Oauth { token } => {
                Ok(PreparedLoginCredential::OAuth(token.clone()))
            }
            #[cfg(feature = "external-browser-sso")]
            SnowflakeAuthMethod::ExternalBrowser(config) => {
                let credential =
                    acquire_external_browser_credential(_client, context, config).await?;
                Ok(PreparedLoginCredential::ExternalBrowser {
                    token: credential.token,
                    proof_key: credential.proof_key,
                })
            }
        }
    }
}

fn prepare_jwt_credential(
    pem: &str,
    password: Option<&[u8]>,
    username: &str,
    account: &str,
    timestamp: i64,
) -> Result<PreparedLoginCredential> {
    let jwt = generate_jwt_from_key_pair(pem, password, username, account, timestamp)?;
    Ok(PreparedLoginCredential::SnowflakeJwt(jwt))
}

#[cfg(test)]
mod tests {
    use reqwest::Url;

    use super::*;

    fn dummy_client() -> AuthApiClient {
        AuthApiClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/").unwrap(),
        )
    }

    #[tokio::test]
    async fn password_auth_is_prepared_as_password_credential() {
        let auth = SnowflakeAuthMethod::Password("secret".to_string());
        let credential = auth
            .prepare(
                &dummy_client(),
                LoginContext {
                    username: "user",
                    account: "account",
                },
            )
            .await
            .unwrap();

        assert_eq!(
            credential,
            PreparedLoginCredential::Password("secret".to_string())
        );
    }

    #[tokio::test]
    async fn oauth_auth_is_prepared_as_oauth_credential() {
        let auth = SnowflakeAuthMethod::Oauth {
            token: "oauth-token".to_string(),
        };
        let credential = auth
            .prepare(
                &dummy_client(),
                LoginContext {
                    username: "user",
                    account: "account",
                },
            )
            .await
            .unwrap();

        assert_eq!(
            credential,
            PreparedLoginCredential::OAuth("oauth-token".to_string())
        );
    }

    #[test]
    fn key_pair_jwt_generation_is_deterministic_for_tests() {
        let encrypted_pem = include_str!("./test_snowflake_key.p8");
        let credential = prepare_jwt_credential(
            encrypted_pem,
            Some("12345".as_bytes()),
            "USER_NAME",
            "myaccount.ap-northeast-1.aws",
            1700746374,
        )
        .unwrap();

        assert_eq!(
            credential,
            PreparedLoginCredential::SnowflakeJwt(
                r#"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3MDA3NDY5NzQsImlhdCI6MTcwMDc0NjM3NCwiaXNzIjoiTVlBQ0NPVU5ULlVTRVJfTkFNRS5TSEEyNTY6S1NFV3pOaW9sbkpGcTAwNDlNV2diU0dMbFhHdjZnVHNpaGlVUmxPZTE1dz0iLCJzdWIiOiJNWUFDQ09VTlQuVVNFUl9OQU1FIn0.CymITivfHERyl_JiM49BSG_sgD0jAD7lTa1qeTMpKFpkGS7TMfOZBYuNj4FsIGxBQtob60pUiyunjKaQbtPjHLlMDQP62rW03qC68m-d4RuYZqzi7P16Go_FVYGIxaoHUsM25IWxuKBLOmsWwG7tVhT6ZHFKvMvqxOZEVIBbB7pFEIMjsAOBCjSDARxu7fhHmR6Oy64XPMr2Xw_NDm-yVPcEv3NdonyO1zMS6QiRKX4Yqzku5fXeOJWvPaUtkYdwm15jzVvV3zH5OkSw252ZiVaZBSkWmwpn7YQk8StjRBamncLiOAU7EFmSgAt6Lzi-kLv0fg4ZfMTcxfhxwBNVfN0vK6UTPcnxbjZK0n6i5JK2m6XPdiByHmhSgwCvDJ0ZLn8uGze5nU1Zdlfcg8fci5tsh-Q6BMuqvx6M21dQ_E3GF6GDcuX-_d8Ap7CUtdMmWLUYNdgnDDV3reKqdeopPuuBO5zXXEYtek1Q6iqb57bQMFcK6tg3HMnHqUxruzuyKZv0S30teC1STBKS7IrGB_etFtEQ2eF7Qea3yIoxxXAkCxUXcIWkDWyt5RzUyCpQd-MYYTiD2o_bf_XS588bGZ1zzQ9lB-9aRVWYW3gUAOVykv-IW8FnndNePGVkPiX8uhUMW1NC6VHQvEcgWY-EfxZ4eoUZxd5ldk5kxPzDEeA"#.to_string()
            )
        );
    }
}
