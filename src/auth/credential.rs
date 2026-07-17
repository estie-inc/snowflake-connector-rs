#[cfg(feature = "key-pair-auth")]
use chrono::Utc;

use crate::{
    AuthConfig, Result,
    auth::{
        client::AuthApiClient,
        config::{AuthConfigKind, PasscodeMode},
        wire::{LoginCredentialWire, PasscodeWire},
    },
};

#[cfg(feature = "external-browser-sso")]
use crate::auth::external_browser::acquire_external_browser_credential;
#[cfg(feature = "key-pair-auth")]
use crate::auth::key_pair::generate_jwt_from_key_pair;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedLoginCredential {
    Password {
        password: String,
        passcode: Option<PreparedPasscode>,
    },
    #[cfg(feature = "key-pair-auth")]
    SnowflakeJwt(String),
    OAuth(String),
    #[cfg(feature = "external-browser-sso")]
    ExternalBrowser {
        token: String,
        proof_key: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedPasscode {
    Separate(String),
    InPassword,
}

impl PreparedLoginCredential {
    pub(crate) fn as_wire(&self) -> LoginCredentialWire<'_> {
        match self {
            Self::Password { password, passcode } => LoginCredentialWire::Password {
                password,
                passcode: passcode.as_ref().map(|passcode| match passcode {
                    PreparedPasscode::Separate(code) => PasscodeWire::Separate(code),
                    PreparedPasscode::InPassword => PasscodeWire::InPassword,
                }),
            },
            #[cfg(feature = "key-pair-auth")]
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
        if matches!(
            self,
            Self::Password {
                passcode: Some(_),
                ..
            }
        ) {
            return true;
        }
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

impl LoginCredentialProvider for AuthConfig {
    async fn prepare<'a>(
        &'a self,
        _client: &'a AuthApiClient,
        _context: LoginContext<'a>,
    ) -> Result<PreparedLoginCredential> {
        match self.kind() {
            AuthConfigKind::Password(config) => Ok(PreparedLoginCredential::Password {
                password: config.password().to_owned(),
                passcode: config.passcode().map(|passcode| match passcode {
                    PasscodeMode::Separate(code) => PreparedPasscode::Separate(code.clone()),
                    PasscodeMode::InPassword => PreparedPasscode::InPassword,
                }),
            }),
            #[cfg(feature = "key-pair-auth")]
            AuthConfigKind::KeyPair(config) => prepare_jwt_credential(
                config.pem(),
                config.password(),
                _context.username,
                _context.account,
                Utc::now().timestamp(),
            ),
            AuthConfigKind::OAuth(config) => {
                Ok(PreparedLoginCredential::OAuth(config.token().to_owned()))
            }
            #[cfg(feature = "external-browser-sso")]
            AuthConfigKind::ExternalBrowser(config) => {
                let credential =
                    acquire_external_browser_credential(_client, _context, config).await?;
                Ok(PreparedLoginCredential::ExternalBrowser {
                    token: credential.token,
                    proof_key: credential.proof_key,
                })
            }
        }
    }
}

#[cfg(feature = "key-pair-auth")]
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

    use crate::{ClientShared, PasswordConfig};

    fn dummy_client() -> AuthApiClient {
        AuthApiClient::new(ClientShared::for_test(
            Url::parse("https://example.com/").unwrap(),
        ))
    }

    #[tokio::test]
    async fn password_auth_is_prepared_as_password_credential() {
        let auth = AuthConfig::password("secret");
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
            PreparedLoginCredential::Password {
                password: "secret".to_string(),
                passcode: None,
            }
        );
    }

    #[tokio::test]
    async fn password_auth_with_passcode_is_prepared_with_separate_passcode() {
        let auth = AuthConfig::password(PasswordConfig::new("secret").with_passcode("123456"));
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
            PreparedLoginCredential::Password {
                password: "secret".to_string(),
                passcode: Some(PreparedPasscode::Separate("123456".to_string())),
            }
        );
    }

    #[tokio::test]
    async fn password_auth_with_passcode_in_password_is_prepared_accordingly() {
        let auth =
            AuthConfig::password(PasswordConfig::new("secret123456").with_passcode_in_password());
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
            PreparedLoginCredential::Password {
                password: "secret123456".to_string(),
                passcode: Some(PreparedPasscode::InPassword),
            }
        );
    }

    #[tokio::test]
    async fn oauth_auth_is_prepared_as_oauth_credential() {
        let auth = AuthConfig::oauth("oauth-token");
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

    #[cfg(feature = "key-pair-auth")]
    mod key_pair {
        use crate::KeyPairConfig;

        use super::*;

        const ENCRYPTED_TEST_PEM: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/test_snowflake_key.p8"
        ));
        const UNENCRYPTED_TEST_PEM: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/test_snowflake_key_unencrypted.p8"
        ));

        async fn assert_key_pair_auth_prepares_jwt(auth: AuthConfig) {
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

            match credential {
                PreparedLoginCredential::SnowflakeJwt(token) => {
                    assert!(!token.is_empty());
                    assert_eq!(token.split('.').count(), 3);
                }
                other => panic!("expected SnowflakeJwt credential, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn encrypted_key_pair_auth_is_prepared_as_jwt_credential() {
            let auth = AuthConfig::key_pair(KeyPairConfig::from_encrypted_pem(
                ENCRYPTED_TEST_PEM,
                b"12345".to_vec(),
            ));

            assert_key_pair_auth_prepares_jwt(auth).await;
        }

        #[tokio::test]
        async fn unencrypted_key_pair_auth_is_prepared_as_jwt_credential() {
            let auth = AuthConfig::key_pair(KeyPairConfig::from_pem(UNENCRYPTED_TEST_PEM));

            assert_key_pair_auth_prepares_jwt(auth).await;
        }

        #[test]
        fn key_pair_jwt_generation_is_deterministic_for_tests() {
            let credential = prepare_jwt_credential(
                ENCRYPTED_TEST_PEM,
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
}
