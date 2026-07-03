use std::collections::HashMap;

use serde::{Serialize, ser::SerializeMap as _};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct LoginRequest<'a> {
    pub(crate) query: LoginQuery<'a>,
    pub(crate) body: LoginBody<'a>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct LoginQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) warehouse: Option<&'a str>,
    #[serde(rename = "databaseName", skip_serializing_if = "Option::is_none")]
    pub(crate) database_name: Option<&'a str>,
    #[serde(rename = "schemaName", skip_serializing_if = "Option::is_none")]
    pub(crate) schema_name: Option<&'a str>,
    #[serde(rename = "roleName", skip_serializing_if = "Option::is_none")]
    pub(crate) role_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) request_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub(crate) struct LoginBody<'a> {
    pub(crate) data: LoginData<'a>,
}

#[derive(Debug, Serialize)]
pub(crate) struct LoginData<'a> {
    #[serde(rename = "ACCOUNT_NAME")]
    pub(crate) account_name: &'a str,
    #[serde(rename = "LOGIN_NAME")]
    pub(crate) login_name: &'a str,
    #[serde(flatten)]
    pub(crate) credential: LoginCredentialWire<'a>,
    #[serde(rename = "SESSION_PARAMETERS", skip_serializing_if = "Option::is_none")]
    pub(crate) session_parameters: Option<&'a HashMap<String, serde_json::Value>>,
}

#[derive(Debug)]
pub(crate) enum LoginCredentialWire<'a> {
    Password {
        password: &'a str,
        passcode: Option<PasscodeWire<'a>>,
    },
    #[cfg(feature = "key-pair-auth")]
    SnowflakeJwt {
        token: &'a str,
    },
    OAuth {
        token: &'a str,
    },
    #[cfg(feature = "external-browser-sso")]
    ExternalBrowser {
        token: &'a str,
        proof_key: Option<&'a str>,
    },
}

/// MFA passcode carried alongside `PASSWORD` in a password login request.
#[derive(Debug)]
pub(crate) enum PasscodeWire<'a> {
    /// Sent as a separate `PASSCODE` field.
    Separate(&'a str),
    /// The passcode is already appended to `PASSWORD`; only the method flag is sent.
    InPassword,
}

impl Serialize for LoginCredentialWire<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Password { password, passcode } => {
                let len = match passcode {
                    None => 1,
                    Some(PasscodeWire::InPassword) => 2,
                    Some(PasscodeWire::Separate(_)) => 3,
                };
                let mut map = serializer.serialize_map(Some(len))?;
                map.serialize_entry("PASSWORD", password)?;
                match passcode {
                    None => {}
                    Some(PasscodeWire::InPassword) => {
                        map.serialize_entry("EXT_AUTHN_DUO_METHOD", "passcode")?;
                    }
                    Some(PasscodeWire::Separate(code)) => {
                        map.serialize_entry("EXT_AUTHN_DUO_METHOD", "passcode")?;
                        map.serialize_entry("PASSCODE", code)?;
                    }
                }
                map.end()
            }
            #[cfg(feature = "key-pair-auth")]
            Self::SnowflakeJwt { token } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("AUTHENTICATOR", "SNOWFLAKE_JWT")?;
                map.serialize_entry("TOKEN", token)?;
                map.end()
            }
            Self::OAuth { token } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("AUTHENTICATOR", "OAUTH")?;
                map.serialize_entry("TOKEN", token)?;
                map.end()
            }
            #[cfg(feature = "external-browser-sso")]
            Self::ExternalBrowser { token, proof_key } => {
                let len = if proof_key.is_some() { 3 } else { 2 };
                let mut map = serializer.serialize_map(Some(len))?;
                map.serialize_entry("AUTHENTICATOR", "EXTERNALBROWSER")?;
                map.serialize_entry("TOKEN", token)?;
                if let Some(proof_key) = proof_key {
                    map.serialize_entry("PROOF_KEY", proof_key)?;
                }
                map.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use reqwest::Url;
    use serde_json::json;

    use super::*;

    #[test]
    fn password_login_body_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::Password {
                    password: "secret",
                    passcode: None,
                },
                session_parameters: None,
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "PASSWORD": "secret",
                }
            })
        );
    }

    #[test]
    fn password_login_body_with_separate_passcode_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::Password {
                    password: "secret",
                    passcode: Some(PasscodeWire::Separate("123456")),
                },
                session_parameters: None,
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "PASSWORD": "secret",
                    "EXT_AUTHN_DUO_METHOD": "passcode",
                    "PASSCODE": "123456",
                }
            })
        );
    }

    #[test]
    fn password_login_body_with_passcode_in_password_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::Password {
                    password: "secret123456",
                    passcode: Some(PasscodeWire::InPassword),
                },
                session_parameters: None,
            },
        };

        // No PASSCODE field: the passcode is already inside PASSWORD.
        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "PASSWORD": "secret123456",
                    "EXT_AUTHN_DUO_METHOD": "passcode",
                }
            })
        );
    }

    #[cfg(feature = "key-pair-auth")]
    #[test]
    fn snowflake_jwt_login_body_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::SnowflakeJwt { token: "jwt-token" },
                session_parameters: None,
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "AUTHENTICATOR": "SNOWFLAKE_JWT",
                    "TOKEN": "jwt-token",
                }
            })
        );
    }

    #[test]
    fn oauth_login_body_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::OAuth {
                    token: "oauth-token",
                },
                session_parameters: None,
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "AUTHENTICATOR": "OAUTH",
                    "TOKEN": "oauth-token",
                }
            })
        );
    }

    #[cfg(feature = "external-browser-sso")]
    #[test]
    fn external_browser_login_body_with_proof_key_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::ExternalBrowser {
                    token: "browser-token",
                    proof_key: Some("proof-key"),
                },
                session_parameters: None,
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "AUTHENTICATOR": "EXTERNALBROWSER",
                    "TOKEN": "browser-token",
                    "PROOF_KEY": "proof-key",
                }
            })
        );
    }

    #[cfg(feature = "external-browser-sso")]
    #[test]
    fn external_browser_login_body_without_proof_key_matches_wire_shape() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::ExternalBrowser {
                    token: "browser-token",
                    proof_key: None,
                },
                session_parameters: None,
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "AUTHENTICATOR": "EXTERNALBROWSER",
                    "TOKEN": "browser-token",
                }
            })
        );
    }

    #[test]
    fn session_parameters_are_omitted_when_empty() {
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::Password {
                    password: "secret",
                    passcode: None,
                },
                session_parameters: None,
            },
        };

        let value = serde_json::to_value(&body).unwrap();
        assert!(value["data"].get("SESSION_PARAMETERS").is_none());
    }

    #[test]
    fn session_parameters_are_included_when_present() {
        let mut session_parameters = HashMap::new();
        session_parameters.insert("AUTOCOMMIT".to_string(), json!(true));
        let body = LoginBody {
            data: LoginData {
                account_name: "account",
                login_name: "username",
                credential: LoginCredentialWire::Password {
                    password: "secret",
                    passcode: None,
                },
                session_parameters: Some(&session_parameters),
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({
                "data": {
                    "ACCOUNT_NAME": "account",
                    "LOGIN_NAME": "username",
                    "PASSWORD": "secret",
                    "SESSION_PARAMETERS": {
                        "AUTOCOMMIT": true,
                    }
                }
            })
        );
    }

    #[test]
    fn login_query_serializes_expected_keys_and_order() {
        let query = LoginQuery {
            warehouse: Some("warehouse"),
            database_name: Some("database"),
            schema_name: Some("schema"),
            role_name: Some("role"),
            #[cfg(feature = "external-browser-sso")]
            request_id: Some(
                Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8")
                    .expect("fixed UUID must parse"),
            ),
            #[cfg(not(feature = "external-browser-sso"))]
            request_id: None,
        };
        let request = reqwest::Client::new()
            .get(Url::parse("https://example.com").unwrap())
            .query(&query)
            .build()
            .unwrap();

        #[cfg(feature = "external-browser-sso")]
        assert_eq!(
            request.url().query(),
            Some(
                "warehouse=warehouse&databaseName=database&schemaName=schema&roleName=role&request_id=936da01f-9abd-4d9d-80c7-02af85c822a8"
            )
        );

        #[cfg(not(feature = "external-browser-sso"))]
        assert_eq!(
            request.url().query(),
            Some("warehouse=warehouse&databaseName=database&schemaName=schema&roleName=role")
        );
    }

    #[test]
    fn login_query_omits_absent_fields() {
        let request = reqwest::Client::new()
            .get(Url::parse("https://example.com").unwrap())
            .query(&LoginQuery::default())
            .build()
            .unwrap();

        assert_eq!(request.url().query(), None);
    }
}
