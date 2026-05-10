use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::RsaPrivateKey;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{Result, error::AuthError};

pub(super) fn generate_jwt_from_key_pair(
    pem: &str,
    password: Option<impl AsRef<[u8]>>,
    username: &str,
    account: &str,
    timestamp: i64,
) -> Result<String> {
    let account = account
        .split('.')
        .next()
        .map(|s| s.to_ascii_uppercase())
        .unwrap_or_default();
    let username = username.to_ascii_uppercase();
    let private = if let Some(password) = password {
        RsaPrivateKey::from_pkcs8_encrypted_pem(pem, password).map_err(AuthError::key_parse)?
    } else {
        RsaPrivateKey::from_pkcs8_pem(pem).map_err(AuthError::key_parse)?
    };
    let public = private.to_public_key();
    let der = public.to_public_key_der().map_err(AuthError::der_parse)?;
    let mut hasher = Sha256::new();
    hasher.update(der);
    let hash = hasher.finalize();
    let fingerprint = STANDARD.encode(hash);

    let payload = json!({
        "iss": format!("{}.{}.SHA256:{}", account, username, fingerprint),
        "sub": format!("{}.{}", account, username),
        "iat": timestamp,
        "exp": timestamp + 600
    });
    let pem = private
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(AuthError::key_parse)?;
    let key = EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(AuthError::jwt_sign)?;
    let jwt = jsonwebtoken::encode(
        &Header {
            alg: Algorithm::RS256,
            ..Default::default()
        },
        &payload,
        &key,
    )
    .map_err(AuthError::jwt_sign)?;
    Ok(jwt)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENCRYPTED_TEST_PEM: &str = include_str!("./test_snowflake_key.p8");
    const UNENCRYPTED_TEST_PEM: &str = include_str!("./test_snowflake_key_unencrypted.p8");
    const EXPECTED_JWT: &str = r#"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3MDA3NDY5NzQsImlhdCI6MTcwMDc0NjM3NCwiaXNzIjoiTVlBQ0NPVU5ULlVTRVJfTkFNRS5TSEEyNTY6S1NFV3pOaW9sbkpGcTAwNDlNV2diU0dMbFhHdjZnVHNpaGlVUmxPZTE1dz0iLCJzdWIiOiJNWUFDQ09VTlQuVVNFUl9OQU1FIn0.CymITivfHERyl_JiM49BSG_sgD0jAD7lTa1qeTMpKFpkGS7TMfOZBYuNj4FsIGxBQtob60pUiyunjKaQbtPjHLlMDQP62rW03qC68m-d4RuYZqzi7P16Go_FVYGIxaoHUsM25IWxuKBLOmsWwG7tVhT6ZHFKvMvqxOZEVIBbB7pFEIMjsAOBCjSDARxu7fhHmR6Oy64XPMr2Xw_NDm-yVPcEv3NdonyO1zMS6QiRKX4Yqzku5fXeOJWvPaUtkYdwm15jzVvV3zH5OkSw252ZiVaZBSkWmwpn7YQk8StjRBamncLiOAU7EFmSgAt6Lzi-kLv0fg4ZfMTcxfhxwBNVfN0vK6UTPcnxbjZK0n6i5JK2m6XPdiByHmhSgwCvDJ0ZLn8uGze5nU1Zdlfcg8fci5tsh-Q6BMuqvx6M21dQ_E3GF6GDcuX-_d8Ap7CUtdMmWLUYNdgnDDV3reKqdeopPuuBO5zXXEYtek1Q6iqb57bQMFcK6tg3HMnHqUxruzuyKZv0S30teC1STBKS7IrGB_etFtEQ2eF7Qea3yIoxxXAkCxUXcIWkDWyt5RzUyCpQd-MYYTiD2o_bf_XS588bGZ1zzQ9lB-9aRVWYW3gUAOVykv-IW8FnndNePGVkPiX8uhUMW1NC6VHQvEcgWY-EfxZ4eoUZxd5ldk5kxPzDEeA"#;

    #[test]
    fn test_generate_jwt_from_key_pair() -> Result<()> {
        let jwt = generate_jwt_from_key_pair(
            ENCRYPTED_TEST_PEM,
            Some("12345".as_bytes()),
            "USER_NAME",
            "myaccount.ap-northeast-1.aws",
            1700746374,
        )?;
        assert_eq!(jwt, EXPECTED_JWT);
        Ok(())
    }

    #[test]
    fn test_generate_jwt_from_unencrypted_key_pair() -> Result<()> {
        let jwt = generate_jwt_from_key_pair(
            UNENCRYPTED_TEST_PEM,
            None::<&[u8]>,
            "USER_NAME",
            "myaccount.ap-northeast-1.aws",
            1700746374,
        )?;

        assert_eq!(jwt, EXPECTED_JWT);
        Ok(())
    }
}
