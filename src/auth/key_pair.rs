use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use pkcs8::{DecodePrivateKey as _, EncodePublicKey as _};
use rsa::{Pkcs1v15Sign, RsaPrivateKey};
use serde::{Serialize, Serializer, ser::SerializeStruct as _};
use sha2::{Digest as _, Sha256};

use crate::error::AuthError;

/// DER-encoded `DigestInfo` prefix for SHA-256, without the 32-byte digest:
/// SEQUENCE {
///   SEQUENCE { OID 2.16.840.1.101.3.4.2.1, NULL }
///   OCTET STRING <digest>
/// }
const SHA256_DIGEST_INFO_PREFIX: &[u8] = &[
    0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01, 0x05,
    0x00, 0x04, 0x20,
];

/// Base64url without padding for `{"typ":"JWT","alg":"RS256"}`.
const ENCODED_JWT_HEADER: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9";

struct JwtPayload {
    exp: i64,
    iat: i64,
    account: String,
    username: String,
    fingerprint: String,
}

impl Serialize for JwtPayload {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JwtPayload", 4)?;
        state.serialize_field("exp", &self.exp)?;
        state.serialize_field("iat", &self.iat)?;
        state.serialize_field("iss", &Issuer(self))?;
        state.serialize_field("sub", &Subject(self))?;
        state.end()
    }
}

struct Issuer<'a>(&'a JwtPayload);

impl Serialize for Issuer<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&format_args!(
            "{}.{}.SHA256:{}",
            self.0.account, self.0.username, self.0.fingerprint
        ))
    }
}

struct Subject<'a>(&'a JwtPayload);

impl Serialize for Subject<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&format_args!("{}.{}", self.0.account, self.0.username))
    }
}

pub(super) fn generate_jwt_from_key_pair(
    pem: &str,
    password: Option<impl AsRef<[u8]>>,
    username: &str,
    account: &str,
    timestamp: i64,
) -> std::result::Result<String, AuthError> {
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

    let payload = JwtPayload {
        exp: timestamp + 600,
        iat: timestamp,
        account: account
            .split('.')
            .next()
            .map(str::to_ascii_uppercase)
            .unwrap_or_default(),
        username: username.to_ascii_uppercase(),
        fingerprint,
    };
    let jwt = encode_rs256_jwt(&payload, private)?;

    Ok(jwt)
}

fn encode_rs256_jwt(
    payload: &JwtPayload,
    private: RsaPrivateKey,
) -> std::result::Result<String, AuthError> {
    let encoded_payload =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload).map_err(AuthError::jwt_sign)?);

    let mut signing_input =
        String::with_capacity(ENCODED_JWT_HEADER.len() + 1 + encoded_payload.len());
    signing_input.push_str(ENCODED_JWT_HEADER);
    signing_input.push('.');
    signing_input.push_str(&encoded_payload);

    let digest = Sha256::digest(signing_input.as_bytes());
    let signature = private
        .sign(
            Pkcs1v15Sign {
                hash_len: Some(digest.len()),
                prefix: SHA256_DIGEST_INFO_PREFIX.into(),
            },
            &digest,
        )
        .map_err(AuthError::jwt_sign)?;
    let encoded_signature = URL_SAFE_NO_PAD.encode(signature);

    let mut jwt = String::with_capacity(signing_input.len() + 1 + encoded_signature.len());
    jwt.push_str(&signing_input);
    jwt.push('.');
    jwt.push_str(&encoded_signature);

    Ok(jwt)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENCRYPTED_TEST_PEM: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/test_snowflake_key.p8"
    ));
    const UNENCRYPTED_TEST_PEM: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/test_snowflake_key_unencrypted.p8"
    ));
    const EXPECTED_JWT: &str = r#"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3MDA3NDY5NzQsImlhdCI6MTcwMDc0NjM3NCwiaXNzIjoiTVlBQ0NPVU5ULlVTRVJfTkFNRS5TSEEyNTY6S1NFV3pOaW9sbkpGcTAwNDlNV2diU0dMbFhHdjZnVHNpaGlVUmxPZTE1dz0iLCJzdWIiOiJNWUFDQ09VTlQuVVNFUl9OQU1FIn0.CymITivfHERyl_JiM49BSG_sgD0jAD7lTa1qeTMpKFpkGS7TMfOZBYuNj4FsIGxBQtob60pUiyunjKaQbtPjHLlMDQP62rW03qC68m-d4RuYZqzi7P16Go_FVYGIxaoHUsM25IWxuKBLOmsWwG7tVhT6ZHFKvMvqxOZEVIBbB7pFEIMjsAOBCjSDARxu7fhHmR6Oy64XPMr2Xw_NDm-yVPcEv3NdonyO1zMS6QiRKX4Yqzku5fXeOJWvPaUtkYdwm15jzVvV3zH5OkSw252ZiVaZBSkWmwpn7YQk8StjRBamncLiOAU7EFmSgAt6Lzi-kLv0fg4ZfMTcxfhxwBNVfN0vK6UTPcnxbjZK0n6i5JK2m6XPdiByHmhSgwCvDJ0ZLn8uGze5nU1Zdlfcg8fci5tsh-Q6BMuqvx6M21dQ_E3GF6GDcuX-_d8Ap7CUtdMmWLUYNdgnDDV3reKqdeopPuuBO5zXXEYtek1Q6iqb57bQMFcK6tg3HMnHqUxruzuyKZv0S30teC1STBKS7IrGB_etFtEQ2eF7Qea3yIoxxXAkCxUXcIWkDWyt5RzUyCpQd-MYYTiD2o_bf_XS588bGZ1zzQ9lB-9aRVWYW3gUAOVykv-IW8FnndNePGVkPiX8uhUMW1NC6VHQvEcgWY-EfxZ4eoUZxd5ldk5kxPzDEeA"#;

    #[test]
    fn encoded_jwt_header_matches_rs256_header_json() {
        assert_eq!(
            URL_SAFE_NO_PAD.encode(r#"{"typ":"JWT","alg":"RS256"}"#),
            ENCODED_JWT_HEADER
        );
    }

    #[test]
    fn sha256_digest_info_prefix_matches_der_shape() {
        assert_eq!(SHA256_DIGEST_INFO_PREFIX[0..2], [0x30, 0x31]);
        assert_eq!(SHA256_DIGEST_INFO_PREFIX[2..4], [0x30, 0x0d]);
        assert_eq!(SHA256_DIGEST_INFO_PREFIX[4..6], [0x06, 0x09]);
        assert_eq!(
            SHA256_DIGEST_INFO_PREFIX[6..15],
            [0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01]
        );
        assert_eq!(SHA256_DIGEST_INFO_PREFIX[15..17], [0x05, 0x00]);
        assert_eq!(SHA256_DIGEST_INFO_PREFIX[17..19], [0x04, 0x20]);
    }

    #[test]
    fn test_generate_jwt_from_key_pair() -> std::result::Result<(), AuthError> {
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
    fn test_generate_jwt_from_unencrypted_key_pair() -> std::result::Result<(), AuthError> {
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
