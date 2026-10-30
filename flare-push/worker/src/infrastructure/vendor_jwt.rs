//! 厂商令牌（APNs provider token、FCM 服务账号断言）的签名：显式用 jsonwebtoken 的 aws-lc 后端，
//! 不经进程级默认 `CryptoProvider`。
//!
//! jsonwebtoken 10 的 `encode` 取进程级默认 provider。flare-server-core 在建令牌服务时装上自己的
//! provider，只认 HS256/384/512 与 EdDSA（接入令牌、Cell 内部上下文），ES256、RS256 在那里一律
//! `InvalidAlgorithm`；它装上之前，默认值又由 crate 特性决定。厂商令牌若走默认 provider，能不能签
//! 取决于进程里谁先用到 jsonwebtoken。这里每处调用只认自己的那一个算法（APNs 为 ES256，FCM 为
//! RS256），头部算法、密钥类型和签名器三者都必须是它；进程默认 provider 与服务端令牌的算法白名单不变。

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER;
use jsonwebtoken::errors::{ErrorKind, Result};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;

/// 以 `algorithm` 签一枚紧凑 JWS：`base64url(header).base64url(claims).base64url(signature)`，无填充。
///
/// `header.alg`、`key` 的类型与签名器的算法都必须是 `algorithm`，否则返回 `InvalidAlgorithm`，
/// 不会退到别的算法。
pub fn sign_vendor_jwt<T: Serialize>(
    algorithm: Algorithm,
    header: &Header,
    claims: &T,
    key: &EncodingKey,
) -> Result<String> {
    if header.alg != algorithm || key.family() != algorithm.family() {
        return Err(ErrorKind::InvalidAlgorithm.into());
    }
    let signer = (DEFAULT_PROVIDER.signer_factory)(&algorithm, key)?;
    if signer.algorithm() != algorithm {
        return Err(ErrorKind::InvalidAlgorithm.into());
    }
    let message = format!("{}.{}", encoded_part(header)?, encoded_part(claims)?);
    let signature = signer.try_sign(message.as_bytes())?;
    Ok(format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature)))
}

fn encoded_part<T: Serialize>(value: &T) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeyPair as RsaKeyPair, KeySize, PublicKeyComponents};
    use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair as _};
    use base64::engine::general_purpose::STANDARD;
    use jsonwebtoken::DecodingKey;

    #[derive(Serialize)]
    struct Claims<'a> {
        iss: &'a str,
        iat: u64,
    }

    fn pem(label: &str, der: &[u8]) -> String {
        let body = STANDARD.encode(der);
        let lines: Vec<&str> = body
            .as_bytes()
            .chunks(64)
            .map(|line| std::str::from_utf8(line).unwrap())
            .collect();
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            lines.join("\n")
        )
    }

    /// An APNs-style key: a P-256 key as a PKCS#8 `PRIVATE KEY` document (the `.p8` file), with its public point.
    fn apns_key() -> (EncodingKey, DecodingKey) {
        let pair = EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING).unwrap();
        let pkcs8 = pair.to_pkcs8v1().unwrap();
        (
            EncodingKey::from_ec_pem(pem("PRIVATE KEY", pkcs8.as_ref()).as_bytes()).unwrap(),
            DecodingKey::from_ec_der(pair.public_key().as_ref()),
        )
    }

    /// An FCM-style key: an RSA 2048 key as the PKCS#8 `PRIVATE KEY` document of a service account, with its modulus and exponent.
    fn fcm_key() -> (EncodingKey, DecodingKey) {
        let pair = RsaKeyPair::generate(KeySize::Rsa2048).unwrap();
        let pkcs8 = AsDer::<aws_lc_rs::encoding::Pkcs8V1Der>::as_der(&pair).unwrap();
        let public = PublicKeyComponents::<Vec<u8>>::from(pair.public_key());
        (
            EncodingKey::from_rsa_pem(pem("PRIVATE KEY", pkcs8.as_ref()).as_bytes()).unwrap(),
            DecodingKey::from_rsa_raw_components(&public.n, &public.e),
        )
    }

    fn split(token: &str) -> (&str, Vec<u8>) {
        let (message, signature) = token.rsplit_once('.').unwrap();
        (message, URL_SAFE_NO_PAD.decode(signature).unwrap())
    }

    fn verifies(algorithm: Algorithm, token: &str, key: &DecodingKey) -> bool {
        let (message, signature) = split(token);
        (DEFAULT_PROVIDER.verifier_factory)(&algorithm, key)
            .unwrap()
            .verify(message.as_bytes(), &signature)
            .is_ok()
    }

    fn header(algorithm: Algorithm) -> Header {
        let mut header = Header::new(algorithm);
        header.kid = Some("ABC123DEFG".into());
        header
    }

    /// The APNs provider token is an ES256 JWS: header `{alg: ES256, kid}`, the JOSE fixed-length r‖s signature
    /// (64 bytes, not DER), verified by the key's public point over exactly the first two segments.
    #[test]
    fn an_es256_vendor_token_verifies_with_the_public_key() {
        let (key, public) = apns_key();
        let claims = Claims {
            iss: "TEAMID1234",
            iat: 1_700_000_000,
        };
        let token =
            sign_vendor_jwt(Algorithm::ES256, &header(Algorithm::ES256), &claims, &key).unwrap();

        let decoded = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(decoded.alg, Algorithm::ES256);
        assert_eq!(decoded.kid.as_deref(), Some("ABC123DEFG"));
        assert_eq!(
            split(&token).1.len(),
            64,
            "JOSE ECDSA signatures are r||s, not DER"
        );
        assert!(verifies(Algorithm::ES256, &token, &public));

        // The signature covers the claims: a changed issuer no longer verifies.
        let (message, _) = split(&token);
        let forged = token.replacen(
            message.split('.').nth(1).unwrap(),
            &URL_SAFE_NO_PAD.encode(br#"{"iss":"OTHERTEAM1","iat":1700000000}"#),
            1,
        );
        assert!(!verifies(Algorithm::ES256, &forged, &public));
        // Another key's public point does not verify it.
        assert!(!verifies(Algorithm::ES256, &token, &apns_key().1));
    }

    /// The FCM service-account assertion is an RS256 JWS verified by the account key's public half.
    #[test]
    fn an_rs256_vendor_token_verifies_with_the_public_key() {
        let (key, public) = fcm_key();
        let claims = Claims {
            iss: "svc@example.iam.gserviceaccount.com",
            iat: 1_700_000_000,
        };
        let token = sign_vendor_jwt(
            Algorithm::RS256,
            &Header::new(Algorithm::RS256),
            &claims,
            &key,
        )
        .unwrap();
        assert_eq!(
            jsonwebtoken::decode_header(&token).unwrap().alg,
            Algorithm::RS256
        );
        assert_eq!(
            split(&token).1.len(),
            256,
            "an RSA 2048 signature is the modulus length"
        );
        assert!(verifies(Algorithm::RS256, &token, &public));
        assert!(!verifies(Algorithm::RS256, &token, &fcm_key().1));
    }

    /// Each call signs with its one algorithm: a header naming another one, a key of another family or an
    /// HMAC secret is refused instead of signed with whatever the key would allow.
    #[test]
    fn a_vendor_token_is_only_signed_with_the_expected_algorithm_and_key_family() {
        let (ec, _) = apns_key();
        let (rsa, _) = fcm_key();
        let secret = EncodingKey::from_secret(b"not a vendor key");
        let claims = Claims {
            iss: "TEAMID1234",
            iat: 0,
        };
        let refused = |algorithm, header: Header, key: &EncodingKey| {
            matches!(
                sign_vendor_jwt(algorithm, &header, &claims, key).map_err(|e| e.into_kind()),
                Err(ErrorKind::InvalidAlgorithm)
            )
        };
        // The header must name the expected algorithm.
        assert!(refused(
            Algorithm::ES256,
            Header::new(Algorithm::ES384),
            &ec
        ));
        assert!(refused(
            Algorithm::RS256,
            Header::new(Algorithm::PS256),
            &rsa
        ));
        assert!(refused(
            Algorithm::ES256,
            Header::new(Algorithm::HS256),
            &secret
        ));
        // The key must be of the algorithm's family.
        assert!(refused(
            Algorithm::ES256,
            Header::new(Algorithm::ES256),
            &rsa
        ));
        assert!(refused(
            Algorithm::RS256,
            Header::new(Algorithm::RS256),
            &ec
        ));
        assert!(refused(
            Algorithm::RS256,
            Header::new(Algorithm::RS256),
            &secret
        ));
        assert!(refused(
            Algorithm::ES256,
            Header::new(Algorithm::ES256),
            &secret
        ));
    }
}
