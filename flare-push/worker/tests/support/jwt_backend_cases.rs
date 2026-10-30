//! JWT 后端互验的共用用例。两个测试二进制各自先定下进程级默认 provider（`super::setup`），再跑这些用例：
//! flare-server-core 签验的 HS256 接入令牌与 EdDSA 令牌，在 server-core 的 provider 与 aws-lc 后端下逐字节相同、
//! 互相可验；算法混淆一律被拒；过期、签发时间与 nbf 的判定与后端无关。

use std::time::{SystemTime, UNIX_EPOCH};

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::{KeyPair as RsaKeyPair, KeySize};
use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_push_worker::infrastructure::vendor_jwt::sign_vendor_jwt;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    TokenService,
};
use jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER as AWS_LC;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde_json::json;

pub const SECRET: &str = "jwt-backend-interop-secret-0123456789abcdef";
const ISSUER: &str = "flare-jwt-backend-test";
const ED_ISSUER: &str = "flare-console";
const ED_AUDIENCE: &str = "flare-admin";
const ED_USE: &str = "admin";

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
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

struct Ed25519 {
    private_pem: String,
    public_pem: String,
    public_raw: [u8; 32],
}

fn ed25519() -> Ed25519 {
    let key = SigningKey::from_bytes(&[7; 32]);
    Ed25519 {
        private_pem: key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
        public_pem: key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
        public_raw: key.verifying_key().to_bytes(),
    }
}

fn ec_key() -> EncodingKey {
    let pkcs8 = EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING)
        .unwrap()
        .to_pkcs8v1()
        .unwrap();
    EncodingKey::from_ec_pem(pem("PRIVATE KEY", pkcs8.as_ref()).as_bytes()).unwrap()
}

/// An RSA private key and the PEM of its public half.
fn rsa_key() -> (EncodingKey, String) {
    use aws_lc_rs::signature::KeyPair as _;
    let pair = RsaKeyPair::generate(KeySize::Rsa2048).unwrap();
    let pkcs8 = AsDer::<aws_lc_rs::encoding::Pkcs8V1Der>::as_der(&pair).unwrap();
    let public = AsDer::<aws_lc_rs::encoding::PublicKeyX509Der>::as_der(pair.public_key()).unwrap();
    (
        EncodingKey::from_rsa_pem(pem("PRIVATE KEY", pkcs8.as_ref()).as_bytes()).unwrap(),
        pem("PUBLIC KEY", public.as_ref()),
    )
}

fn split(token: &str) -> (&str, Vec<u8>) {
    let (message, signature) = token.rsplit_once('.').unwrap();
    (message, URL_SAFE_NO_PAD.decode(signature).unwrap())
}

/// The aws-lc signature of a JWS signing input (`header.claims`).
fn aws_lc_signature(algorithm: Algorithm, key: &EncodingKey, message: &str) -> Vec<u8> {
    (AWS_LC.signer_factory)(&algorithm, key)
        .unwrap()
        .try_sign(message.as_bytes())
        .unwrap()
}

fn aws_lc_verifies(algorithm: Algorithm, key: &DecodingKey, token: &str) -> bool {
    let (message, signature) = split(token);
    (AWS_LC.verifier_factory)(&algorithm, key)
        .unwrap()
        .verify(message.as_bytes(), &signature)
        .is_ok()
}

/// A token over `header` and `claims` signed by aws-lc with `algorithm` (an HMAC `secret` or `key`).
fn aws_lc_token(
    algorithm: Algorithm,
    header: serde_json::Value,
    claims: serde_json::Value,
    key: &EncodingKey,
) -> String {
    let message = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let signature = aws_lc_signature(algorithm, key, &message);
    format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature))
}

fn access_claims(exp: u64, iat: u64) -> serde_json::Value {
    json!({"sub": "u1", "iss": ISSUER, "exp": exp, "iat": iat, "jti": "j1", "device_id": null, "tenant_id": "t1"})
}

fn hs256(claims: serde_json::Value) -> String {
    aws_lc_token(
        Algorithm::HS256,
        json!({"typ": "JWT", "alg": "HS256"}),
        claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
}

fn access_service() -> TokenService {
    TokenService::new(SECRET, ISSUER, 3600)
}

fn ed_issuer() -> TokenService {
    TokenService::from_ed25519_private_key(&ed25519().private_pem, ED_ISSUER, 600).unwrap()
}

fn ed_verifier() -> TokenService {
    TokenService::from_ed25519_public_key(&ed25519().public_pem, ED_ISSUER).unwrap()
}

fn signed_context() -> String {
    SignedContextSigner::from_private_key(&ed25519().private_pem)
        .unwrap()
        .sign(&SignedContextIdentity::new(
            SignedActorType::Service,
            "flare-orchestrator",
            "tenant-a",
            "request-1",
        ))
        .unwrap()
}

/// HS256 is deterministic: server-core's access token carries exactly the aws-lc signature of its signing input,
/// so a token from either backend verifies under the other.
#[test]
fn hs256_access_tokens_are_byte_identical_across_backends() {
    super::setup();
    let service = access_service();
    let token = service
        .generate_token("u1", Some("d1"), Some("t1"))
        .unwrap();
    let (message, signature) = split(&token);
    let secret = EncodingKey::from_secret(SECRET.as_bytes());
    assert_eq!(
        aws_lc_signature(Algorithm::HS256, &secret, message),
        signature
    );
    assert!(aws_lc_verifies(
        Algorithm::HS256,
        &DecodingKey::from_secret(SECRET.as_bytes()),
        &token
    ));
    let claims = service.validate_access_token(&token).unwrap();
    assert_eq!(
        (claims.sub.as_str(), claims.tenant_id.as_deref()),
        ("u1", Some("t1"))
    );

    // An aws-lc token server-core never issued is accepted by its verifier, with the same claims.
    let now = now();
    let claims = service
        .validate_access_token(&hs256(access_claims(now + 600, now)))
        .unwrap();
    assert_eq!(claims.jti, "j1");
}

/// Ed25519 is deterministic as well: the cell-internal context and the Ed25519 token service sign exactly what
/// aws-lc signs for the same key and signing input, and each verifier accepts the other's token.
#[test]
fn eddsa_tokens_are_byte_identical_across_backends() {
    super::setup();
    let keys = ed25519();
    let private = EncodingKey::from_ed_pem(keys.private_pem.as_bytes()).unwrap();
    let public = DecodingKey::from_ed_der(&keys.public_raw);

    let context = signed_context();
    let (message, signature) = split(&context);
    assert_eq!(
        aws_lc_signature(Algorithm::EdDSA, &private, message),
        signature
    );
    assert!(aws_lc_verifies(Algorithm::EdDSA, &public, &context));
    let claims = SignedContextVerifier::from_public_key(&keys.public_pem)
        .unwrap()
        .verify(&context)
        .unwrap();
    assert_eq!(claims.actor_id, "flare-orchestrator");

    let admin = ed_issuer()
        .generate_with_ed25519("staff-1", Some("t1"), ED_USE, ED_AUDIENCE)
        .unwrap();
    let (message, signature) = split(&admin);
    assert_eq!(
        aws_lc_signature(Algorithm::EdDSA, &private, message),
        signature
    );
    assert!(aws_lc_verifies(Algorithm::EdDSA, &public, &admin));
    ed_verifier()
        .validate_ed25519_with_use(&admin, ED_USE, ED_AUDIENCE)
        .unwrap();

    // aws-lc's own token for the same claims is accepted by server-core's Ed25519 verifier.
    let now = now();
    let token = aws_lc_token(
        Algorithm::EdDSA,
        json!({"typ": "JWT", "alg": "EdDSA"}),
        json!({"sub": "staff-2", "iss": ED_ISSUER, "aud": ED_AUDIENCE, "exp": now + 300, "iat": now, "jti": "j2",
               "device_id": null, "tenant_id": "t1", "token_use": ED_USE}),
        &private,
    );
    assert_eq!(
        ed_verifier()
            .validate_ed25519_with_use(&token, ED_USE, ED_AUDIENCE)
            .unwrap()
            .sub,
        "staff-2"
    );
}

/// `alg: none` is not an algorithm to either backend: no verifier accepts an unsigned token.
#[test]
fn unsigned_tokens_are_refused() {
    super::setup();
    let now = now();
    let unsigned = |claims: serde_json::Value| {
        format!(
            "{}.{}.",
            URL_SAFE_NO_PAD.encode(json!({"typ": "JWT", "alg": "none"}).to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    };
    assert!(
        access_service()
            .validate_token(&unsigned(access_claims(now + 600, now)))
            .is_err()
    );
    assert!(
        ed_verifier()
            .validate_ed25519_with_use(
                &unsigned(json!({"sub": "s", "iss": ED_ISSUER, "aud": ED_AUDIENCE,
            "exp": now + 300, "iat": now, "jti": "j", "token_use": ED_USE})),
                ED_USE,
                ED_AUDIENCE
            )
            .is_err()
    );
    let context = signed_context();
    let (message, _) = split(&context);
    let claims = message.split('.').nth(1).unwrap();
    let unsigned_context = format!(
        "{}.{claims}.",
        URL_SAFE_NO_PAD.encode(json!({"typ": "JWT", "alg": "none"}).to_string())
    );
    assert!(
        SignedContextVerifier::from_public_key(&ed25519().public_pem)
            .unwrap()
            .verify(&unsigned_context)
            .is_err()
    );
}

/// HMAC and public-key algorithms do not stand in for each other: an HS256 token keyed by a public key, an
/// Ed25519 token at the HS256 verifier, and RS256 / ES256 tokens at either verifier are all refused.
#[test]
fn hmac_and_public_key_algorithms_are_not_interchangeable() {
    super::setup();
    let keys = ed25519();
    let now = now();

    // HS256 keyed by the Ed25519 public key (raw bytes and its PEM) at the Ed25519 verifiers.
    let context = signed_context();
    let (message, _) = split(&context);
    let context_claims: serde_json::Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(message.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap();
    let admin_claims = json!({"sub": "s", "iss": ED_ISSUER, "aud": ED_AUDIENCE, "exp": now + 300, "iat": now,
        "jti": "j", "token_use": ED_USE});
    let hs_header = json!({"typ": "JWT", "alg": "HS256"});
    for secret in [keys.public_raw.as_slice(), keys.public_pem.as_bytes()] {
        let key = EncodingKey::from_secret(secret);
        let forged = aws_lc_token(
            Algorithm::HS256,
            hs_header.clone(),
            context_claims.clone(),
            &key,
        );
        assert!(
            SignedContextVerifier::from_public_key(&keys.public_pem)
                .unwrap()
                .verify(&forged)
                .is_err()
        );
        let forged = aws_lc_token(
            Algorithm::HS256,
            hs_header.clone(),
            admin_claims.clone(),
            &key,
        );
        assert!(
            ed_verifier()
                .validate_ed25519_with_use(&forged, ED_USE, ED_AUDIENCE)
                .is_err()
        );
    }

    // Ed25519 tokens at the HS256 access-token verifier.
    assert!(access_service().validate_token(&context).is_err());
    let admin = ed_issuer()
        .generate_with_ed25519("staff-1", Some("t1"), ED_USE, ED_AUDIENCE)
        .unwrap();
    assert!(access_service().validate_token(&admin).is_err());

    // RS256 and ES256 tokens over access-token claims at both verifiers.
    let (rsa, rsa_public_pem) = rsa_key();
    let ec = ec_key();
    let claims = access_claims(now + 600, now);
    let rs = sign_vendor_jwt(
        Algorithm::RS256,
        &Header::new(Algorithm::RS256),
        &claims,
        &rsa,
    )
    .unwrap();
    let es = sign_vendor_jwt(
        Algorithm::ES256,
        &Header::new(Algorithm::ES256),
        &claims,
        &ec,
    )
    .unwrap();
    for token in [&rs, &es] {
        assert!(access_service().validate_token(token).is_err());
        assert!(
            ed_verifier()
                .validate_ed25519_with_use(token, ED_USE, ED_AUDIENCE)
                .is_err()
        );
    }

    // The textbook confusion: an HS256 token keyed by the RSA public key's PEM, at an RS256 verifier.
    let forged = hs256_with(&rsa_public_pem, claims);
    let rs256 = DecodingKey::from_rsa_pem(rsa_public_pem.as_bytes()).unwrap();
    assert!(
        jsonwebtoken::decode::<serde_json::Value>(
            &forged,
            &rs256,
            &Validation::new(Algorithm::RS256)
        )
        .is_err()
    );
    // The same public key does verify the real RS256 token, so the refusal above is the algorithm, not the key.
    assert!(aws_lc_verifies(Algorithm::RS256, &rs256, &rs));
}

fn hs256_with(secret: &str, claims: serde_json::Value) -> String {
    aws_lc_token(
        Algorithm::HS256,
        json!({"typ": "JWT", "alg": "HS256"}),
        claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// Expiry, issue time and `nbf` are decided by the claims checks, not the signature backend.
#[test]
fn expiry_issue_time_and_nbf_are_decided_as_before() {
    super::setup();
    let service = access_service();
    let now = now();
    // HS256 access tokens: expired beyond the default 60 s leeway is refused, within it accepted.
    assert!(
        service
            .validate_token(&hs256(access_claims(now - 120, now - 3600)))
            .is_err()
    );
    assert!(
        service
            .validate_token(&hs256(access_claims(now - 30, now - 3600)))
            .is_ok()
    );
    // Access-token validation does not check `nbf` (access tokens never carry one): a future nbf does not change the verdict.
    let mut with_nbf = access_claims(now + 600, now);
    with_nbf["nbf"] = json!(now + 3600);
    assert!(service.validate_token(&hs256(with_nbf)).is_ok());

    // Ed25519 tokens: no leeway; expired or issued in the future is refused.
    let keys = ed25519();
    let private = EncodingKey::from_ed_pem(keys.private_pem.as_bytes()).unwrap();
    let ed = |exp: u64, iat: u64| {
        aws_lc_token(
            Algorithm::EdDSA,
            json!({"typ": "JWT", "alg": "EdDSA"}),
            json!({"sub": "staff-1", "iss": ED_ISSUER, "aud": ED_AUDIENCE, "exp": exp, "iat": iat, "jti": "j",
                   "device_id": null, "tenant_id": "t1", "token_use": ED_USE}),
            &private,
        )
    };
    let verifier = ed_verifier();
    assert!(
        verifier
            .validate_ed25519_with_use(&ed(now + 300, now), ED_USE, ED_AUDIENCE)
            .is_ok()
    );
    assert!(
        verifier
            .validate_ed25519_with_use(&ed(now - 1, now - 300), ED_USE, ED_AUDIENCE)
            .is_err()
    );
    assert!(
        verifier
            .validate_ed25519_with_use(&ed(now + 600, now + 300), ED_USE, ED_AUDIENCE)
            .is_err()
    );
}
