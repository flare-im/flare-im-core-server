//! JWT 后端互验（一）：进程级默认 provider 是 flare-server-core 装的那一个（HS256/384/512、EdDSA）。
//!
//! 生产进程在建令牌服务、签名器或验证器时装上它，之后 jsonwebtoken 的 `encode` / `decode` 都走它。这里先建一个
//! 令牌服务，再跑共用用例；另外确认厂商令牌不受这个默认值影响。

use std::sync::Once;

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, EncodingKey, Header};

#[path = "support/jwt_backend_cases.rs"]
mod cases;

fn setup() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = flare_server_core::TokenService::new(cases::SECRET, "flare-jwt-backend-test", 3600);
    });
}

/// Under server-core's provider an ES256 `encode` is refused (the provider knows HS256/384/512 and EdDSA only),
/// while the vendor token signer, which names aws-lc explicitly, still signs it.
#[test]
fn vendor_tokens_do_not_depend_on_the_process_default() {
    setup();
    use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    let pkcs8 = EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING)
        .unwrap()
        .to_pkcs8v1()
        .unwrap();
    let key = EncodingKey::from_ec_der(pkcs8.as_ref());
    let claims = serde_json::json!({"iss": "TEAMID1234", "iat": 0});
    let header = Header::new(Algorithm::ES256);
    assert!(matches!(
        jsonwebtoken::encode(&header, &claims, &key).map_err(|e| e.into_kind()),
        Err(ErrorKind::InvalidAlgorithm)
    ));
    flare_push_worker::infrastructure::vendor_jwt::sign_vendor_jwt(
        Algorithm::ES256,
        &header,
        &claims,
        &key,
    )
    .unwrap();
}
