//! JWT 后端互验（二）：进程级默认 provider 由 crate 特性决定，即 aws-lc。
//!
//! 进程里在 flare-server-core 装上自己的 provider 之前若已用过 jsonwebtoken，默认值就定成 aws-lc，server-core
//! 之后的安装不再生效。这里先签一次让默认值落定，再跑同一组用例：令牌、算法混淆与声明判定都与（一）一致。

use std::sync::Once;

use jsonwebtoken::{Algorithm, EncodingKey, Header};

#[path = "support/jwt_backend_cases.rs"]
mod cases;

fn setup() {
    static RESOLVE: Once = Once::new();
    RESOLVE.call_once(|| {
        jsonwebtoken::crypto::sign(
            b"resolve",
            &EncodingKey::from_secret(b"resolve"),
            Algorithm::HS256,
        )
        .unwrap();
    });
}

/// The default here is aws-lc: it signs ES256, which server-core's provider would refuse.
#[test]
fn the_process_default_is_aws_lc() {
    setup();
    use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    let pkcs8 = EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING)
        .unwrap()
        .to_pkcs8v1()
        .unwrap();
    let key = EncodingKey::from_ec_der(pkcs8.as_ref());
    jsonwebtoken::encode(
        &Header::new(Algorithm::ES256),
        &serde_json::json!({"iss": "TEAMID1234"}),
        &key,
    )
    .unwrap();
}
