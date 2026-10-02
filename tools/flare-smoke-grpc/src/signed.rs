//! 进程内签名的 Cell 内部上下文（`x-flare-ctx`）。
//!
//! 本工具发出的每个 gRPC 请求都只能由 [`signed_request`] 构造，而它只接受
//! [`SignedToken`]；`SignedToken` 只能由 [`ContextSigner::sign_user`] 产生。
//! 因此不存在未签名的调用路径，也不发送任何旧的身份元数据。

use std::fmt;
use std::path::Path;

use crate::error::{Result, err};
use flare_server_core::auth::{SignedActorType, SignedContextIdentity, SignedContextSigner};
use flare_server_core::grpc::client::set_signed_context_token;

use crate::secret_file::{MAX_KEY_FILE, read_private_file};

/// 持有 Cell 私钥的签名器。私钥只在本进程内存里，不打印、不落盘。
pub struct ContextSigner(SignedContextSigner);

/// 已签名的内部上下文令牌；Debug 不显示内容。
pub struct SignedToken(String);

impl fmt::Debug for SignedToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SignedToken([redacted])")
    }
}

impl ContextSigner {
    /// 从 600 文件读取 Ed25519 私钥（PKCS#8 PEM、base64 PEM 或 base64 DER，
    /// 即 flarectl 写进 core.env 的 `FLARE_INTERNAL_CTX_PRIVATE_KEY` 的值）。
    pub fn load(path: &Path) -> Result<Self> {
        let material = read_private_file(path, "signing key", MAX_KEY_FILE)?;
        let signer = SignedContextSigner::from_private_key(material.trim()).map_err(|_| {
            err!(
                "signing key file {} does not hold the Cell Ed25519 private key \
                 (PKCS#8 PEM, base64 PEM or base64 DER)",
                path.display()
            )
        })?;
        Ok(Self(signer))
    }

    #[cfg(test)]
    pub fn from_material(material: &str) -> Result<Self> {
        Ok(Self(SignedContextSigner::from_private_key(material)?))
    }

    /// 以终端用户身份签名（与 api-gateway 发送时签的身份相同：actor_type=user）。
    /// 有效期 60 s，每次调用前现签。
    pub fn sign_user(&self, tenant_id: &str, user_id: &str) -> Result<SignedToken> {
        let identity = SignedContextIdentity::new(
            SignedActorType::User,
            user_id,
            tenant_id,
            uuid::Uuid::new_v4().to_string(),
        );
        self.0
            .sign(&identity)
            .map(SignedToken)
            .map_err(|_| err!("signing the internal context failed"))
    }
}

/// 唯一的请求构造入口：负载 + 已签名上下文。只写 `x-flare-ctx` 一项元数据。
pub fn signed_request<T>(payload: T, token: &SignedToken) -> Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(payload);
    set_signed_context_token(&mut request, &token.0)
        .map_err(|_| err!("signed context token is not valid metadata"))?;
    Ok(request)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::SignedContextVerifier;
    use flare_server_core::grpc::middleware::signed_context::SIGNED_CONTEXT_HEADER;

    pub(crate) fn key_pair(seed: u8) -> (String, String) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap().to_string();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (private, public)
    }

    /// 旧的未签名身份元数据键，测试里拼出来，避免在源码里出现字面量。
    pub(crate) fn legacy_identity_keys() -> Vec<String> {
        [
            "tenant-id",
            "user-id",
            "actor-id",
            "actor-type",
            "actor-roles",
        ]
        .iter()
        .map(|suffix| format!("x-{suffix}"))
        .collect()
    }

    #[test]
    fn signed_request_carries_only_a_verifiable_user_context() {
        let (private, public) = key_pair(7);
        let signer = ContextSigner::from_material(&private).unwrap();
        let token = signer
            .sign_user("e2e-0123456789abcdef0123", "7312345678901234567")
            .unwrap();
        assert_eq!(format!("{token:?}"), "SignedToken([redacted])");

        let request = signed_request((), &token).unwrap();
        let values: Vec<_> = request
            .metadata()
            .get_all(SIGNED_CONTEXT_HEADER)
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect();
        assert_eq!(values.len(), 1, "exactly one signed context");
        assert_eq!(request.metadata().len(), 1, "no other metadata");
        for key in legacy_identity_keys() {
            assert!(request.metadata().get(key.as_str()).is_none(), "{key}");
        }

        let claims = SignedContextVerifier::from_public_key(&public)
            .unwrap()
            .verify(&values[0])
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::User);
        assert_eq!(claims.actor_id, "7312345678901234567");
        assert_eq!(claims.sub, "7312345678901234567");
        assert_eq!(claims.tenant_id, "e2e-0123456789abcdef0123");
        assert!(claims.roles.is_empty());
        assert!(claims.exp - claims.iat <= 60);

        let (_, other_public) = key_pair(8);
        assert!(
            SignedContextVerifier::from_public_key(&other_public)
                .unwrap()
                .verify(&values[0])
                .is_err(),
            "a different Cell key must not verify"
        );
    }

    #[test]
    fn signer_needs_a_private_key_file() {
        let dir = std::env::temp_dir().join(format!("flare-smoke-key-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let missing = ContextSigner::load(&dir.join("absent")).err().unwrap();
        assert!(missing.to_string().contains("missing"), "{missing}");

        let (private, public) = key_pair(9);
        let write = |name: &str, text: &str, mode: u32| {
            use std::os::unix::fs::PermissionsExt;
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            path
        };
        let public_only = ContextSigner::load(&write("public", &public, 0o600))
            .err()
            .unwrap()
            .to_string();
        assert!(public_only.contains("Ed25519 private key"), "{public_only}");
        let open = ContextSigner::load(&write("open", &private, 0o644))
            .err()
            .unwrap()
            .to_string();
        assert!(open.contains("chmod 600"), "{open}");
        assert!(!open.contains("PRIVATE KEY"), "{open}");
        assert!(ContextSigner::load(&write("pem", &format!("{private}\n"), 0o600)).is_ok());
        // flarectl 写进 core.env 的是 base64 编码的 PEM。
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&private);
        assert!(ContextSigner::load(&write("b64", &format!("{encoded}\n"), 0o600)).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
