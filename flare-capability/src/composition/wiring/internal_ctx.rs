//! Cell 签名上下文（`x-flare-ctx`）的验签公钥集：`FLARE_INTERNAL_CTX_PUBLIC_KEYS` 叠加在
//! `FLARE_INTERNAL_CTX_PUBLIC_KEY` 之上（见 flare-server-core `KeySet`）。

use flare_server_core::SignedContextVerifier;
use flare_server_core::error::{FlareError, Result};

/// 读验签公钥集。没有配置时告警并返回 `None`，`consumer` 的接口据此一律拒绝（fail closed）；
/// 配置了但解析不了时启动失败。
pub(crate) fn internal_ctx_verifier_from_env(
    consumer: &str,
) -> Result<Option<SignedContextVerifier>> {
    match SignedContextVerifier::from_env_optional() {
        Ok(Some(verifier)) => Ok(Some(verifier)),
        Ok(None) => {
            tracing::warn!("{consumer} disabled: FLARE_INTERNAL_CTX_PUBLIC_KEY(S) missing");
            Ok(None)
        }
        Err(error) => Err(FlareError::system(format!(
            "invalid Cell public key set for {consumer}: {error}"
        ))),
    }
}

/// 管理 RPC 的验签器：同一公钥集，Staff 钉死 `kid=gateway`。没有配置时告警并返回 `None`。
pub(crate) fn admin_staff_verifier_from_env(
    consumer: &str,
) -> Result<Option<SignedContextVerifier>> {
    if internal_ctx_verifier_from_env(consumer)?.is_none() {
        return Ok(None);
    }
    flare_im_service_kit::staff_context::admin_staff_verifier_from_env(consumer).map(Some)
}
