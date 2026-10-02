//! Cell 签名上下文（`x-flare-ctx`）的验签公钥：`FLARE_INTERNAL_CTX_PUBLIC_KEY`。

use flare_server_core::SignedContextVerifier;
use flare_server_core::error::{FlareError, Result};

/// 读验签公钥。没有配置时告警并返回 `None`，`consumer` 的接口据此一律拒绝（fail closed）；
/// 配置了但解析不了时启动失败。
pub(crate) fn internal_ctx_verifier_from_env(
    consumer: &str,
) -> Result<Option<SignedContextVerifier>> {
    match std::env::var("FLARE_INTERNAL_CTX_PUBLIC_KEY") {
        Ok(key) => SignedContextVerifier::from_public_key(&key)
            .map(Some)
            .map_err(|_| {
                FlareError::system(format!(
                    "invalid FLARE_INTERNAL_CTX_PUBLIC_KEY for {consumer}"
                ))
            }),
        Err(std::env::VarError::NotPresent) => {
            tracing::warn!("{consumer} disabled: FLARE_INTERNAL_CTX_PUBLIC_KEY missing");
            Ok(None)
        }
        Err(std::env::VarError::NotUnicode(_)) => Err(FlareError::system(
            "invalid FLARE_INTERNAL_CTX_PUBLIC_KEY encoding",
        )),
    }
}
