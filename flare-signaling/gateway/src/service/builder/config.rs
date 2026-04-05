//! 配置解析和设置

use flare_core::common::compression::CompressionAlgorithm;

/// 加密配置
pub struct EncryptionConfig {
    pub enabled: bool,
}

/// 解析压缩算法
pub fn parse_compression_algorithm(algorithm: Option<&str>) -> CompressionAlgorithm {
    let result = match algorithm {
        Some("gzip") => CompressionAlgorithm::Gzip,
        Some("zstd") => CompressionAlgorithm::Zstd,
        Some("none") | Some("") | None => CompressionAlgorithm::None,
        Some(other) => {
            tracing::warn!(algorithm = %other, "Unknown compression algorithm, using None");
            CompressionAlgorithm::None
        }
    };

    tracing::debug!(algorithm = ?algorithm, parsed = ?result, "Parsed compression algorithm");
    result
}

/// 配置加密（如果启用）
pub async fn setup_encryption_config(
    enable_encryption: bool,
    encryption_key: Option<&str>,
) -> EncryptionConfig {
    if !enable_encryption {
        return EncryptionConfig { enabled: false };
    }

    use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionUtil};
    use tracing::{info, warn};

    // 解析加密密钥（32字节）
    let key_bytes = encryption_key.and_then(|key| {
        if key.len() == 32 {
            // 直接32字符的字符串
            Some(key.as_bytes().to_vec())
        } else if key.len() == 64 {
            // hex 编码的 64 字符字符串（32字节）
            (0..32)
                .try_fold(Vec::new(), |mut acc, i| {
                    u8::from_str_radix(&key[i * 2..i * 2 + 2], 16).map(|b| {
                        acc.push(b);
                        acc
                    })
                })
                .ok()
        } else {
            None
        }
    });

    let encryption_key = key_bytes.unwrap_or_else(|| {
        warn!("Encryption key not set or invalid (expected 32 bytes or 64 hex chars), using default key (NOT SECURE FOR PRODUCTION)");
        b"01234567890123456789012345678901".to_vec() // 32 bytes for AES-256
    });

    match Aes256GcmEncryptor::new(&encryption_key) {
        Ok(encryptor) => {
            EncryptionUtil::register_custom(std::sync::Arc::new(encryptor));
            info!("🔐 AES-256-GCM encryption enabled with custom key");
            EncryptionConfig { enabled: true }
        }
        Err(e) => {
            warn!(error = %e, "Failed to initialize encryption, disabled");
            EncryptionConfig { enabled: false }
        }
    }
}
