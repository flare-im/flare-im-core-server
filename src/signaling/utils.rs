//! 信令服务工具函数（原 flare-signaling/common）

use chrono::Utc;

/// 获取当前时间戳 (毫秒)
pub fn current_timestamp_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// 生成唯一ID
pub fn generate_id(prefix: &str) -> String {
    format!("{}_{}", prefix, ulid::Ulid::new())
}

/// 验证用户ID有效性
pub fn validate_user_id(user_id: &str) -> Result<(), String> {
    if user_id.is_empty() {
        return Err("User ID cannot be empty".to_string());
    }
    if user_id.len() > 255 {
        return Err("User ID too long (max 255 characters)".to_string());
    }
    Ok(())
}

/// 验证网关ID有效性
pub fn validate_gateway_id(gateway_id: &str) -> Result<(), String> {
    if gateway_id.is_empty() {
        return Err("Gateway ID cannot be empty".to_string());
    }
    if gateway_id.len() > 255 {
        return Err("Gateway ID too long (max 255 characters)".to_string());
    }
    Ok(())
}
