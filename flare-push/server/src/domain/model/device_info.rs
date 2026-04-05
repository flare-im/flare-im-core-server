//! 设备信息值对象
//!
//! 表示推送目标设备的详细信息。

/// 设备信息
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    /// 设备 ID
    pub device_id: String,
    /// 用户 ID
    pub user_id: String,
    /// 设备平台（ios, android, web, desktop）
    pub platform: String,
    /// 推送令牌（APNs Device Token 或 FCM Registration Token）
    pub push_token: Option<String>,
}

impl DeviceInfo {
    /// 创建新的设备信息
    pub fn new(
        device_id: String,
        user_id: String,
        platform: String,
        push_token: Option<String>,
    ) -> Self {
        Self {
            device_id,
            user_id,
            platform,
            push_token,
        }
    }

    /// 检查是否有推送令牌
    pub fn has_push_token(&self) -> bool {
        self.push_token.is_some()
    }
}
