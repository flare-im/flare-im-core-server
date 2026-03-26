//! 推送结果值对象

/// 单用户推送结果
#[derive(Debug, Clone)]
pub struct DomainPushResult {
    pub user_id: String,
    pub success_count: i32,
    pub failure_count: i32,
    pub error_message: String,
}
