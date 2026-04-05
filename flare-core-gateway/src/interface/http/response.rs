// 重导出 flare-server-core 的 ApiResponse
pub use flare_server_core::http::ApiResponse;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 错误响应
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    /// 错误码
    pub code: i32,
    /// 错误原因
    pub reason: String,
    /// 错误消息
    pub message: String,
}

// ============================================================================
// MediaService HTTP 请求模型
// ============================================================================

/// 生成上传 URL 请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct GenerateUploadUrlHttpRequest {
    /// 桶名
    pub bucket: String,
    /// 对象键
    pub object_key: String,
    /// MIME 类型
    pub mime_type: String,
    /// 期望大小
    pub expected_size: i64,
    /// 有效期秒数
    #[serde(default = "default_expires_in")]
    pub expires_in: i32,
}

fn default_expires_in() -> i32 {
    3600
}

/// 生成上传 URL 响应
#[derive(Debug, Serialize, ToSchema)]
pub struct GenerateUploadUrlHttpResponse {
    /// 上传 URL
    pub upload_url: String,
    /// 对象键
    pub object_key: String,
}

/// 获取文件 URL 请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct GetFileUrlHttpRequest {
    /// 文件 ID
    pub file_id: String,
    /// 有效期秒数
    #[serde(default = "default_expires_in")]
    pub expires_in: i32,
    /// 是否下载
    #[serde(default)]
    pub download: bool,
}

/// 获取文件 URL 响应
#[derive(Debug, Serialize, ToSchema)]
pub struct GetFileUrlHttpResponse {
    /// 访问 URL
    pub url: String,
    /// CDN URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
}

/// 获取文件信息请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct GetFileInfoHttpRequest {
    /// 文件 ID
    pub file_id: String,
}

/// 文件信息响应
#[derive(Debug, Serialize, ToSchema)]
pub struct FileInfoHttpResponse {
    /// 文件 ID
    pub file_id: String,
    /// 文件名
    pub file_name: String,
    /// MIME 类型
    pub mime_type: String,
    /// 大小
    pub size: i64,
    /// 访问 URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// CDN URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
}

/// 删除文件请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct DeleteFileHttpRequest {
    /// 文件 ID
    pub file_id: String,
    /// 是否硬删除
    #[serde(default)]
    pub hard_delete: bool,
}

/// 删除文件响应
#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteFileHttpResponse {
    /// 是否成功
    pub success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_success() {
        let response = ApiResponse::success("test data".to_string());
        assert!(response.success);
        assert_eq!(response.data, Some("test data".to_string()));
        assert!(response.error.is_none());
    }

    #[test]
    fn test_api_response_error() {
        let response: ApiResponse<String> = ApiResponse::error("TEST_ERROR", "test message");
        assert!(!response.success);
        assert!(response.data.is_none());
        assert_eq!(response.error.unwrap().code, "TEST_ERROR");
    }
}

// ============================================================================
// MessageService HTTP 请求模型
// ============================================================================

/// 发送消息请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct SendMessageHttpRequest {
    /// 会话 ID
    pub conversation_id: String,
    /// 消息内容(JSON)
    pub content: serde_json::Value,
    /// 消息类型
    pub message_type: i32,
}

/// 发送消息响应
#[derive(Debug, Serialize, ToSchema)]
pub struct SendMessageHttpResponse {
    /// 服务端消息 ID
    pub server_msg_id: String,
    /// 序号
    pub seq: u64,
    /// 是否成功
    pub success: bool,
}

/// 撤回消息请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct RecallMessageHttpRequest {
    /// 会话 ID
    pub conversation_id: String,
    /// 消息 ID
    pub message_id: String,
}

/// 撤回消息响应
#[derive(Debug, Serialize, ToSchema)]
pub struct RecallMessageHttpResponse {
    /// 是否成功
    pub success: bool,
}

/// 标记已读请求
#[derive(Debug, Deserialize, ToSchema)]
pub struct MarkReadHttpRequest {
    /// 会话 ID
    pub conversation_id: String,
    /// 消息 ID
    pub message_id: String,
}

/// 标记已读响应
#[derive(Debug, Serialize, ToSchema)]
pub struct MarkReadHttpResponse {
    /// 是否成功
    pub success: bool,
}
