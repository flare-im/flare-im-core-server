//! 可见性存储仓储接口定义（Port）
//!
//! 负责消息的可见性状态管理（如软删除、隐藏等）

use anyhow::Result;
use flare_proto::common::VisibilityStatus;

// Rust 2024: trait 中直接使用 async fn
// 注意：对于 trait 对象（dyn Trait），仍需要使用 async_trait
#[async_trait::async_trait]
pub trait VisibilityStorage: Send + Sync {
    async fn set_visibility(
        &self,
        message_id: &str,
        user_id: &str,
        conversation_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<()>;

    async fn get_visibility(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<VisibilityStatus>>;

    async fn batch_set_visibility(
        &self,
        message_ids: &[String],
        user_id: &str,
        conversation_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<usize>;

    async fn query_visible_message_ids(
        &self,
        user_id: &str,
        conversation_id: &str,
        visibility_status: VisibilityStatus,
    ) -> Result<Vec<String>>;
}