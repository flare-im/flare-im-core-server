//! 连接查询端口（读侧）：按用户解析当前网关上的长连接列表
//!
//! 与 [`crate::domain::ports::IConnectionPort`] 分离：前者专注「谁在连」，后者专注登录/心跳等在线 RPC。

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

use crate::domain::model::ConnectionInfo;

#[async_trait]
pub trait ConnectionQuery: Send + Sync {
    /// 查询用户当前在本网关上的连接（含设备/平台等读模型字段）
    async fn query_user_connections(&self, tx: &Ctx, user_id: &str) -> Result<Vec<ConnectionInfo>>;

    /// 与 [`Self::query_user_connections`] 等价读路径；部分调用方无 `Ctx` 时使用。
    async fn list_user_connections(&self, user_id: &str) -> Result<Vec<ConnectionInfo>>;

    /// 只取连接 id，**不组装读模型**。
    ///
    /// 会话订阅那条路径（`ensure_conversation_members_subscribed`）只用 id，
    /// 但走 [`Self::list_user_connections`] 会为每个连接额外做一次
    /// `get_connection().await` 去拼设备/平台等字段——那些字段随后被直接丢弃。
    ///
    /// 这在大群里是实打实的开销：该路径**每条消息都会对全部参与者跑一遍**，
    /// 万人群就是每条消息上万次无用的读模型组装。
    ///
    /// 默认实现回落到完整路径并丢掉多余字段，保证既有实现方不受影响；
    /// 真实实现应当覆写它，直接读连接表。
    async fn list_user_connection_ids(&self, user_id: &str) -> Result<Vec<String>> {
        Ok(self
            .list_user_connections(user_id)
            .await?
            .into_iter()
            .map(|info| info.connection_id)
            .collect())
    }
}
