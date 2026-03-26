//! 连接查询端口（读侧）：按用户解析当前网关上的长连接列表
//!
//! 与 [`crate::domain::ports::IConnectionPort`] 分离：前者专注「谁在连」，后者专注登录/心跳等在线 RPC。

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_server_core::error::Result;

use crate::domain::model::ConnectionInfo;

#[async_trait]
pub trait ConnectionQuery: Send + Sync {
    /// 查询用户当前在本网关上的连接（含设备/平台等读模型字段）
    async fn query_user_connections(&self, tx: &Ctx, user_id: &str) -> Result<Vec<ConnectionInfo>>;

    /// 与 [`Self::query_user_connections`] 等价读路径；部分调用方无 `Ctx` 时使用。
    async fn list_user_connections(&self, user_id: &str) -> Result<Vec<ConnectionInfo>>;
}
