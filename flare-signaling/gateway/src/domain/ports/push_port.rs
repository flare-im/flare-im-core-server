//! 下行推送端口（Port）——**网关 → 客户端** 的统一写模型
//!
//! ## 职责边界（生产约定）
//!
//! - **经 [`IPushPort`] 下发的流量**：服务端主动下行、AccessGateway gRPC 批量推送、以及任何需与
//!   [`flare_core::server::handle::ServerHandle`] 对齐的发送路径（与 `PayloadCommand` 帧格式一致）。
//! - **长连接请求-响应帧**：`ServerEventHandler` 对 **MESSAGE** 等仍通过返回 [`Frame`](flare_core::common::protocol::Frame)
//!   回包；flare-core 在返回 [`None`] 时会发送**通用自动 ACK**，**不能**用「仅 `IPushPort` + 返回 `None`」
//!   代替 EVENT 的 `EventAck` 或 DATA 的 `DataPacket` 回包（见 `flare-core` `ServerMessageWrapper`）。
//! - **推荐**：多目标、业务侧发起的下行一律走本 Port + [`crate::domain::service::PushDomainService`]；
//!   同步回包仍使用 `interface` 层组帧（内部载荷编码与 `flare_proto::common` 一致）。
//!
//! ## Payload 类型（与 `PayloadCommand.type` / `flare_core` 一致）
//!
//! | `i32` | 含义 |
//! |-------|------|
//! | `1` | Message |
//! | `2` | Event |
//! | `3` | Ack（如 [`flare_proto::common::Ack`] 序列化体） |
//! | `4` | Data（[`flare_proto::common::DataPacket`]：`sync_request` / `sync_response` / `user_custom`） |
//!
//! 具体数值以 `flare_core::common::protocol::payload_command::Type` 为准。

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

/// 推送端口：仅描述「如何把载荷送到连接/用户」，由基础设施适配 `ServerHandle`。
#[async_trait]
pub trait IPushPort: Send + Sync {
    /// 按 **Message** 语义推送 protobuf `Message` 的 **已编码字节**（`type`=Message）
    async fn push_message_to_user(&self, tx: &Ctx, user_id: &str, message: Vec<u8>) -> Result<()>;

    async fn push_message_to_connection(
        &self,
        tx: &Ctx,
        connection_id: &str,
        message: Vec<u8>,
    ) -> Result<()>;

    /// 按给定 `payload_type` 向指定连接推送**已编码**载荷（与上表一致）
    async fn push_payload_to_connection(
        &self,
        tx: &Ctx,
        connection_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn push_payload_to_user(
        &self,
        tx: &Ctx,
        user_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()>;

    /// 向多个连接广播同一载荷，返回 `(成功数, 失败数)`（去重 `connection_id`）
    async fn push_payload_to_connections(
        &self,
        tx: &Ctx,
        connection_ids: &[String],
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<(i32, i32)>;
}
