//! 上行 EVENT 通道处理结果（领域层）
//!
//! 与 `common/event.proto` 对齐：领域事件 → `OperationResponse`，接口层封装为 `Ack(EventAck)` 下行。
//! 同步控制面走 DATA（`DataPacket` / `sync.proto`），不再经 EVENT 通道。

use flare_proto::common::OperationResponse;

/// 客户端经 PayloadCommand(EVENT) 上行后的领域结果
#[derive(Debug, Clone)]
pub enum EventUplinkOutcome {
    /// 下行 ACK 通道 EventAck（`ack.proto`）
    Operation {
        /// 与上行 `Event.event_id` 对齐，供客户端关联
        event_id: String,
        operation: OperationResponse,
    },
}
