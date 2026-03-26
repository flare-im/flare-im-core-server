//! 上行处理器：委托四个领域服务处理 MESSAGE / EVENT / DATA / ACK。

use std::sync::Arc;

use crate::application::commands::{
    SendAckCommand, SendDataCommand, SendEventCommand, SendMessageCommand,
};
use crate::domain::model::EventUplinkOutcome;
use crate::domain::service::{
    SendAckDomainService, SendDataDomainService, SendEventDomainService, SendMessageDomainService,
};
use flare_core::common::error::Result;
use crate::domain::ports::IContextResolver;

/// 上行处理器：持有四条上行线的领域服务，委托执行。
pub struct SendHandler {
    send_message_service: Arc<SendMessageDomainService>,
    send_event_service: Arc<SendEventDomainService>,
    send_data_service: Arc<SendDataDomainService>,
    send_ack_service: Arc<SendAckDomainService>,
    context_resolver: Arc<dyn IContextResolver>,
}

impl SendHandler {
    pub fn new(
        send_message_service: Arc<SendMessageDomainService>,
        send_event_service: Arc<SendEventDomainService>,
        send_data_service: Arc<SendDataDomainService>,
        send_ack_service: Arc<SendAckDomainService>,
        context_resolver: Arc<dyn IContextResolver>,
    ) -> Self {
        Self {
            send_message_service,
            send_event_service,
            send_data_service,
            send_ack_service,
            context_resolver,
        }
    }

    /// 处理发送消息 → 委托 SendMessageDomainService
    pub async fn handle_send_message(
        &self,
        command: &SendMessageCommand,
    ) -> Result<(String, u64)> {
        let ctx = self.context_resolver.resolve(&command.connection_id).await?;
        self.send_message_service.execute(&ctx, command).await
    }

    /// 处理发送事件 → 委托 `SendEventDomainService`（仅业务事件）
    pub async fn handle_send_event(
        &self,
        command: &SendEventCommand,
    ) -> Result<EventUplinkOutcome> {
        let ctx = self.context_resolver.resolve(&command.connection_id).await?;
        self.send_event_service.execute(&ctx, command).await
    }

    /// 处理发送数据，返回可选响应 payload（供 DATA 通道回包）→ 委托 SendDataDomainService
    pub async fn handle_send_data(
        &self,
        command: &SendDataCommand,
    ) -> Result<Option<Vec<u8>>> {
        let ctx = self.context_resolver.resolve(&command.connection_id).await?;
        self.send_data_service.execute(&ctx, command).await
    }

    /// 处理发送 ack（PushAck/ConversationAck 等上报）→ 委托 SendAckDomainService
    pub async fn handle_send_ack(&self, command: &SendAckCommand) -> Result<()> {
        let ctx = self.context_resolver.resolve(&command.connection_id).await?;
        self.send_ack_service.execute(&ctx, command).await
    }
}
