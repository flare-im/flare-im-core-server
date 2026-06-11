//! 消息发送领域服务
//!
//! 负责上行 MESSAGE 的业务编排：解析连接上下文 → 调用 MessageCommandPort 发送 → 返回 SendAck(server_msg_id, seq)。

use std::sync::Arc;

use flare_core::common::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use flare_im_contracts::Ctx;
use flare_proto::common::send_ack;
use tracing::instrument;

use crate::application::commands::SendMessageCommand;
use crate::domain::model::MessageSendOutcome;
use crate::domain::ports::IMessageCommandPort;

/// 消息发送领域服务
///
/// 依赖 ConnectionContextResolver 解析连接上下文，MessageCommandPort 执行实际发送。
pub struct SendMessageDomainService {
    message_port: Arc<dyn IMessageCommandPort>,
}

impl SendMessageDomainService {
    pub fn new(message_port: Arc<dyn IMessageCommandPort>) -> Self {
        Self { message_port }
    }

    /// 处理发送消息：解析上下文 → 调用端口发送 → 返回发送结果。
    #[instrument(skip(self, tx, cmd), fields(connection_id = %cmd.connection_id))]
    pub async fn execute(&self, tx: &Ctx, cmd: &SendMessageCommand) -> Result<MessageSendOutcome> {
        let ack = self
            .message_port
            .send_message(tx, cmd.msg.clone())
            .await
            .map_err(from_server_error)?;

        match ack.result {
            Some(send_ack::Result::Accepted(accepted)) => {
                let durability = accepted.durability();
                Ok(MessageSendOutcome {
                    server_msg_id: accepted.server_msg_id,
                    conversation_seq: accepted.conversation_seq,
                    durability,
                })
            }
            Some(send_ack::Result::Error(error)) => Err(FlareError::message_send_failed(
                if error.message.trim().is_empty() {
                    "message send failed".to_string()
                } else {
                    error.message
                },
            )),
            None => Err(FlareError::message_send_failed(
                "message send failed: empty SendAck.result".to_string(),
            )),
        }
    }
}

fn from_server_error(err: flare_server_core::error::FlareError) -> FlareError {
    match err {
        flare_server_core::error::FlareError::Localized {
            code,
            reason,
            details,
            ..
        } => {
            let code = ErrorCode::from_u32(code.as_u32()).unwrap_or(ErrorCode::GeneralError);
            let mut builder = ErrorBuilder::new(code, reason);
            if let Some(details) = details {
                builder = builder.details(details);
            }
            builder.build()
        }
        flare_server_core::error::FlareError::System(msg) => FlareError::system(msg),
        flare_server_core::error::FlareError::Io(msg) => FlareError::io(msg),
    }
}
