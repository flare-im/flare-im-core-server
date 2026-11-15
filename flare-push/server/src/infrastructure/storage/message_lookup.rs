//! 按会话与 seq 读回一条消息：经 Storage Reader 的按序号查询。
//!
//! 只有补提示用：交接里只有会话与 seq，要渲染一条正常的离线任务得把消息读回来。
//! 不直连消息库，理由与通知偏好不直连会话库相同。

use std::sync::Arc;
use std::time::Duration;

use flare_grpc_proto::storage::QueryMessagesBySeqRequest;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{STORAGE_READER, get_service_name};
use flare_proto::common::Message;
use flare_server_core::error::{ErrorCode, FlareError};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::repository::MessageLookupRepository;
use crate::infrastructure::read_request::request_with_context;

/// 一次读取的时限。补提示在等它：读不回来就用通用文案，不值得等更久。
const READ_TIMEOUT: Duration = Duration::from_secs(3);

fn unavailable(reason: impl Into<String>) -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, reason)
}

/// 只取 seq 这一条的查询窗口 `(seq - 1, seq + 1)`；seq 为 0 或放不进有符号数时没有这样的窗口。
fn single_seq_window(seq: u64) -> Option<(i64, i64)> {
    let seq = i64::try_from(seq).ok().filter(|seq| *seq > 0)?;
    Some((seq - 1, seq.checked_add(1)?))
}

/// 窗口里取回的消息是否就是要的那条。Storage Reader 不填 seq 的消息无从核对，按窗口为准。
fn is_message_at(message: &Message, seq: u64) -> bool {
    message.conversation_seq == 0 || message.conversation_seq == seq
}

#[derive(Default)]
pub struct StorageMessageLookup {
    channel: Arc<Mutex<Option<Channel>>>,
}

impl StorageMessageLookup {
    pub fn new() -> Self {
        Self::default()
    }

    async fn client(&self) -> Result<StorageReaderServiceClient<Channel>, FlareError> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = guard.as_ref() {
            return Ok(StorageReaderServiceClient::new(channel.clone()));
        }
        let name = get_service_name(STORAGE_READER);
        let fallback = flare_im_service_kit::discovery::default_static_grpc_fallback(&name);
        let channel =
            flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(&name, fallback)
                .await
                .map_err(|e| unavailable(format!("connect {name}: {e}")))?;
        *guard = Some(channel.clone());
        Ok(StorageReaderServiceClient::new(channel))
    }
}

#[async_trait::async_trait]
impl MessageLookupRepository for StorageMessageLookup {
    async fn message_at(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        seq: u64,
        user_id: &str,
    ) -> Result<Option<Message>, FlareError> {
        let Some((after_seq, before_seq)) = single_seq_window(seq) else {
            return Ok(None);
        };
        if conversation_id.trim().is_empty() {
            return Ok(None);
        }
        let mut client = self.client().await?;
        let request = request_with_context(
            QueryMessagesBySeqRequest {
                conversation_id: conversation_id.to_string(),
                after_seq,
                before_seq,
                limit: 1,
                // 按这个用户的可见性读：入群前的、为自己删除的消息不该出现在他的通知里。
                user_id: user_id.to_string(),
                include_burned_placeholder: false,
            },
            ctx,
        );
        let response = tokio::time::timeout(READ_TIMEOUT, client.query_messages_by_seq(request))
            .await
            .map_err(|_| unavailable("storage reader did not answer in time"))?
            .map_err(|status| unavailable(format!("query message by seq: {:?}", status.code())))?
            .into_inner();
        Ok(response
            .messages
            .into_iter()
            .find(|message| is_message_at(message, seq)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_selects_exactly_one_seq() {
        assert_eq!(single_seq_window(1), Some((0, 2)));
        assert_eq!(single_seq_window(42), Some((41, 43)));
        assert_eq!(single_seq_window(0), None, "seq 0 不指向任何消息");
        assert_eq!(single_seq_window(u64::MAX), None);
        assert_eq!(single_seq_window(i64::MAX as u64), None, "上界加一会溢出");
    }

    #[test]
    fn a_message_with_another_seq_is_not_the_one_asked_for() {
        let at = |seq| Message {
            conversation_seq: seq,
            ..Default::default()
        };
        assert!(is_message_at(&at(7), 7));
        assert!(is_message_at(&at(0), 7));
        assert!(!is_message_at(&at(8), 7));
    }
}
