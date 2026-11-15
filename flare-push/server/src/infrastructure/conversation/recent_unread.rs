//! 网关失联的补提示范围：经会话服务的内部读接口，取某个用户在窗口内有新未读的会话。
//!
//! 「仍在会话里、最后一条消息晚于窗口起点、不是本人发的、还有未读」由会话服务按它自己的
//! 未读口径筛好，这里只换形状。以服务身份读：交接是逐条重新背书成本服务身份后消费的，
//! 没有用户在场，也不冒用用户身份。

use std::sync::Arc;
use std::time::Duration;

use flare_grpc_proto::conversation::conversation_internal_read_service_client::ConversationInternalReadServiceClient;
use flare_grpc_proto::conversation::{
    ListRecentUnreadConversationsRequest, RecentUnreadConversation,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, get_service_name};
use flare_server_core::error::{ErrorCode, FlareError};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::repository::{RecentConversation, RecentConversationRepository};
use crate::infrastructure::read_request::request_with_context;

/// 一次读取的时限。整条交接在等它：超时按暂时不可用报，由 broker 重投。
const READ_TIMEOUT: Duration = Duration::from_secs(3);

fn unavailable(reason: impl Into<String>) -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, reason)
}

/// 会话服务的拒绝换成错误码。被拒绝（身份不被接受、参数不合法、接口不存在）重投多少遍
/// 结果都一样，报不可重试的错误；其余一律按暂时不可用，可重试。
fn read_failure(status: &tonic::Status) -> FlareError {
    let reason = format!("list recent unread conversations: {:?}", status.code());
    let code = match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => ErrorCode::PermissionDenied,
        tonic::Code::InvalidArgument => ErrorCode::InvalidParameter,
        tonic::Code::Unimplemented => ErrorCode::OperationNotSupported,
        _ => ErrorCode::ServiceUnavailable,
    };
    FlareError::localized(code, reason)
}

/// 请求里的条数上限。会话服务自己还有上限（单次至多 20 个），这里只保证数值放得进字段。
fn wire_limit(limit: usize) -> i32 {
    i32::try_from(limit).unwrap_or(i32::MAX)
}

fn recent_conversation(row: RecentUnreadConversation) -> RecentConversation {
    RecentConversation {
        conversation_id: row.conversation_id,
        last_seq: row.last_message_seq,
        last_message_id: row.last_message_id,
        last_sender_id: row.last_sender_id,
        last_message_at: row.last_message_time,
        unread: row.unread_count,
    }
}

#[derive(Default)]
pub struct ConversationRecentUnread {
    channel: Arc<Mutex<Option<Channel>>>,
}

impl ConversationRecentUnread {
    pub fn new() -> Self {
        Self::default()
    }

    async fn client(&self) -> Result<ConversationInternalReadServiceClient<Channel>, FlareError> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = guard.as_ref() {
            return Ok(ConversationInternalReadServiceClient::new(channel.clone()));
        }
        let name = get_service_name(CONVERSATION);
        let fallback = flare_im_service_kit::discovery::default_static_grpc_fallback(&name);
        let channel =
            flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(&name, fallback)
                .await
                .map_err(|e| unavailable(format!("connect {name}: {e}")))?;
        *guard = Some(channel.clone());
        Ok(ConversationInternalReadServiceClient::new(channel))
    }
}

#[async_trait::async_trait]
impl RecentConversationRepository for ConversationRecentUnread {
    async fn recent_conversations(
        &self,
        ctx: &Ctx,
        user_id: &str,
        since: i64,
        limit: usize,
    ) -> Result<Vec<RecentConversation>, FlareError> {
        let mut client = self.client().await?;
        // 租户不在请求体里：会话服务只认签名上下文里的租户。
        let request = request_with_context(
            ListRecentUnreadConversationsRequest {
                user_id: user_id.to_string(),
                since,
                limit: wire_limit(limit),
            },
            ctx,
        );
        let response = tokio::time::timeout(
            READ_TIMEOUT,
            client.list_recent_unread_conversations(request),
        )
        .await
        .map_err(|_| unavailable("conversation service did not answer in time"))?
        .map_err(|status| read_failure(&status))?
        .into_inner();
        Ok(response
            .conversations
            .into_iter()
            .map(recent_conversation)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wire_row_maps_field_by_field() {
        let mapped = recent_conversation(RecentUnreadConversation {
            conversation_id: "conv-1".to_string(),
            last_message_seq: 42,
            last_message_id: "m-42".to_string(),
            last_message_time: 1_760_000_000_123,
            last_sender_id: "bob".to_string(),
            unread_count: 3,
        });
        assert_eq!(
            mapped,
            RecentConversation {
                conversation_id: "conv-1".to_string(),
                last_seq: 42,
                last_message_id: "m-42".to_string(),
                last_sender_id: "bob".to_string(),
                last_message_at: 1_760_000_000_123,
                unread: 3,
            }
        );
    }

    #[test]
    fn the_limit_always_fits_the_wire_field() {
        assert_eq!(wire_limit(0), 0);
        assert_eq!(wire_limit(20), 20);
        assert_eq!(wire_limit(usize::MAX), i32::MAX);
    }

    /// 被拒绝的读取不值得重投；会话服务暂时答不上来的才重投。
    #[test]
    fn only_transient_failures_are_retryable() {
        for (code, expected) in [
            (tonic::Code::Unauthenticated, ErrorCode::PermissionDenied),
            (tonic::Code::PermissionDenied, ErrorCode::PermissionDenied),
            (tonic::Code::InvalidArgument, ErrorCode::InvalidParameter),
            (tonic::Code::Unimplemented, ErrorCode::OperationNotSupported),
        ] {
            let error = read_failure(&tonic::Status::new(code, "refused"));
            assert_eq!(error.code(), Some(expected), "{code:?}");
            assert!(!error.is_retryable(), "{code:?}");
        }
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Internal,
            tonic::Code::ResourceExhausted,
            tonic::Code::Unknown,
        ] {
            let error = read_failure(&tonic::Status::new(code, "down"));
            assert_eq!(
                error.code(),
                Some(ErrorCode::ServiceUnavailable),
                "{code:?}"
            );
            assert!(error.is_retryable(), "{code:?}");
        }
    }
}
