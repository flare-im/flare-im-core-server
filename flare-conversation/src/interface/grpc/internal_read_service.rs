//! `ConversationInternalReadService`：Cell 内服务以自己的身份**按用户**读（只读）。
//!
//! 请求体里的 `user_id`（或 `user_ids`、`conversation_id`）说的是读谁的数据，不是谁在读，所以
//! 调用者只能是服务：边界
//! （[`super::signed_internal_read_service`]）一律要求签名并且只放行服务身份，处理器再按上下文里
//! 的身份核对一次——它被挂到别的边界后面时也不会向用户敞开。租户只取上下文（来自签名），
//! 请求不带租户。

use flare_grpc_proto::conversation::conversation_internal_read_service_server::ConversationInternalReadService;
use flare_grpc_proto::conversation::{
    BatchGetUserNotificationSettingsRequest, BatchGetUserNotificationSettingsResponse,
    ListNotificationSubscribersRequest, ListNotificationSubscribersResponse,
    ListRecentUnreadConversationsRequest, ListRecentUnreadConversationsResponse,
    RecentUnreadConversation as ProtoRecentUnreadConversation,
};
use flare_server_core::context::{ActorType, Context};
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};

use super::ConversationGrpcHandler;
use super::notification_settings_service::settings_to_proto;
use crate::application::queries::{
    BatchGetUserNotificationSettingsQuery, ListNotificationSubscribersQuery,
    ListRecentUnreadConversationsQuery,
};
use crate::domain::model::{NotificationSubscriberPage, RecentUnreadConversation};

/// 调用者必须是服务身份。与成员读放行内部服务的判定一样看上下文里的 actor，但这里**只**认
/// 服务：没有身份、用户、管理员一律拒绝。
fn require_service_caller(ctx: &Context) -> Result<(), Status> {
    let service = ctx
        .actor()
        .is_some_and(|actor| actor.actor_type == ActorType::Service);
    if service {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "only a Cell service may read conversations on behalf of a user",
        ))
    }
}

fn subscriber_page_to_proto(
    page: NotificationSubscriberPage,
) -> ListNotificationSubscribersResponse {
    ListNotificationSubscribersResponse {
        user_ids: page.user_ids,
        next_page_token: page.next_page_token.unwrap_or_default(),
        truncated: page.truncated,
    }
}

fn recent_unread_to_proto(conversation: RecentUnreadConversation) -> ProtoRecentUnreadConversation {
    ProtoRecentUnreadConversation {
        conversation_id: conversation.conversation_id,
        last_message_seq: u64::try_from(conversation.last_message_seq).unwrap_or_default(),
        last_message_id: conversation.last_message_id,
        last_message_time: conversation.last_message_time.timestamp_millis(),
        last_sender_id: conversation.last_sender_id,
        unread_count: u32::try_from(conversation.unread_count).unwrap_or_default(),
    }
}

#[tonic::async_trait]
impl ConversationInternalReadService for ConversationGrpcHandler {
    async fn list_recent_unread_conversations(
        &self,
        request: Request<ListRecentUnreadConversationsRequest>,
    ) -> Result<Response<ListRecentUnreadConversationsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        require_service_caller(&ctx)?;
        let req = request.into_inner();
        let conversations = self
            .query_handler
            .handle_list_recent_unread_conversations(
                &ctx,
                ListRecentUnreadConversationsQuery {
                    user_id: req.user_id,
                    since_ms: req.since,
                    limit: req.limit,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(ListRecentUnreadConversationsResponse {
            conversations: conversations
                .into_iter()
                .map(recent_unread_to_proto)
                .collect(),
        }))
    }

    async fn batch_get_user_notification_settings(
        &self,
        request: Request<BatchGetUserNotificationSettingsRequest>,
    ) -> Result<Response<BatchGetUserNotificationSettingsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        require_service_caller(&ctx)?;
        let req = request.into_inner();
        let entries = self
            .query_handler
            .handle_batch_get_user_notification_settings(
                &ctx,
                BatchGetUserNotificationSettingsQuery {
                    user_ids: req.user_ids,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(BatchGetUserNotificationSettingsResponse {
            settings: entries
                .into_iter()
                .map(|(user_id, settings)| (user_id, settings_to_proto(&settings)))
                .collect(),
        }))
    }

    async fn list_notification_subscribers(
        &self,
        request: Request<ListNotificationSubscribersRequest>,
    ) -> Result<Response<ListNotificationSubscribersResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        require_service_caller(&ctx)?;
        let req = request.into_inner();
        let page = self
            .query_handler
            .handle_list_notification_subscribers(
                &ctx,
                ListNotificationSubscribersQuery {
                    conversation_id: req.conversation_id,
                    page_token: req.page_token,
                    page_size: req.page_size,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(subscriber_page_to_proto(page)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use flare_server_core::context::ActorContext;

    fn actor(actor_type: ActorType) -> Context {
        Context::with_request_id("req-internal-read")
            .with_tenant_id("tenant-a")
            .with_actor(ActorContext::new("caller").with_type(actor_type))
    }

    /// 按用户读只对服务开放：用户（哪怕读的是自己）、管理员、系统身份、没有身份的调用都拒绝。
    #[test]
    fn only_a_service_actor_may_read_on_behalf_of_a_user() {
        assert!(require_service_caller(&actor(ActorType::Service)).is_ok());
        for other in [ActorType::User, ActorType::TenantAdmin, ActorType::System] {
            assert_eq!(
                require_service_caller(&actor(other)).unwrap_err().code(),
                tonic::Code::PermissionDenied
            );
        }
        let anonymous = Context::with_request_id("req-internal-read")
            .with_tenant_id("tenant-a")
            .with_user_id("alice");
        assert_eq!(
            require_service_caller(&anonymous).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn a_subscriber_page_maps_to_the_wire_shape() {
        let wire = subscriber_page_to_proto(NotificationSubscriberPage {
            user_ids: vec!["a".into(), "b".into()],
            next_page_token: Some("2:b".into()),
            truncated: false,
        });
        assert_eq!(wire.user_ids, ["a", "b"]);
        assert_eq!(wire.next_page_token, "2:b");
        assert!(!wire.truncated);

        let last = subscriber_page_to_proto(NotificationSubscriberPage {
            user_ids: vec!["z".into()],
            next_page_token: None,
            truncated: true,
        });
        assert_eq!(last.next_page_token, "");
        assert!(last.truncated);
    }

    #[test]
    fn rows_map_to_the_wire_shape() {
        let wire = recent_unread_to_proto(RecentUnreadConversation {
            conversation_id: "conv-1".into(),
            last_message_seq: 42,
            last_message_id: "m-42".into(),
            last_sender_id: "bob".into(),
            last_message_time: Utc
                .timestamp_millis_opt(1_760_000_000_123)
                .single()
                .unwrap(),
            unread_count: 3,
        });
        assert_eq!(wire.conversation_id, "conv-1");
        assert_eq!(wire.last_message_seq, 42);
        assert_eq!(wire.last_message_id, "m-42");
        assert_eq!(wire.last_message_time, 1_760_000_000_123);
        assert_eq!(wire.last_sender_id, "bob");
        assert_eq!(wire.unread_count, 3);
    }
}
