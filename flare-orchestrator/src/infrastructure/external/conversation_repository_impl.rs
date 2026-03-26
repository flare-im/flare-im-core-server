use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::error::Result;

/// ConversationRepository 的枚举封装，用于在 Rust 2024 下避免 `dyn` + async trait 带来的
/// `E0038: trait is not dyn compatible` 问题。
#[derive(Debug)]
pub enum ConversationRepositoryItem {
    Grpc(Arc<crate::infrastructure::external::session_client::GrpcConversationClient>),
}

impl crate::domain::repository::conversation_repository::ConversationRepository for ConversationRepositoryItem {
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a flare_server_core::context::Context,
        conversation_id: &'a str,
        conversation_type: &'a str,
        business_type: &'a str,
        participants: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            ConversationRepositoryItem::Grpc(repo) => repo.ensure_conversation(
                ctx,
                conversation_id,
                conversation_type,
                business_type,
                participants,
            ),
        }
    }

    fn mark_conversation_as_read<'a>(
        &'a self,
        ctx: &'a flare_server_core::context::Context,
        conversation_id: &'a str,
        read_seq: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            ConversationRepositoryItem::Grpc(repo) => {
                Box::pin(async move { repo.mark_conversation_as_read(ctx, conversation_id, read_seq).await })
            }
        }
    }
}