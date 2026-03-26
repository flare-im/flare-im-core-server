use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use redis::{AsyncCommands, aio::ConnectionManager};

use crate::config::ConversationConfig;
use crate::domain::model::{
    Conversation, ConversationBootstrapResult, ConversationFilter, ConversationParticipant, ConversationSort, ConversationSummary,
};
use crate::domain::repository::ConversationRepository;

pub struct RedisConversationRepository {
    client: Arc<redis::Client>,
    config: Arc<ConversationConfig>,
}

impl RedisConversationRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<ConversationConfig>) -> Self {
        Self { client, config }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        Ok(ConnectionManager::new(self.client.as_ref().clone()).await?)
    }

    fn session_state_key(&self, conversation_id: &str) -> String {
        format!("{}:{}", self.config.conversation_state_prefix, conversation_id)
    }

    fn session_unread_key(&self, conversation_id: &str) -> String {
        format!("{}:{}", self.config.conversation_unread_prefix, conversation_id)
    }

    fn user_cursor_key(&self, user_id: &str) -> String {
        format!("{}:{}", self.config.user_cursor_prefix, user_id)
    }
}

impl ConversationRepository for RedisConversationRepository {
    async fn load_bootstrap(
        &self,
        ctx: &flare_server_core::context::Context,
        client_cursor: &HashMap<String, i64>,
    ) -> Result<ConversationBootstrapResult> {
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let mut conn = self.connection().await?;

        let cursor_key = self.user_cursor_key(user_id);
        let mut server_cursor: HashMap<String, i64> = conn
            .hgetall::<_, HashMap<String, String>>(&cursor_key)
            .await?
            .into_iter()
            .filter_map(|(k, v)| v.parse::<i64>().ok().map(|ts| (k, ts)))
            .collect();

        // merge client cursor hints to ensure we cover requested conversations
        for (conversation_id, ts) in client_cursor {
            server_cursor.entry(conversation_id.clone()).or_insert(*ts);
        }

        let mut summaries = Vec::new();

        for conversation_id in server_cursor.keys() {
            let state_key = self.session_state_key(conversation_id);
            let state: HashMap<String, String> = conn
                .hgetall::<_, HashMap<String, String>>(&state_key)
                .await
                .with_context(|| format!("load session state {}", conversation_id))?;

            if state.is_empty() {
                continue;
            }

            let unread_key = self.session_unread_key(conversation_id);
            let unread_raw: Option<String> = conn.hget(&unread_key, user_id.to_string()).await?;
            let unread: i32 = unread_raw
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or_default();

            let last_ts = state
                .get("last_message_ts")
                .and_then(|v| v.parse::<i64>().ok());

            let summary = ConversationSummary {
                conversation_id: conversation_id.clone(),
                conversation_type: state.get("conversation_type").cloned(),
                business_type: state.get("business_type").cloned(),
                last_message_id: state.get("last_message_id").cloned(),
                last_message_time: last_ts.and_then(|ts| Utc.timestamp_millis_opt(ts).single()),
                last_sender_id: state.get("last_sender_id").cloned(),
                last_message_type: state
                    .get("last_message_type")
                    .and_then(|v| v.parse::<i32>().ok()),
                last_content_type: state.get("last_content_type").cloned(),
                unread_count: unread,
                metadata: HashMap::new(),
                server_cursor_ts: last_ts.or_else(|| server_cursor.get(conversation_id).copied()),
                display_name: state.get("display_name").cloned(),
            };

            summaries.push(summary);
        }

        summaries.sort_by(|a, b| {
            let at = a.server_cursor_ts.unwrap_or_default();
            let bt = b.server_cursor_ts.unwrap_or_default();
            bt.cmp(&at)
        });

        Ok(ConversationBootstrapResult {
            summaries,
            recent_messages: Vec::new(),
            cursor_map: server_cursor,
            policy: self.config.default_policy.clone(),
        })
    }

    async fn update_cursor(&self, ctx: &flare_server_core::context::Context, conversation_id: &str, ts: i64) -> Result<()> {
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let mut conn = self.connection().await?;
        let cursor_key = self.user_cursor_key(user_id);
        let _: () = conn.hset(cursor_key, conversation_id, ts).await?;
        Ok(())
    }

    async fn create_conversation(&self, _ctx: &flare_server_core::context::Context, _session: &Conversation) -> Result<()> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support create_conversation. Use PostgresConversationRepository instead."
        ))
    }

    async fn get_conversation(&self, _ctx: &flare_server_core::context::Context, _conversation_id: &str) -> Result<Option<Conversation>> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support get_conversation. Use PostgresConversationRepository instead."
        ))
    }

    async fn update_conversation(&self, _ctx: &flare_server_core::context::Context, _session: &Conversation) -> Result<()> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support update_conversation. Use PostgresConversationRepository instead."
        ))
    }

    async fn delete_conversation(&self, _ctx: &flare_server_core::context::Context, _conversation_id: &str, _hard_delete: bool) -> Result<()> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support delete_conversation. Use PostgresConversationRepository instead."
        ))
    }

    async fn manage_participants(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
        _to_add: &[ConversationParticipant],
        _to_remove: &[String],
        _role_updates: &[(String, Vec<String>)],
    ) -> Result<Vec<ConversationParticipant>> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support manage_participants. Use PostgresConversationRepository instead."
        ))
    }

    async fn batch_acknowledge(&self, ctx: &flare_server_core::context::Context, cursors: &[(String, i64)]) -> Result<()> {
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let mut conn = self.connection().await?;
        let cursor_key = self.user_cursor_key(user_id);
        for (conversation_id, ts) in cursors {
            let _: () = conn.hset(&cursor_key, conversation_id, ts).await?;
        }
        Ok(())
    }

    async fn search_conversations(
        &self,
        _ctx: &flare_server_core::context::Context,
        _filters: &[ConversationFilter],
        _sort: &[ConversationSort],
        _limit: usize,
        _offset: usize,
    ) -> Result<(Vec<ConversationSummary>, usize)> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support search_conversations. Use PostgresConversationRepository instead."
        ))
    }

    async fn mark_as_read(&self, _ctx: &flare_server_core::context::Context, _conversation_id: &str, _seq: i64) -> Result<()> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support mark_as_read. Use PostgresConversationRepository instead."
        ))
    }

    async fn get_last_message_seq(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
    ) -> Result<Option<i64>> {
        Err(anyhow::anyhow!(
            "RedisConversationRepository does not support get_last_message_seq. Use PostgresConversationRepository instead."
        ))
    }

    async fn get_unread_count(&self, ctx: &flare_server_core::context::Context, conversation_id: &str) -> Result<i32> {
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        // Redis repository 支持读取未读数（从缓存）
        let mut conn = self.connection().await?;
        let unread_key = self.session_unread_key(conversation_id);
        let unread_raw: Option<String> = conn.hget(&unread_key, user_id.to_string()).await?;
        let unread: i32 = unread_raw
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or_default();
        Ok(unread)
    }
}
