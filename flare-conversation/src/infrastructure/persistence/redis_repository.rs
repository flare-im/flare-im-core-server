use std::collections::HashMap;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use redis::{AsyncCommands, aio::ConnectionManager};

use crate::config::ConversationConfig;
use crate::domain::model::{
    Conversation, ConversationBootstrapResult, ConversationFilter, ConversationParticipant,
    ConversationSort, ConversationSummary, ConversationType,
};
use crate::domain::repository::ConversationRepository;
use crate::error::{ErrorBuilder, ErrorCode, Result, map_infra_error, require_user_id};

pub struct RedisConversationRepository {
    client: Arc<redis::Client>,
    config: Arc<ConversationConfig>,
}

fn redis_not_supported(method: &str) -> crate::error::FlareError {
    ErrorBuilder::new(
        ErrorCode::OperationNotSupported,
        format!(
            "RedisConversationRepository does not support {}. Use PostgresConversationRepository instead.",
            method
        ),
    )
    .build_error()
}

impl RedisConversationRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<ConversationConfig>) -> Self {
        Self { client, config }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis connection"))
    }

    fn session_state_key(&self, conversation_id: &str) -> String {
        format!(
            "{}:{}",
            self.config.conversation_state_prefix, conversation_id
        )
    }

    fn session_unread_key(&self, conversation_id: &str) -> String {
        format!(
            "{}:{}",
            self.config.conversation_unread_prefix, conversation_id
        )
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
        let user_id = require_user_id(ctx)?;
        let mut conn = self.connection().await?;

        let cursor_key = self.user_cursor_key(&user_id);
        let mut server_cursor: HashMap<String, i64> = conn
            .hgetall::<_, HashMap<String, String>>(&cursor_key)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis hgetall user cursor"))?
            .into_iter()
            .filter_map(|(k, v)| v.parse::<i64>().ok().map(|ts| (k, ts)))
            .collect();

        for (conversation_id, ts) in client_cursor {
            server_cursor.entry(conversation_id.clone()).or_insert(*ts);
        }

        let mut summaries = Vec::new();

        for conversation_id in server_cursor.keys() {
            let state_key = self.session_state_key(conversation_id);
            let state: HashMap<String, String> = conn
                .hgetall::<_, HashMap<String, String>>(&state_key)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        format!("load session state {}", conversation_id),
                    )
                })?;

            if state.is_empty() {
                continue;
            }

            let unread_key = self.session_unread_key(conversation_id);
            let unread_raw: Option<String> = conn
                .hget(&unread_key, user_id.clone())
                .await
                .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis hget unread"))?;
            let unread: i32 = unread_raw
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or_default();

            let last_ts = state
                .get("last_message_ts")
                .and_then(|v| v.parse::<i64>().ok());

            let summary = ConversationSummary {
                conversation_id: conversation_id.clone(),
                conversation_type: ConversationType::from_db_optional(
                    state.get("conversation_type").cloned(),
                ),
                business_type: state.get("business_type").cloned(),
                last_message_id: state.get("last_message_id").cloned(),
                last_message_time: last_ts.and_then(|ts| Utc.timestamp_millis_opt(ts).single()),
                last_sender_id: state.get("last_sender_id").cloned(),
                last_message_type: state
                    .get("last_message_type")
                    .and_then(|v| v.parse::<i32>().ok()),
                last_content_type: state.get("last_content_type").cloned(),
                unread_count: unread,
                last_read_seq: 0,
                metadata: HashMap::new(),
                server_cursor_ts: last_ts.or_else(|| server_cursor.get(conversation_id).copied()),
                display_name: state.get("display_name").cloned(),
                last_message_seq: state
                    .get("last_message_seq")
                    .and_then(|v| v.parse::<i64>().ok()),
                channel_id: state.get("channel_id").cloned().unwrap_or_default(),
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

    async fn update_cursor(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        ts: i64,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;
        let mut conn = self.connection().await?;
        let cursor_key = self.user_cursor_key(&user_id);
        let _: () = conn
            .hset(cursor_key, conversation_id, ts)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis hset cursor"))?;
        Ok(())
    }

    async fn create_conversation(
        &self,
        _ctx: &flare_server_core::context::Context,
        _session: &Conversation,
    ) -> Result<()> {
        Err(redis_not_supported("create_conversation"))
    }

    async fn get_conversation(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
    ) -> Result<Option<Conversation>> {
        Err(redis_not_supported("get_conversation"))
    }

    async fn update_conversation(
        &self,
        _ctx: &flare_server_core::context::Context,
        _session: &Conversation,
    ) -> Result<()> {
        Err(redis_not_supported("update_conversation"))
    }

    async fn delete_conversation(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
        _hard_delete: bool,
    ) -> Result<()> {
        Err(redis_not_supported("delete_conversation"))
    }

    async fn manage_participants(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
        _to_add: &[ConversationParticipant],
        _to_remove: &[String],
        _role_updates: &[(String, Vec<String>)],
    ) -> Result<Vec<ConversationParticipant>> {
        Err(redis_not_supported("manage_participants"))
    }

    async fn batch_acknowledge(
        &self,
        ctx: &flare_server_core::context::Context,
        cursors: &[(String, i64)],
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;
        let mut conn = self.connection().await?;
        let cursor_key = self.user_cursor_key(&user_id);
        for (conversation_id, ts) in cursors {
            let _: () = conn
                .hset(&cursor_key, conversation_id, *ts)
                .await
                .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis hset batch ack"))?;
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
        Err(redis_not_supported("search_conversations"))
    }

    async fn mark_as_read(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
        _seq: i64,
    ) -> Result<()> {
        Err(redis_not_supported("mark_as_read"))
    }

    async fn get_last_message_seq(
        &self,
        _ctx: &flare_server_core::context::Context,
        _conversation_id: &str,
    ) -> Result<Option<i64>> {
        Err(redis_not_supported("get_last_message_seq"))
    }

    async fn get_unread_count(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
    ) -> Result<i32> {
        let user_id = require_user_id(ctx)?;
        let mut conn = self.connection().await?;
        let unread_key = self.session_unread_key(conversation_id);
        let unread_raw: Option<String> = conn
            .hget(&unread_key, user_id.clone())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis hget unread"))?;
        let unread: i32 = unread_raw
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or_default();
        Ok(unread)
    }
}
