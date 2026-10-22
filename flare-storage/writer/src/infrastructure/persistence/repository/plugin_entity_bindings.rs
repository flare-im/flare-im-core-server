//! 插件实体绑定（`plugin_entity_bindings`）：插件卡片落库后按插件目录投影登记。
//!
//! 卡片是 APP_CARD `flare.plugin.message.v1`，`app_id` 是插件 ID，载荷是 [`PluginMessage`] 信封。
//! 只有租户内已启用（未撤销、在有效期内）的插件、其 message-content 贡献声明了 `entityPath`、且载荷
//! 在该路径上有合法实体 ID 时才登记，`(租户, 插件, 实体) → (会话, 服务端消息 ID)`。插件之后为这个实体
//! 发布的共享状态只投到这里登记过的消息。
//!
//! 投影是控制面推给 Capability 的 `plugin_authorization_snapshots`（同一个 Core 库），这里只读。解析
//! 一次投影要校验其中每个 manifest，所以按 (租户, 投影版本) 缓存解析结果；每批插件卡片只多一次按主键
//! 读版本号，版本变了才重读整份投影。不是插件卡片的消息不碰数据库。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use flare_im_capability_core::plugin::manifest::{UiContribution, UiContributionKind};
use flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot;
use flare_im_capability_core::plugin::ui::PluginMessage;
use flare_im_contracts::Ctx;
use flare_proto::common::message_content::Content;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use sqlx::{Pool, Postgres};
use tracing::warn;

use crate::domain::model::Message;
use crate::domain::repository::PluginEntityBindingRepository;

/// APP_CARD `card_type` of a plugin card.
pub const PLUGIN_MESSAGE_CARD_TYPE: &str = "flare.plugin.message.v1";
/// Tenants whose parsed projection is kept; the cache starts over when it is full.
const MAX_CACHED_TENANTS: usize = 1024;

/// One binding a stored card asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityBinding {
    pub plugin_id: String,
    pub contribution_id: String,
    pub entity: String,
    pub server_msg_id: String,
    pub conversation_id: String,
}

/// The plugin envelope of a stored message: an APP_CARD `flare.plugin.message.v1` whose `app_id`
/// is the plugin of a valid envelope. Anything else is not a plugin card.
pub fn plugin_card(message: &Message) -> Option<PluginMessage> {
    let Some(Content::AppCard(card)) = message.content.as_ref()?.content.as_ref() else {
        return None;
    };
    if card.card_type != PLUGIN_MESSAGE_CARD_TYPE || card.payload.len() > 65536 {
        return None;
    }
    let envelope: PluginMessage = serde_json::from_slice(&card.payload).ok()?;
    (envelope.validate().is_ok() && envelope.plugin_id == card.app_id).then_some(envelope)
}

/// The message-content contributions declaring `entityPath` of the plugins one projection version
/// enables.
#[derive(Debug, Default)]
pub struct EntityRules {
    version: u64,
    valid_until_ms: i64,
    contributions: HashMap<(String, String), UiContribution>,
}

impl EntityRules {
    pub fn from_snapshot(snapshot: &PluginRuntimeSnapshot, now_ms: i64) -> Self {
        let mut contributions = HashMap::new();
        for projected in &snapshot.plugins {
            let grant = &projected.grant;
            if !grant.enabled
                || grant.revoked
                || now_ms < grant.valid_from_ms
                || now_ms >= grant.valid_until_ms
            {
                continue;
            }
            let Ok(manifest) = projected.manifest() else {
                continue;
            };
            for contribution in manifest.client_extensions.contributions {
                if contribution.kind == UiContributionKind::MessageContent
                    && contribution.entity_path.is_some()
                {
                    contributions.insert(
                        (manifest.plugin_id.clone(), contribution.id.clone()),
                        contribution,
                    );
                }
            }
        }
        Self {
            version: snapshot.version,
            valid_until_ms: snapshot.valid_until_ms,
            contributions,
        }
    }

    /// The binding `message` (a plugin card with `envelope`) asks for under these rules.
    pub fn binding_of(
        &self,
        message: &Message,
        envelope: &PluginMessage,
        now_ms: i64,
    ) -> Option<EntityBinding> {
        if now_ms >= self.valid_until_ms
            || message.server_id.is_empty()
            || message.conversation_id.is_empty()
        {
            return None;
        }
        let contribution = self
            .contributions
            .get(&(envelope.plugin_id.clone(), envelope.contribution_id.clone()))?;
        Some(EntityBinding {
            plugin_id: envelope.plugin_id.clone(),
            contribution_id: envelope.contribution_id.clone(),
            entity: contribution.entity_of(&envelope.payload)?,
            server_msg_id: message.server_id.clone(),
            conversation_id: message.conversation_id.clone(),
        })
    }
}

pub struct PostgresPluginEntityBindings {
    pool: Pool<Postgres>,
    rules: Mutex<HashMap<String, Arc<EntityRules>>>,
}

fn db_error(operation: &str, err: sqlx::Error) -> flare_server_core::error::FlareError {
    map_infra_error(err, ErrorCode::DatabaseError, operation)
}

impl PostgresPluginEntityBindings {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            rules: Mutex::new(HashMap::new()),
        }
    }

    /// The rules of the tenant's current, unexpired projection; `None` without one.
    async fn rules(&self, tenant_id: &str, now_ms: i64) -> Result<Option<Arc<EntityRules>>> {
        let version: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM plugin_authorization_snapshots WHERE app_id = $1 AND valid_until_ms > $2",
        )
        .bind(tenant_id)
        .bind(now_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("read plugin projection version", err))?;
        let Some(version) = version else {
            return Ok(None);
        };
        if let Some(cached) = self
            .rules
            .lock()
            .ok()
            .and_then(|rules| rules.get(tenant_id).cloned())
            && cached.version == version as u64
            && cached.valid_until_ms > now_ms
        {
            return Ok(Some(cached));
        }
        let snapshot: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT snapshot FROM plugin_authorization_snapshots WHERE app_id = $1 AND version = $2",
        )
        .bind(tenant_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("read plugin projection", err))?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let snapshot = match serde_json::from_value::<PluginRuntimeSnapshot>(snapshot) {
            Ok(snapshot) if snapshot.validate(now_ms).is_ok() => snapshot,
            _ => {
                warn!(
                    tenant_id,
                    version, "stored plugin projection is invalid; plugin cards stay unbound"
                );
                return Ok(None);
            }
        };
        let rules = Arc::new(EntityRules::from_snapshot(&snapshot, now_ms));
        if let Ok(mut cache) = self.rules.lock() {
            if cache.len() >= MAX_CACHED_TENANTS && !cache.contains_key(tenant_id) {
                cache.clear();
            }
            cache.insert(tenant_id.to_owned(), rules.clone());
        }
        Ok(Some(rules))
    }

    /// One binding per entity for now: a card is bound only when its entity has no binding yet. A
    /// copy of a card posted elsewhere (forwarded or forged) therefore never receives the entity's
    /// state, and a redelivered card finds its own row. The table keeps one-to-many for later.
    async fn insert(&self, tenant_id: &str, bindings: &[EntityBinding]) -> Result<usize> {
        let mut inserted = 0;
        for binding in bindings {
            let done = sqlx::query(
                r#"
                INSERT INTO plugin_entity_bindings (
                    tenant_id, plugin_id, entity, server_msg_id, conversation_id, contribution_id
                )
                SELECT $1, $2, $3, $4, $5, $6
                WHERE NOT EXISTS (
                    SELECT 1 FROM plugin_entity_bindings
                    WHERE tenant_id = $1 AND plugin_id = $2 AND entity = $3
                )
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(tenant_id)
            .bind(&binding.plugin_id)
            .bind(&binding.entity)
            .bind(&binding.server_msg_id)
            .bind(&binding.conversation_id)
            .bind(&binding.contribution_id)
            .execute(&self.pool)
            .await
            .map_err(|err| db_error("register plugin entity binding", err))?;
            inserted += done.rows_affected() as usize;
        }
        Ok(inserted)
    }
}

impl PluginEntityBindingRepository for PostgresPluginEntityBindings {
    fn register<'a>(
        &'a self,
        _ctx: &'a Ctx,
        tenant_id: &'a str,
        messages: &'a [Message],
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            let cards: Vec<(&Message, PluginMessage)> = messages
                .iter()
                .filter_map(|message| plugin_card(message).map(|card| (message, card)))
                .collect();
            if cards.is_empty() {
                return Ok(0);
            }
            let now_ms = chrono::Utc::now().timestamp_millis();
            let Some(rules) = self.rules(tenant_id, now_ms).await? else {
                return Ok(0);
            };
            let bindings: Vec<EntityBinding> = cards
                .iter()
                .filter_map(|(message, card)| rules.binding_of(message, card, now_ms))
                .collect();
            if bindings.is_empty() {
                return Ok(0);
            }
            self.insert(tenant_id, &bindings).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_capability_core::plugin::authorization::TenantPluginGrant;
    use flare_im_capability_core::plugin::runtime::{
        PluginDeployment, ProjectedPlugin, configuration_digest, digest_bytes,
    };
    use flare_proto::common::{AppCardContent, MessageContent};

    /// The protocol's v2 example poll: `example.poll.card` binds `/pollId`.
    pub(crate) const POLL_MANIFEST: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../flare-proto/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
    ));

    pub(crate) fn poll_snapshot(tenant: &str, now: i64) -> PluginRuntimeSnapshot {
        let manifest: serde_json::Value = serde_json::from_str(POLL_MANIFEST).unwrap();
        let raw = serde_json::to_string(&manifest).unwrap();
        let digest = digest_bytes(raw.as_bytes());
        PluginRuntimeSnapshot {
            tenant_id: tenant.into(),
            version: 7,
            issued_at_ms: now,
            valid_until_ms: now + 300_000,
            plugins: vec![ProjectedPlugin {
                grant: TenantPluginGrant {
                    grant_id: "g-poll".into(),
                    revision: 1,
                    tenant_id: tenant.into(),
                    plugin_id: "example.poll".into(),
                    plugin_version: "0.2.0".into(),
                    manifest_digest: digest.clone(),
                    approved_operations: Default::default(),
                    approved_permissions: Default::default(),
                    approved_data_scopes: Default::default(),
                    enabled: true,
                    revoked: false,
                    valid_from_ms: now,
                    valid_until_ms: now + 300_000,
                },
                manifest_json: raw,
                deployment: PluginDeployment {
                    deployment_id: "poll-1".into(),
                    plugin_id: "example.poll".into(),
                    plugin_version: "0.2.0".into(),
                    manifest_digest: digest,
                    artifact_digest: manifest["artifact"]["digest"].as_str().unwrap().into(),
                    grpc_authority: "http://127.0.0.1:1".into(),
                },
                configuration_digest: configuration_digest(&serde_json::json!({})).unwrap(),
                client_configuration: Default::default(),
            }],
        }
    }

    pub(crate) fn card(server_id: &str, app_id: &str, envelope: serde_json::Value) -> Message {
        Message {
            server_id: server_id.into(),
            conversation_id: "2-group-a".into(),
            content: Some(MessageContent {
                content: Some(Content::AppCard(AppCardContent {
                    app_id: app_id.into(),
                    card_type: PLUGIN_MESSAGE_CARD_TYPE.into(),
                    payload: serde_json::to_vec(&envelope).unwrap(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }
    }

    pub(crate) fn poll_envelope(poll_id: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "pluginId": "example.poll",
            "contributionId": "example.poll.card",
            "schemaVersion": 1,
            "summary": "[Poll]",
            "payload": {"pollId": poll_id, "question": "Lunch?"}
        })
    }

    fn binding_for(rules: &EntityRules, message: &Message, now: i64) -> Option<EntityBinding> {
        plugin_card(message).and_then(|envelope| rules.binding_of(message, &envelope, now))
    }

    #[test]
    fn a_card_of_an_enabled_plugin_binds_its_entity() {
        let now = chrono::Utc::now().timestamp_millis();
        let rules = EntityRules::from_snapshot(&poll_snapshot("app-a", now), now);
        let message = card("m-1", "example.poll", poll_envelope("poll-1".into()));
        assert_eq!(
            binding_for(&rules, &message, now),
            Some(EntityBinding {
                plugin_id: "example.poll".into(),
                contribution_id: "example.poll.card".into(),
                entity: "poll-1".into(),
                server_msg_id: "m-1".into(),
                conversation_id: "2-group-a".into(),
            })
        );
        // An integer entity id binds as its decimal text.
        let message = card("m-2", "example.poll", poll_envelope(42.into()));
        assert_eq!(binding_for(&rules, &message, now).unwrap().entity, "42");
    }

    #[test]
    fn other_cards_plugins_and_payloads_bind_nothing() {
        let now = chrono::Utc::now().timestamp_millis();
        let rules = EntityRules::from_snapshot(&poll_snapshot("app-a", now), now);
        // app_id is not the envelope's plugin.
        let forged = card("m-1", "example.other", poll_envelope("poll-1".into()));
        assert!(plugin_card(&forged).is_none());
        // A contribution without entityPath (the composer action), or an unknown one.
        for contribution in ["example.poll.create", "example.poll.missing"] {
            let mut envelope = poll_envelope("poll-1".into());
            envelope["contributionId"] = contribution.into();
            assert!(binding_for(&rules, &card("m-1", "example.poll", envelope), now).is_none());
        }
        // Missing or invalid entity ids.
        for poll_id in [
            serde_json::Value::Null,
            "".into(),
            "has space".into(),
            serde_json::json!(["poll-1"]),
            serde_json::Value::String("x".repeat(129)),
        ] {
            let message = card("m-1", "example.poll", poll_envelope(poll_id.clone()));
            assert!(binding_for(&rules, &message, now).is_none(), "{poll_id}");
        }
        // A text message is not a plugin card.
        assert!(plugin_card(&Message::default()).is_none());
    }

    #[test]
    fn disabled_revoked_and_expired_grants_bind_nothing() {
        let now = chrono::Utc::now().timestamp_millis();
        let message = card("m-1", "example.poll", poll_envelope("poll-1".into()));
        for mode in 0..3 {
            let mut snapshot = poll_snapshot("app-a", now);
            match mode {
                0 => snapshot.plugins[0].grant.enabled = false,
                1 => snapshot.plugins[0].grant.revoked = true,
                _ => snapshot.plugins[0].grant.valid_until_ms = now,
            }
            let rules = EntityRules::from_snapshot(&snapshot, now);
            assert!(binding_for(&rules, &message, now).is_none(), "mode {mode}");
        }
        let rules = EntityRules::from_snapshot(&poll_snapshot("app-a", now), now);
        assert!(binding_for(&rules, &message, now + 300_000).is_none());
    }
}
