//! 插件实体绑定（`plugin_entity_bindings`）：插件卡片落库后按插件目录投影登记。
//!
//! 卡片是 APP_CARD `flare.plugin.message.v1`，`app_id` 是插件 ID，载荷是 [`PluginMessage`] 信封。
//! 只有租户内已启用（未撤销、在有效期内）的插件、其 message-content 贡献声明了 `entityPath`、且载荷
//! 在该路径上有合法实体 ID 时才登记，`(租户, 插件, 实体) → (会话, 服务端消息 ID)`。插件之后为这个实体
//! 发布的共享状态只投到这里登记过的消息。
//!
//! 投影是控制面推给 Capability 的 `plugin_authorization_snapshots`（同一个 Core 库），这里只读。投影
//! 只按摘要指向 manifest，正文在注册表 `plugin_versions`：每个摘要在进程内只读取、解析一次。规则按
//! (租户, 投影版本) 缓存；每批插件卡片只多一次按主键读版本号，版本变了才重读整份投影。不是插件卡片的
//! 消息不碰数据库。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::manifest::{UiContribution, UiContributionKind};
use flare_im_capability_core::plugin::runtime::{ManifestCache, PluginProjection};
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
/// Tenants whose rules are kept; the cache starts over when it is full.
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
    /// `manifests` holds the projection's manifests by digest; a plugin without one is skipped.
    pub fn from_projection(
        projection: &PluginProjection,
        manifests: &HashMap<String, Arc<PluginManifest>>,
        now_ms: i64,
    ) -> Self {
        let mut contributions = HashMap::new();
        for projected in &projection.plugins {
            let grant = &projected.grant;
            if !grant.enabled || grant.revoked || !grant.in_effect(now_ms) {
                continue;
            }
            let Some(manifest) = manifests.get(projected.manifest_digest()) else {
                continue;
            };
            for contribution in &manifest.client_extensions.contributions {
                if contribution.kind == UiContributionKind::MessageContent
                    && contribution.entity_path.is_some()
                {
                    contributions.insert(
                        (manifest.plugin_id.clone(), contribution.id.clone()),
                        contribution.clone(),
                    );
                }
            }
        }
        Self {
            version: projection.version,
            valid_until_ms: projection.valid_until_ms,
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
    manifests: ManifestCache,
}

fn db_error(operation: &str, err: sqlx::Error) -> flare_server_core::error::FlareError {
    map_infra_error(err, ErrorCode::DatabaseError, operation)
}

impl PostgresPluginEntityBindings {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            rules: Mutex::new(HashMap::new()),
            manifests: ManifestCache::default(),
        }
    }

    /// The manifests of `projection` by digest. The registry is read only for digests this process
    /// has not parsed yet. A digest it does not hold, or whose text does not hash to it, stays out
    /// of the answer, so its plugin is skipped.
    async fn manifests(
        &self,
        projection: &PluginProjection,
    ) -> Result<HashMap<String, Arc<PluginManifest>>> {
        let mut found = HashMap::new();
        let mut missing = Vec::new();
        for projected in &projection.plugins {
            let digest = projected.manifest_digest();
            match self.manifests.get(digest) {
                Some(manifest) => {
                    found.insert(digest.to_owned(), manifest);
                }
                None => missing.push(digest.to_owned()),
            }
        }
        if missing.is_empty() {
            return Ok(found);
        }
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT manifest_digest, manifest_json FROM plugin_versions \
             WHERE manifest_digest = ANY($1)",
        )
        .bind(&missing)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_error("read plugin manifests", err))?;
        for (digest, raw) in rows {
            match self.manifests.insert(&digest, raw.as_bytes()) {
                Ok(manifest) => {
                    found.insert(digest, manifest);
                }
                Err(error) => {
                    warn!(digest, %error, "registered plugin manifest is invalid; its plugin cards stay unbound");
                }
            }
        }
        for digest in missing.iter().filter(|digest| !found.contains_key(*digest)) {
            warn!(
                digest,
                "plugin manifest is not in the registry; its plugin cards stay unbound"
            );
        }
        Ok(found)
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
        let projection = match serde_json::from_value::<PluginProjection>(snapshot) {
            Ok(projection) if projection.validate(now_ms).is_ok() => projection,
            _ => {
                warn!(
                    tenant_id,
                    version, "stored plugin projection is invalid; plugin cards stay unbound"
                );
                return Ok(None);
            }
        };
        let manifests = self.manifests(&projection).await?;
        let rules = Arc::new(EntityRules::from_projection(
            &projection,
            &manifests,
            now_ms,
        ));
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
        ProjectedLifecycle, ProjectedPlugin, digest_bytes,
    };
    use flare_proto::common::{AppCardContent, MessageContent};

    /// The protocol's v2 example poll: `example.poll.card` binds `/pollId`.
    pub(crate) const POLL_MANIFEST: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../flare-protocol/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
    ));

    /// A projection of the poll, and the manifests its digests name.
    pub(crate) fn poll_projection(
        tenant: &str,
        now: i64,
    ) -> (PluginProjection, HashMap<String, Arc<PluginManifest>>) {
        let manifest: serde_json::Value = serde_json::from_str(POLL_MANIFEST).unwrap();
        let raw = serde_json::to_string(&manifest).unwrap();
        let digest = digest_bytes(raw.as_bytes());
        let parsed = Arc::new(PluginManifest::from_json(raw.as_bytes()).unwrap());
        let projection = PluginProjection {
            organization_id: Some("org-1".into()),
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
                    suspended: false,
                    valid_from_ms: now,
                    valid_until_ms: now + 300_000,
                },
                lifecycle: ProjectedLifecycle::Installed,
                export_until_ms: None,
                features: Default::default(),
            }],
            bindings: Vec::new(),
        };
        (projection, HashMap::from([(digest, parsed)]))
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
        let (projection, manifests) = poll_projection("app-a", now);
        let rules = EntityRules::from_projection(&projection, &manifests, now);
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
        let (projection, manifests) = poll_projection("app-a", now);
        let rules = EntityRules::from_projection(&projection, &manifests, now);
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
        let (enabled, manifests) = poll_projection("app-a", now);
        for mode in 0..3 {
            let mut projection = enabled.clone();
            match mode {
                0 => projection.plugins[0].grant.enabled = false,
                1 => projection.plugins[0].grant.revoked = true,
                _ => projection.plugins[0].grant.valid_until_ms = now,
            }
            let rules = EntityRules::from_projection(&projection, &manifests, now);
            assert!(binding_for(&rules, &message, now).is_none(), "mode {mode}");
        }
        let rules = EntityRules::from_projection(&enabled, &manifests, now);
        assert!(binding_for(&rules, &message, now + 300_000).is_none());
        // A digest the registry does not hold skips the plugin.
        let rules = EntityRules::from_projection(&enabled, &HashMap::new(), now);
        assert!(binding_for(&rules, &message, now).is_none());
    }
}
