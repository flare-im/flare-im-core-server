//! `plugin_entity_bindings` for Capability: Capability registers the card it posts and Storage Writer
//! the cards it stores, and the writer marks them recalled; Capability reads them, checks card
//! actions against them and raises `published_version` by compare-and-set before it fans a version
//! out (migrations/0016).
use crate::domain::repository::plugin_state::{
    ClaimedBinding, EntityBinding, MessageBinding, PluginEntityBindingStore,
};
use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;
use std::sync::Arc;

pub struct PostgresPluginEntityBindings {
    pub pool: Arc<PgPool>,
}

fn unavailable(_: impl std::fmt::Debug) -> FlareError {
    FlareError::system("plugin entity bindings unavailable")
}

fn version_param(version: u64) -> Result<i64> {
    i64::try_from(version).map_err(unavailable)
}

impl PostgresPluginEntityBindings {
    /// Fails when the table is absent (migration 0016 not applied).
    pub async fn check_schema(&self) -> Result<()> {
        sqlx::query("SELECT tenant_id, plugin_id, entity, server_msg_id, conversation_id, contribution_id, published_version, recalled_at FROM plugin_entity_bindings LIMIT 0")
            .execute(self.pool.as_ref())
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PluginEntityBindingStore for PostgresPluginEntityBindings {
    async fn bindings(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
    ) -> Result<Vec<EntityBinding>> {
        let rows: Vec<(String, String, i64, bool)> = sqlx::query_as(
            "SELECT conversation_id, server_msg_id, published_version, recalled_at IS NOT NULL \
             FROM plugin_entity_bindings WHERE tenant_id = $1 AND plugin_id = $2 AND entity = $3 \
             ORDER BY created_at, server_msg_id LIMIT 64",
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .bind(entity)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(
                |(conversation_id, server_msg_id, published_version, recalled)| EntityBinding {
                    conversation_id,
                    server_msg_id,
                    published_version: published_version.max(0) as u64,
                    recalled,
                },
            )
            .collect())
    }

    async fn binding_by_message(
        &self,
        tenant_id: &str,
        server_msg_id: &str,
    ) -> Result<Option<MessageBinding>> {
        // idx_plugin_entity_bindings_message. Two rows for one message would mean two entities on
        // one card: refused rather than guessed.
        let mut rows: Vec<(String, String, String, String, bool)> = sqlx::query_as(
            "SELECT plugin_id, contribution_id, entity, conversation_id, recalled_at IS NOT NULL \
             FROM plugin_entity_bindings WHERE tenant_id = $1 AND server_msg_id = $2 LIMIT 2",
        )
        .bind(tenant_id)
        .bind(server_msg_id)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        if rows.len() > 1 {
            return Err(FlareError::system(
                "plugin card bound to more than one entity",
            ));
        }
        Ok(rows.pop().map(
            |(plugin_id, contribution_id, entity, conversation_id, recalled)| MessageBinding {
                plugin_id,
                contribution_id,
                entity,
                conversation_id,
                recalled,
            },
        ))
    }

    async fn claim_version(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
        version: u64,
    ) -> Result<Vec<ClaimedBinding>> {
        let version = version_param(version)?;
        // The row lock of the UPDATE serializes concurrent claims of other instances: the loser
        // re-checks `published_version < $4` on the committed row and claims nothing.
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            r#"
            WITH prior AS (
                SELECT server_msg_id, published_version
                FROM plugin_entity_bindings
                WHERE tenant_id = $1 AND plugin_id = $2 AND entity = $3
                  AND published_version < $4 AND recalled_at IS NULL
                FOR UPDATE
            )
            UPDATE plugin_entity_bindings b
            SET published_version = $4
            FROM prior
            WHERE b.tenant_id = $1 AND b.plugin_id = $2 AND b.entity = $3
              AND b.server_msg_id = prior.server_msg_id
              AND b.published_version < $4
            RETURNING b.conversation_id, b.server_msg_id, prior.published_version
            "#,
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .bind(entity)
        .bind(version)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(
                |(conversation_id, server_msg_id, prior_version)| ClaimedBinding {
                    conversation_id,
                    server_msg_id,
                    prior_version: prior_version.max(0) as u64,
                },
            )
            .collect())
    }

    async fn register(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        server_msg_id: &str,
        conversation_id: &str,
    ) -> Result<()> {
        // The writer's statement (`plugin_entity_bindings.rs` in Storage Writer): one binding per
        // entity; the writer's later registration of the same card finds this row.
        sqlx::query(
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
        .bind(plugin_id)
        .bind(entity)
        .bind(server_msg_id)
        .bind(conversation_id)
        .bind(contribution_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }

    async fn release_version(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
        binding: &ClaimedBinding,
        version: u64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE plugin_entity_bindings SET published_version = $5 \
             WHERE tenant_id = $1 AND plugin_id = $2 AND entity = $3 AND server_msg_id = $4 \
               AND published_version = $6",
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .bind(entity)
        .bind(&binding.server_msg_id)
        .bind(version_param(binding.prior_version)?)
        .bind(version_param(version)?)
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }
}
