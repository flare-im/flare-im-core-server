use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_im_service_kit::config::PostgresInstanceConfig;
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool, Row};

use crate::domain::model::{
    FileAccessType, MediaAssetStatus, MediaFileMetadata, MediaReference,
    STORAGE_BUCKET_METADATA_KEY, STORAGE_PATH_METADATA_KEY,
};
use crate::domain::repository::{MediaMetadataStore, MediaReferenceStore, OrphanAssetClaim};

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const MEDIA_ASSET_COLUMNS: &str = "owner_id, file_id, file_name, mime_type, file_size, url, \
     cdn_url, md5, sha256, metadata, uploaded_at, reference_count, status, grace_expires_at, \
     access_type";

#[cfg(test)]
#[path = "postgres_metadata_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "postgres_race_tests.rs"]
mod race_tests;

#[cfg(test)]
#[path = "media_source_audit_pg_tests.rs"]
mod source_audit_tests;

#[derive(Debug, FromRow)]
struct MediaAssetRow {
    owner_id: Option<String>,
    file_id: String,
    file_name: String,
    mime_type: String,
    file_size: i64,
    url: String,
    cdn_url: String,
    md5: Option<String>,
    sha256: Option<String>,
    metadata: Option<Value>,
    uploaded_at: DateTime<Utc>,
    reference_count: i64,
    status: String,
    grace_expires_at: Option<DateTime<Utc>>,
    access_type: String,
}

impl TryFrom<MediaAssetRow> for MediaFileMetadata {
    type Error = FlareError;

    fn try_from(row: MediaAssetRow) -> std::result::Result<Self, Self::Error> {
        let metadata_map = match row.metadata {
            Some(value) => {
                serde_json::from_value::<HashMap<String, String>>(value).map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DeserializationError,
                        "failed to deserialize metadata",
                    )
                })?
            }
            None => HashMap::new(),
        };

        let storage_path = metadata_map.get(STORAGE_PATH_METADATA_KEY).cloned();
        let storage_bucket = metadata_map.get(STORAGE_BUCKET_METADATA_KEY).cloned();

        let status = MediaAssetStatus::from_str(&row.status).map_err(|_| {
            map_infra_error(
                format!("invalid media asset status: {}", row.status),
                ErrorCode::DeserializationError,
                "invalid media asset status",
            )
        })?;

        let access_type = match row.access_type.as_str() {
            "public" => FileAccessType::Public,
            "private" => FileAccessType::Private,
            _ => FileAccessType::default(), // 默认使用私有访问类型
        };

        Ok(MediaFileMetadata {
            owner_id: row.owner_id,
            file_id: row.file_id,
            file_name: row.file_name,
            mime_type: row.mime_type,
            file_size: row.file_size,
            url: row.url,
            cdn_url: row.cdn_url,
            md5: row.md5,
            sha256: row.sha256,
            metadata: metadata_map,
            uploaded_at: row.uploaded_at,
            reference_count: row.reference_count as u64,
            status,
            grace_expires_at: row.grace_expires_at,
            access_type,
            storage_bucket,
            storage_path,
        })
    }
}

impl MediaAssetRow {
    fn status_to_str(status: MediaAssetStatus) -> &'static str {
        match status {
            MediaAssetStatus::Pending => "pending",
            MediaAssetStatus::Active => "active",
            MediaAssetStatus::SoftDeleted => "soft_deleted",
        }
    }

    fn access_type_to_str(access_type: FileAccessType) -> &'static str {
        match access_type {
            FileAccessType::Public => "public",
            FileAccessType::Private => "private",
        }
    }
}

#[derive(Debug, FromRow)]
struct MediaReferenceRow {
    tenant_id: String,
    reference_id: String,
    file_id: String,
    namespace: String,
    owner_id: String,
    business_tag: Option<String>,
    metadata: Option<Value>,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
}

impl TryFrom<MediaReferenceRow> for MediaReference {
    type Error = FlareError;

    fn try_from(row: MediaReferenceRow) -> std::result::Result<Self, Self::Error> {
        let metadata_map = match row.metadata {
            Some(value) => {
                serde_json::from_value::<HashMap<String, String>>(value).map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DeserializationError,
                        "failed to deserialize reference metadata",
                    )
                })?
            }
            None => HashMap::new(),
        };

        Ok(MediaReference {
            tenant_id: row.tenant_id,
            reference_id: row.reference_id,
            file_id: row.file_id,
            namespace: row.namespace,
            owner_id: row.owner_id,
            business_tag: row.business_tag,
            metadata: metadata_map,
            created_at: row.created_at,
            expires_at: row.expires_at,
        })
    }
}

/// 未提交的孤儿认领：持有事务即持有资产行的 FOR UPDATE 锁。
struct PostgresOrphanClaim {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    asset: MediaFileMetadata,
}

#[async_trait::async_trait]
impl OrphanAssetClaim for PostgresOrphanClaim {
    fn asset(&self) -> &MediaFileMetadata {
        &self.asset
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.tx.commit().await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to commit orphaned media claim",
            )
        })
    }
}

#[derive(Clone)]
pub struct PostgresMetadataStore {
    pool: Arc<PgPool>,
}

impl PostgresMetadataStore {
    pub async fn new(config: &str) -> Result<Self> {
        Self::connect(config, DEFAULT_MAX_CONNECTIONS, 2, 10, 300, 1800).await
    }

    pub async fn from_config(config: &PostgresInstanceConfig) -> Result<Self> {
        Self::connect(
            &config.url,
            config.max_connections_or(DEFAULT_MAX_CONNECTIONS),
            config.min_connections_or(2),
            config.acquire_timeout_seconds.unwrap_or(10),
            config.idle_timeout_seconds.unwrap_or(300),
            config.max_lifetime_seconds.unwrap_or(1800),
        )
        .await
    }

    async fn connect(
        config: &str,
        max_connections: u32,
        min_connections: u32,
        acquire_timeout_seconds: u64,
        idle_timeout_seconds: u64,
        max_lifetime_seconds: u64,
    ) -> Result<Self> {
        let max_connections = max_connections.max(1);
        let min_connections = min_connections.min(max_connections);
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(std::time::Duration::from_secs(
                acquire_timeout_seconds.max(1),
            ))
            .idle_timeout(std::time::Duration::from_secs(idle_timeout_seconds.max(1)))
            .max_lifetime(std::time::Duration::from_secs(max_lifetime_seconds.max(1)))
            .connect(config)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "failed to connect to postgres")
            })?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn audit_pool(&self) -> Arc<PgPool> {
        self.pool.clone()
    }

    fn metadata_to_json(metadata: &HashMap<String, String>) -> Result<Value> {
        serde_json::to_value(metadata).map_err(|e| {
            map_infra_error(e, ErrorCode::SerializationError, "serialize metadata map")
        })
    }
}

#[async_trait::async_trait]
impl MediaMetadataStore for PostgresMetadataStore {
    async fn replace_read_grants(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        users: &[String],
    ) -> Result<()> {
        let app = crate::domain::service::require_tenant(ctx)?;
        let work = async {
            let mut tx = self.pool.begin().await?;
            sqlx::query("SET LOCAL lock_timeout='2s'").execute(&mut *tx).await?;
            sqlx::query("SET LOCAL statement_timeout='3s'").execute(&mut *tx).await?;
            let exists: Option<String> = sqlx::query_scalar("SELECT file_id FROM media_assets WHERE tenant_id=$1 AND file_id=$2 FOR UPDATE")
                .bind(app).bind(file_id).fetch_optional(&mut *tx).await?;
            if exists.is_none() { return Ok::<_,sqlx::Error>(false); }
            let existing: Vec<String> = sqlx::query_scalar("SELECT user_id FROM media_read_grants WHERE tenant_id=$1 AND file_id=$2 ORDER BY user_id")
                .bind(app).bind(file_id).fetch_all(&mut *tx).await?;
            if existing != users {
                sqlx::query("DELETE FROM media_read_grants WHERE tenant_id=$1 AND file_id=$2").bind(app).bind(file_id).execute(&mut *tx).await?;
                for user in users {
                    sqlx::query("INSERT INTO media_read_grants(tenant_id,file_id,user_id) VALUES($1,$2,$3)")
                        .bind(app).bind(file_id).bind(user).execute(&mut *tx).await?;
                }
                flare_im_service_kit::audit_effects::durable::record(&mut tx,app,"media_read_grants",file_id,"replace").await?;
            }
            tx.commit().await?;
            Ok(true)
        }.await.map_err(|_|FlareError::system("media grants transaction unavailable"))?;
        if !work {
            return Err(flare_server_core::flare_err!(
                ErrorCode::MessageNotFound,
                "media asset not found"
            ));
        }
        Ok(())
    }
    async fn has_read_grant(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        user: &str,
    ) -> Result<bool> {
        let app = crate::domain::service::require_tenant(ctx)?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM media_read_grants WHERE tenant_id=$1 AND file_id=$2 AND user_id=$3)")
            .bind(app).bind(file_id).bind(user).fetch_one(self.pool()).await.map_err(|_|FlareError::system("media grants unavailable"))
    }
    async fn list_registered(
        &self,
        ctx: &flare_server_core::context::Context,
        prefix: &str,
        after: &str,
        limit: u32,
    ) -> Result<Vec<MediaFileMetadata>> {
        let app = crate::domain::service::require_tenant(ctx)?;
        let sql = format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_assets WHERE tenant_id=$1 AND file_id>$2 AND starts_with(metadata->>'storage_path',$3) ORDER BY file_id LIMIT $4"
        );
        sqlx::query_as::<_, MediaAssetRow>(&sql)
            .bind(app)
            .bind(after)
            .bind(prefix)
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await
            .map_err(|_| FlareError::system("media inventory unavailable"))?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }
    async fn save_metadata(
        &self,
        ctx: &flare_server_core::context::Context,
        metadata: &MediaFileMetadata,
    ) -> Result<()> {
        let metadata_json = Self::metadata_to_json(&metadata.metadata)?;

        let tenant_id = crate::domain::service::require_tenant(ctx)?;

        sqlx::query(
            r#"
            INSERT INTO media_assets (
                tenant_id,
                file_id,
                file_name,
                mime_type,
                file_size,
                url,
                cdn_url,
                md5,
                sha256,
                metadata,
                uploaded_at,
                reference_count,
                status,
                grace_expires_at,
                access_type,
                owner_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (tenant_id, file_id) DO UPDATE SET
                file_name = EXCLUDED.file_name,
                mime_type = EXCLUDED.mime_type,
                file_size = EXCLUDED.file_size,
                url = EXCLUDED.url,
                cdn_url = EXCLUDED.cdn_url,
                md5 = EXCLUDED.md5,
                sha256 = EXCLUDED.sha256,
                metadata = EXCLUDED.metadata,
                uploaded_at = EXCLUDED.uploaded_at,
                reference_count = EXCLUDED.reference_count,
                status = EXCLUDED.status,
                grace_expires_at = EXCLUDED.grace_expires_at,
                access_type = EXCLUDED.access_type
            "#,
        )
        .bind(tenant_id)
        .bind(&metadata.file_id)
        .bind(&metadata.file_name)
        .bind(&metadata.mime_type)
        .bind(metadata.file_size)
        .bind(&metadata.url)
        .bind(&metadata.cdn_url)
        .bind(&metadata.md5)
        .bind(&metadata.sha256)
        .bind(metadata_json)
        .bind(metadata.uploaded_at)
        .bind(metadata.reference_count as i64)
        .bind(MediaAssetRow::status_to_str(metadata.status))
        .bind(metadata.grace_expires_at)
        .bind(MediaAssetRow::access_type_to_str(metadata.access_type))
        .bind(&metadata.owner_id)
        .execute(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to persist media metadata",
            )
        })?;

        Ok(())
    }

    async fn load_metadata(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Option<MediaFileMetadata>> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let row = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
                owner_id,
                file_id,
                file_name,
                mime_type,
                file_size,
                url,
                cdn_url,
                md5,
                sha256,
                metadata,
                uploaded_at,
                reference_count,
                status,
                grace_expires_at,
                access_type
            FROM media_assets
            WHERE tenant_id = $1 AND file_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(file_id)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "failed to load media metadata")
        })?;

        match row {
            Some(row) => Ok(Some(row.try_into()?)),
            None => Ok(None),
        }
    }

    async fn load_by_hash(
        &self,
        ctx: &flare_server_core::context::Context,
        sha256: &str,
    ) -> Result<Option<MediaFileMetadata>> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let row = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
                owner_id,
                file_id,
                file_name,
                mime_type,
                file_size,
                url,
                cdn_url,
                md5,
                sha256,
                metadata,
                uploaded_at,
                reference_count,
                status,
                grace_expires_at,
                access_type
            FROM media_assets
            WHERE tenant_id = $1
              AND sha256 = $2
              AND status <> 'soft_deleted'
            ORDER BY
                CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                uploaded_at DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(sha256)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to load media metadata by hash",
            )
        })?;

        match row {
            Some(row) => Ok(Some(row.try_into()?)),
            None => Ok(None),
        }
    }

    async fn delete_metadata(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<()> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        sqlx::query("DELETE FROM media_references WHERE tenant_id = $1 AND file_id = $2")
            .bind(tenant_id)
            .bind(file_id)
            .execute(self.pool())
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "failed to delete media references",
                )
            })?;

        sqlx::query("DELETE FROM media_assets WHERE tenant_id = $1 AND file_id = $2")
            .bind(tenant_id)
            .bind(file_id)
            .execute(self.pool())
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "failed to delete media asset")
            })?;

        Ok(())
    }

    async fn list_orphaned_assets(
        &self,
        ctx: &flare_server_core::context::Context,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<MediaFileMetadata>> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let rows = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
                owner_id,
                file_id,
                file_name,
                mime_type,
                file_size,
                url,
                cdn_url,
                md5,
                sha256,
                metadata,
                uploaded_at,
                reference_count,
                status,
                grace_expires_at,
                access_type
            FROM media_assets
            WHERE tenant_id = $1
              AND reference_count = 0
              AND status = 'pending'
              AND grace_expires_at IS NOT NULL
              AND grace_expires_at <= $2
              AND (
                    LOWER(COALESCE(metadata->>'media_lifecycle_scope', '')) IN ('message', 'messages', 'im_message', 'im-message')
                 OR LOWER(COALESCE(metadata->>'lifecycle_scope', '')) IN ('message', 'messages', 'im_message', 'im-message')
                 OR LOWER(COALESCE(metadata->>'media_scope', '')) IN ('message', 'messages', 'im_message', 'im-message')
                 OR LOWER(COALESCE(metadata->>'media_usage', '')) IN ('message', 'messages', 'im_message', 'im-message')
                 OR LOWER(COALESCE(metadata->>'usage', '')) IN ('message', 'messages', 'im_message', 'im-message')
                 OR LOWER(COALESCE(metadata->>'namespace', '')) IN ('message', 'messages', 'im_message', 'im-message')
                 OR LOWER(COALESCE(metadata->>'business_tag', '')) IN ('message', 'messages', 'im_message', 'im-message')
              )
            ORDER BY grace_expires_at, file_id
            LIMIT $3
            "#,
        )
        .bind(tenant_id)
        .bind(before)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to list orphaned media assets",
            )
        })?;

        rows.into_iter().map(MediaFileMetadata::try_from).collect()
    }

    async fn claim_orphaned_asset(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        before: DateTime<Utc>,
    ) -> Result<Option<Box<dyn OrphanAssetClaim>>> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let db_error = |e: sqlx::Error, action: &'static str| {
            map_infra_error(e, ErrorCode::DatabaseError, action)
        };
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| db_error(e, "failed to begin orphaned media claim"))?;
        // 先拿行锁。进行中的引用插入持有该行的 FOR KEY SHARE（create_reference 显式加锁，
        // 外键检查也会加），这里会等它提交；之后开始的插入则等本次认领结束。
        let row = sqlx::query_as::<_, MediaAssetRow>(&format!(
            "SELECT {MEDIA_ASSET_COLUMNS} FROM media_assets
             WHERE tenant_id = $1 AND file_id = $2
               AND reference_count = 0
               AND status = 'pending'
               AND grace_expires_at IS NOT NULL
               AND grace_expires_at <= $3
             FOR UPDATE"
        ))
        .bind(tenant_id)
        .bind(file_id)
        .bind(before)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "failed to lock orphaned media asset"))?;
        let Some(row) = row else {
            tx.rollback()
                .await
                .map_err(|e| db_error(e, "failed to release orphaned media claim"))?;
            return Ok(None);
        };
        // READ COMMITTED 下这条语句的快照取于拿锁之后：拿锁前已提交的引用都可见，会取消认领。
        let deleted = sqlx::query(
            "DELETE FROM media_assets a
             WHERE a.tenant_id = $1 AND a.file_id = $2
               AND NOT EXISTS (
                   SELECT 1 FROM media_references r
                   WHERE r.tenant_id = a.tenant_id AND r.file_id = a.file_id
               )",
        )
        .bind(tenant_id)
        .bind(file_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "failed to claim orphaned media asset"))?
        .rows_affected();
        if deleted == 0 {
            tx.rollback()
                .await
                .map_err(|e| db_error(e, "failed to release orphaned media claim"))?;
            return Ok(None);
        }
        // Enqueue before touching object storage. Evidence and metadata deletion remain
        // uncommitted until claim.commit(); a failed audit tail prevents external deletion.
        flare_im_service_kit::audit_effects::durable::record(
            &mut tx,
            tenant_id,
            "media_asset_metadata",
            file_id,
            "reclaim",
        )
        .await
        .map_err(|_| FlareError::system("media source audit transaction unavailable"))?;
        Ok(Some(Box::new(PostgresOrphanClaim {
            tx,
            asset: row.try_into()?,
        })))
    }

    async fn update_status(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        status: MediaAssetStatus,
        grace_expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        sqlx::query(
            r#"
            UPDATE media_assets
            SET status = $2,
                grace_expires_at = $3
            WHERE tenant_id = $4 AND file_id = $1
            "#,
        )
        .bind(file_id)
        .bind(MediaAssetRow::status_to_str(status))
        .bind(grace_expires_at)
        .bind(tenant_id)
        .execute(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to update media asset status",
            )
        })?;

        Ok(())
    }
}

// 添加 MediaReferenceStore trait 的实现
#[async_trait::async_trait]
impl MediaReferenceStore for PostgresMetadataStore {
    async fn create_reference(&self, reference: &MediaReference) -> Result<bool> {
        let metadata_json = Self::metadata_to_json(&reference.metadata)?;
        let tenant_id = crate::domain::service::validate_tenant(&reference.tenant_id)?;
        if tenant_id.is_empty() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "tenant_id is required for media reference"
            ));
        }

        let mut tx = self.pool().begin().await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to begin media reference creation",
            )
        })?;
        // 在本事务内钉住资产行：持有 FOR UPDATE 的孤儿认领会让这里等待并随后看到删除；
        // 被钉住的资产无法被认领。
        let asset_present: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM media_assets WHERE tenant_id = $1 AND file_id = $2 FOR KEY SHARE",
        )
        .bind(tenant_id)
        .bind(&reference.file_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to lock media asset for reference",
            )
        })?;
        if asset_present.is_none() {
            tx.rollback().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "failed to abort media reference creation",
                )
            })?;
            return Err(flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "metadata not found",
                reference.file_id.clone()
            ));
        }

        let result = sqlx::query(
            r#"
            INSERT INTO media_references (
                tenant_id,
                reference_id,
                file_id,
                namespace,
                owner_id,
                business_tag,
                metadata,
                created_at,
                expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, reference_id) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(&reference.reference_id)
        .bind(&reference.file_id)
        .bind(&reference.namespace)
        .bind(&reference.owner_id)
        .bind(&reference.business_tag)
        .bind(metadata_json)
        .bind(reference.created_at)
        .bind(reference.expires_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to create media reference",
            )
        })?;
        tx.commit().await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to commit media reference",
            )
        })?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_reference(
        &self,
        ctx: &flare_server_core::context::Context,
        reference_id: &str,
    ) -> Result<bool> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let result =
            sqlx::query("DELETE FROM media_references WHERE tenant_id = $1 AND reference_id = $2")
                .bind(tenant_id)
                .bind(reference_id)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        "failed to delete media reference",
                    )
                })?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_any_reference(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Option<String>> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let row =
            sqlx::query("DELETE FROM media_references WHERE tenant_id = $1 AND file_id = $2 RETURNING reference_id")
                .bind(tenant_id)
                .bind(file_id)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "failed to delete any media reference")
                })?;

        match row {
            Some(row) => {
                let reference_id: String = row.try_get("reference_id").map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        "failed to get reference_id from row",
                    )
                })?;
                Ok(Some(reference_id))
            }
            None => Ok(None),
        }
    }

    async fn delete_all_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<u64> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let result =
            sqlx::query("DELETE FROM media_references WHERE tenant_id = $1 AND file_id = $2")
                .bind(tenant_id)
                .bind(file_id)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        "failed to delete all media references",
                    )
                })?;

        Ok(result.rows_affected())
    }

    async fn list_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Vec<MediaReference>> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let rows = sqlx::query_as::<_, MediaReferenceRow>(
            r#"
            SELECT
                tenant_id,
                reference_id,
                file_id,
                namespace,
                owner_id,
                business_tag,
                metadata,
                created_at,
                expires_at
            FROM media_references
            WHERE tenant_id = $1 AND file_id = $2
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .bind(file_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to list media references",
            )
        })?;

        rows.into_iter().map(MediaReference::try_from).collect()
    }

    async fn count_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<u64> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let row = sqlx::query(
            "SELECT COUNT(*) as count FROM media_references WHERE tenant_id = $1 AND file_id = $2",
        )
        .bind(tenant_id)
        .bind(file_id)
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "failed to count media references",
            )
        })?;

        let count: i64 = row.try_get("count").map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "failed to get count from row")
        })?;
        Ok(count as u64)
    }

    async fn reference_exists(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        namespace: &str,
        owner_id: &str,
        business_tag: Option<&str>,
    ) -> Result<bool> {
        let tenant_id = crate::domain::service::require_tenant(ctx)?;
        let count: i64 = if let Some(tag) = business_tag {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM media_references WHERE tenant_id = $1 AND file_id = $2 AND namespace = $3 AND owner_id = $4 AND business_tag = $5"
            )
            .bind(tenant_id)
            .bind(file_id)
            .bind(namespace)
            .bind(owner_id)
            .bind(tag)
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "failed to check if media reference exists")
            })?
        } else {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM media_references WHERE tenant_id = $1 AND file_id = $2 AND namespace = $3 AND owner_id = $4 AND business_tag IS NULL"
            )
            .bind(tenant_id)
            .bind(file_id)
            .bind(namespace)
            .bind(owner_id)
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "failed to check if media reference exists")
            })?
        };

        Ok(count > 0)
    }
}
