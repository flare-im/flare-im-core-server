use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::error::{ErrorCode, FlareError, Result, map_infra_error};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool, Row};

use crate::domain::model::{
    FileAccessType, MediaAssetStatus, MediaFileMetadata, MediaReference,
    STORAGE_BUCKET_METADATA_KEY, STORAGE_PATH_METADATA_KEY,
};
use crate::domain::repository::{MediaMetadataStore, MediaReferenceStore};

const DEFAULT_MAX_CONNECTIONS: u32 = 10;

#[derive(Debug, FromRow)]
struct MediaAssetRow {
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
            Some(value) => serde_json::from_value::<HashMap<String, String>>(value).map_err(|e| {
                map_infra_error(e, ErrorCode::DeserializationError, "failed to deserialize metadata")
            })?,
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
            Some(value) => serde_json::from_value::<HashMap<String, String>>(value).map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DeserializationError,
                    "failed to deserialize reference metadata",
                )
            })?,
            None => HashMap::new(),
        };

        Ok(MediaReference {
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

#[derive(Clone)]
pub struct PostgresMetadataStore {
    pool: Arc<PgPool>,
}

impl PostgresMetadataStore {
    pub async fn new(config: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
            .connect(config)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to connect to postgres"))?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn metadata_to_json(metadata: &HashMap<String, String>) -> Result<Value> {
        serde_json::to_value(metadata).map_err(|e| {
            map_infra_error(e, ErrorCode::SerializationError, "serialize metadata map")
        })
    }
}

#[async_trait::async_trait]
impl MediaMetadataStore for PostgresMetadataStore {
    async fn save_metadata(&self, metadata: &MediaFileMetadata) -> Result<()> {
        let metadata_json = Self::metadata_to_json(&metadata.metadata)?;

        // 从 metadata 中提取 tenant_id，如果没有则使用默认值（向后兼容）
        let tenant_id = metadata
            .metadata
            .get("tenant_id")
            .cloned()
            .unwrap_or_else(|| "default".to_string());

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
                access_type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
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
        .bind(&tenant_id)
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
        .execute(self.pool())
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to persist media metadata"))?;

        Ok(())
    }

    async fn load_metadata(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Option<MediaFileMetadata>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let row = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
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
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to load media metadata"))?;

        match row {
            Some(row) => Ok(Some(row.try_into()?)),
            None => Ok(None),
        }
    }

    async fn load_by_hash(&self, sha256: &str) -> Result<Option<MediaFileMetadata>> {
        let row = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
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
            WHERE sha256 = $1
            ORDER BY uploaded_at DESC
            LIMIT 1
            "#,
        )
        .bind(sha256)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "failed to load media metadata by hash")
        })?;

        match row {
            Some(row) => Ok(Some(row.try_into()?)),
            None => Ok(None),
        }
    }

    async fn delete_metadata(&self, file_id: &str) -> Result<()> {
        // 注意：delete_metadata 方法签名中没有 tenant_id，但为了数据安全，应该添加
        // 这里先使用子查询获取 tenant_id，或者需要修改方法签名
        // 暂时保持向后兼容，但建议后续添加 tenant_id 参数
        sqlx::query("DELETE FROM media_references WHERE file_id = $1")
            .bind(file_id)
            .execute(self.pool())
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "failed to delete media references")
            })?;

        sqlx::query("DELETE FROM media_assets WHERE file_id = $1")
            .bind(file_id)
            .execute(self.pool())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to delete media asset"))?;

        Ok(())
    }

    async fn list_orphaned_assets(&self, before: DateTime<Utc>) -> Result<Vec<MediaFileMetadata>> {
        let rows = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
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
            WHERE reference_count = 0
              AND grace_expires_at IS NOT NULL
              AND grace_expires_at <= $1
            "#,
        )
        .bind(before)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "failed to list orphaned media assets")
        })?;

        rows.into_iter().map(MediaFileMetadata::try_from).collect()
    }

    async fn update_status(
        &self,
        file_id: &str,
        status: MediaAssetStatus,
        grace_expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE media_assets
            SET status = $2,
                grace_expires_at = $3
            WHERE file_id = $1
            "#,
        )
        .bind(file_id)
        .bind(MediaAssetRow::status_to_str(status))
        .bind(grace_expires_at)
        .execute(self.pool())
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to update media asset status"))?;

        Ok(())
    }
}

// 添加 MediaReferenceStore trait 的实现
#[async_trait::async_trait]
impl MediaReferenceStore for PostgresMetadataStore {
    async fn create_reference(&self, reference: &MediaReference) -> Result<bool> {
        let metadata_json = Self::metadata_to_json(&reference.metadata)?;

        // 从 reference.metadata 中提取 tenant_id，如果没有则从 file_id 对应的 media_asset 中获取
        let tenant_id = if let Some(tenant_id) = reference.metadata.get("tenant_id") {
            tenant_id.clone()
        } else {
            // 从 media_assets 表中查询 tenant_id
            let row = sqlx::query("SELECT tenant_id FROM media_assets WHERE file_id = $1 LIMIT 1")
                .bind(&reference.file_id)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "failed to get tenant_id from media_assets")
                })?;

            if let Some(row) = row {
                row.get("tenant_id")
            } else {
                // 如果找不到，使用默认值（向后兼容）
                "default".to_string()
            }
        };

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
        .bind(&tenant_id)
        .bind(&reference.reference_id)
        .bind(&reference.file_id)
        .bind(&reference.namespace)
        .bind(&reference.owner_id)
        .bind(&reference.business_tag)
        .bind(metadata_json)
        .bind(reference.created_at)
        .bind(reference.expires_at)
        .execute(self.pool())
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to create media reference"))?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_reference(&self, reference_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM media_references WHERE reference_id = $1")
            .bind(reference_id)
            .execute(self.pool())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to delete media reference"))?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_any_reference(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Option<String>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
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
                    map_infra_error(e, ErrorCode::DatabaseError, "failed to get reference_id from row")
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
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let result =
            sqlx::query("DELETE FROM media_references WHERE tenant_id = $1 AND file_id = $2")
                .bind(tenant_id)
                .bind(file_id)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "failed to delete all media references")
                })?;

        Ok(result.rows_affected())
    }

    async fn list_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Vec<MediaReference>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let rows = sqlx::query_as::<_, MediaReferenceRow>(
            r#"
            SELECT
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
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to list media references"))?;

        rows.into_iter().map(MediaReference::try_from).collect()
    }

    async fn count_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<u64> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let row = sqlx::query(
            "SELECT COUNT(*) as count FROM media_references WHERE tenant_id = $1 AND file_id = $2",
        )
        .bind(tenant_id)
        .bind(file_id)
        .fetch_one(self.pool())
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "failed to count media references"))?;

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
        let tenant_id = ctx.tenant_id().unwrap_or("0");
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
