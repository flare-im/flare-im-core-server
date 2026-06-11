use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::{
    context::Context,
    error::{ErrorCode, Result, map_infra_error},
};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::domain::model::{FileAccessType, MediaAssetStatus, MediaFileMetadata};
use crate::domain::repository::MediaMetadataCache;

#[derive(Debug, Serialize, Deserialize)]
struct CachedMetadata {
    file_id: String,
    file_name: String,
    mime_type: String,
    file_size: i64,
    url: String,
    cdn_url: String,
    md5: Option<String>,
    sha256: Option<String>,
    metadata: HashMap<String, String>,
    uploaded_at: i64,
    reference_count: u64,
    status: String,
    grace_expires_at: Option<i64>,
    access_type: String,
    storage_bucket: Option<String>,
    storage_path: Option<String>,
}

#[derive(Clone)]
pub struct RedisMetadataCache {
    namespace: String,
    connection: Arc<Mutex<ConnectionManager>>,
}

impl RedisMetadataCache {
    pub async fn new(redis_url: &str, namespace: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis client open"))?;
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis connection manager"))?;
        Ok(Self {
            namespace: namespace.into(),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn key(&self, ctx: &Context, file_id: &str) -> String {
        let tenant_id = normalize_tenant_id(ctx.tenant_id().unwrap_or("0"));
        format!(
            "{}:media:metadata:{}:{}",
            self.namespace, tenant_id, file_id
        )
    }
}

#[async_trait::async_trait]
impl MediaMetadataCache for RedisMetadataCache {
    async fn cache_metadata(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
        let cached = CachedMetadata {
            file_id: metadata.file_id.clone(),
            file_name: metadata.file_name.clone(),
            mime_type: metadata.mime_type.clone(),
            file_size: metadata.file_size,
            url: metadata.url.clone(),
            cdn_url: metadata.cdn_url.clone(),
            md5: metadata.md5.clone(),
            sha256: metadata.sha256.clone(),
            metadata: metadata.metadata.clone(),
            uploaded_at: metadata.uploaded_at.timestamp_millis(),
            reference_count: metadata.reference_count,
            status: metadata.status.as_str().to_string(),
            grace_expires_at: metadata.grace_expires_at.map(|t| t.timestamp_millis()),
            access_type: match metadata.access_type {
                FileAccessType::Public => "public".to_string(),
                FileAccessType::Private => "private".to_string(),
            },
            storage_bucket: metadata.storage_bucket().map(|value| value.to_string()),
            storage_path: metadata.storage_path().map(|value| value.to_string()),
        };

        let payload = serde_json::to_string(&cached).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::SerializationError,
                "serialize cached metadata",
            )
        })?;
        let mut conn = self.connection.lock().await;
        let _: () = conn
            .set(self.key(ctx, &metadata.file_id), payload)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to cache media metadata in redis",
                )
            })?;
        Ok(())
    }

    async fn get_cached_metadata(
        &self,
        ctx: &Context,
        file_id: &str,
    ) -> Result<Option<MediaFileMetadata>> {
        let mut conn = self.connection.lock().await;
        let value: Option<String> = conn.get(self.key(ctx, file_id)).await.map_err(|e| {
            map_infra_error(e, ErrorCode::NetworkError, "redis get cached metadata")
        })?;
        if let Some(value) = value {
            let cached: CachedMetadata = serde_json::from_str(&value).map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DeserializationError,
                    "deserialize cached metadata",
                )
            })?;
            let status = MediaAssetStatus::from_str(&cached.status).map_err(|_| {
                map_infra_error(
                    format!("invalid status {}", cached.status),
                    ErrorCode::DeserializationError,
                    "invalid media asset status",
                )
            })?;

            let access_type = match cached.access_type.as_str() {
                "public" => FileAccessType::Public,
                "private" => FileAccessType::Private,
                _ => FileAccessType::default(),
            };

            Ok(Some(MediaFileMetadata {
                file_id: cached.file_id,
                file_name: cached.file_name,
                mime_type: cached.mime_type,
                file_size: cached.file_size,
                url: cached.url,
                cdn_url: cached.cdn_url,
                md5: cached.md5,
                sha256: cached.sha256,
                metadata: cached.metadata,
                uploaded_at: chrono::DateTime::from_timestamp_millis(cached.uploaded_at)
                    .unwrap_or_else(chrono::Utc::now),
                reference_count: cached.reference_count,
                status,
                grace_expires_at: cached
                    .grace_expires_at
                    .and_then(chrono::DateTime::from_timestamp_millis),
                access_type,
                storage_bucket: cached.storage_bucket.clone(),
                storage_path: cached.storage_path.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    async fn invalidate(&self, ctx: &Context, file_id: &str) -> Result<()> {
        let mut conn = self.connection.lock().await;
        let _: () = conn
            .del(self.key(ctx, file_id))
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis delete cache key"))?;
        Ok(())
    }
}
