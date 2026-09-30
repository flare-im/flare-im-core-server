use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_server_core::{context::Context, error::Result};

use crate::domain::model::{
    MediaAssetStatus, MediaFileMetadata, MediaReference, ObjectStat, UploadContext, UploadSession,
    UploadedPartRecord,
};

/// 对象存储仓储接口
///
/// **重要说明**：使用 `#[async_trait::async_trait]` 宏是因为该 trait 需要作为 trait 对象（dyn Trait）使用，
/// 以支持依赖注入和运行时多态。虽然 Rust 2024 支持原生 `async fn in traits`，但原生实现不支持 dyn 兼容性。
///
/// 这是性能与灵活性的权衡：
/// - **性能影响**：动态分发有少量性能开销（约 5-10%）
/// - **灵活性收益**：支持运行时切换实现、依赖注入、测试 Mock
///
/// 如果需要极致性能，建议改用泛型参数而非 trait 对象。
#[async_trait::async_trait]
pub trait MediaObjectRepository: Send + Sync {
    async fn put_object(&self, context: &UploadContext<'_>) -> Result<String>;
    async fn delete_object(&self, object_path: &str) -> Result<()>;
    async fn presign_object(&self, object_path: &str, expires_in: i64) -> Result<String>;
    async fn presign_put_object(
        &self,
        object_path: &str,
        content_type: &str,
        expires_in: i64,
    ) -> Result<String> {
        let _ = (object_path, content_type, expires_in);
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "direct upload is not supported by current object store"
        ))
    }
    async fn create_multipart_upload(
        &self,
        object_path: &str,
        content_type: &str,
    ) -> Result<String> {
        let _ = (object_path, content_type);
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "multipart direct upload is not supported by current object store"
        ))
    }
    async fn presign_upload_part(
        &self,
        object_path: &str,
        upload_id: &str,
        part_number: u32,
        expires_in: i64,
    ) -> Result<String> {
        let _ = (object_path, upload_id, part_number, expires_in);
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "multipart direct upload is not supported by current object store"
        ))
    }
    async fn complete_multipart_upload(
        &self,
        object_path: &str,
        upload_id: &str,
        parts: &[UploadedPartRecord],
    ) -> Result<()> {
        let _ = (object_path, upload_id, parts);
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "multipart direct upload is not supported by current object store"
        ))
    }
    async fn abort_multipart_upload(&self, object_path: &str, upload_id: &str) -> Result<()> {
        let _ = (object_path, upload_id);
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "multipart direct upload is not supported by current object store"
        ))
    }
    async fn stat_object(&self, object_path: &str) -> Result<ObjectStat> {
        let _ = object_path;
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "object stat is not supported by current object store"
        ))
    }
    fn build_object_key_for(
        &self,
        tenant_id: &str,
        file_id: &str,
        _file_name: &str,
        _file_category: &str,
    ) -> String {
        format!(
            "{}/{}",
            crate::domain::model::tenant_storage_segment(tenant_id),
            file_id
        )
    }
    fn base_url(&self) -> Option<String>;
    fn cdn_base_url(&self) -> Option<String>;
    fn use_presigned_urls(&self) -> bool;
    fn bucket_name(&self) -> Option<String> {
        None
    }
    fn storage_provider(&self) -> Option<String> {
        None
    }
}

/// 媒体元数据存储仓储接口
///
/// 使用 `#[async_trait::async_trait]` 宏以支持 trait 对象（详见 `MediaObjectRepository` 说明）
#[async_trait::async_trait]
pub trait MediaMetadataStore: Send + Sync {
    async fn save_metadata(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()>;
    async fn load_metadata(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Option<MediaFileMetadata>>;
    async fn load_by_hash(
        &self,
        ctx: &flare_server_core::context::Context,
        sha256: &str,
    ) -> Result<Option<MediaFileMetadata>>;
    async fn delete_metadata(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<()>;
    async fn list_orphaned_assets(
        &self,
        ctx: &Context,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<MediaFileMetadata>>;
    /// 原子认领一个到期孤儿。`list_orphaned_assets` 只是快照；认领时锁住资产行并在锁内
    /// 重新确认“无引用计数、待归档、宽限期已过、且没有任何引用记录”，然后删除元数据但
    /// 暂不提交。调用方删完对象后 `commit`；丢弃认领即回滚，元数据保持原样可重试。
    /// 与引用创建互斥：先提交的引用会让认领放弃；认领持锁期间的新引用会等待，
    /// 认领提交后因资产不存在而失败。
    async fn claim_orphaned_asset(
        &self,
        ctx: &Context,
        file_id: &str,
        before: DateTime<Utc>,
    ) -> Result<Option<Box<dyn OrphanAssetClaim>>>;
    async fn update_status(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        status: MediaAssetStatus,
        grace_expires_at: Option<DateTime<Utc>>,
    ) -> Result<()>;
}

/// 一次尚未提交的孤儿认领；持有期间资产行对引用创建保持锁定。
#[async_trait::async_trait]
pub trait OrphanAssetClaim: Send {
    fn asset(&self) -> &MediaFileMetadata;
    /// 提交元数据删除。未调用即丢弃时回滚认领。
    async fn commit(self: Box<Self>) -> Result<()>;
}

/// 媒体元数据缓存接口
///
/// 使用 `#[async_trait::async_trait]` 宏以支持 trait 对象（详见 `MediaObjectRepository` 说明）
#[async_trait::async_trait]
pub trait MediaMetadataCache: Send + Sync {
    async fn cache_metadata(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()>;
    async fn get_cached_metadata(
        &self,
        ctx: &Context,
        file_id: &str,
    ) -> Result<Option<MediaFileMetadata>>;
    async fn invalidate(&self, ctx: &Context, file_id: &str) -> Result<()>;
}

/// 本地存储接口
///
/// 使用 `#[async_trait::async_trait]` 宏以支持 trait 对象（详见 `MediaObjectRepository` 说明）
#[async_trait::async_trait]
pub trait MediaLocalStore: Send + Sync {
    async fn write(&self, context: &UploadContext<'_>) -> Result<String>;
    async fn read(&self, file_id: &str) -> Result<Vec<u8>>;
    async fn delete(&self, file_id: &str) -> Result<()>;
    fn base_url(&self) -> Option<String>;
}

/// 媒体引用存储仓储接口
///
/// 使用 `#[async_trait::async_trait]` 宏以支持 trait 对象（详见 `MediaObjectRepository` 说明）
#[async_trait::async_trait]
pub trait MediaReferenceStore: Send + Sync {
    async fn create_reference(&self, reference: &MediaReference) -> Result<bool>;
    async fn delete_reference(
        &self,
        ctx: &flare_server_core::context::Context,
        reference_id: &str,
    ) -> Result<bool>;
    async fn delete_any_reference(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Option<String>>;
    async fn delete_all_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<u64>;
    async fn list_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<Vec<MediaReference>>;
    async fn count_references(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
    ) -> Result<u64>;
    async fn reference_exists(
        &self,
        ctx: &flare_server_core::context::Context,
        file_id: &str,
        namespace: &str,
        owner_id: &str,
        business_tag: Option<&str>,
    ) -> Result<bool>;
}

/// 分片上传会话存储接口
///
/// 使用 `#[async_trait::async_trait]` 宏以支持 trait 对象（详见 `MediaObjectRepository` 说明）
#[async_trait::async_trait]
pub trait UploadSessionStore: Send + Sync {
    async fn create_session(&self, session: &UploadSession) -> Result<()>;
    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>>;
    async fn upsert_session(&self, session: &UploadSession) -> Result<()>;
    async fn delete_session(&self, upload_id: &str) -> Result<()>;
}

pub type MetadataStoreRef = Arc<dyn MediaMetadataStore>;
pub type MetadataCacheRef = Arc<dyn MediaMetadataCache>;
pub type ObjectRepositoryRef = Arc<dyn MediaObjectRepository>;
pub type LocalStoreRef = Arc<dyn MediaLocalStore>;
pub type ReferenceStoreRef = Arc<dyn MediaReferenceStore>;
pub type UploadSessionStoreRef = Arc<dyn UploadSessionStore>;
