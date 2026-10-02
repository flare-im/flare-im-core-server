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
    /// 直传单次 PUT 的预签名地址。`content_length` 必须进签名（SigV4 的 signed headers），
    /// 存储据此拒绝长度与声明不符的请求体，调用方不能借这个地址写入超出声明大小的对象。
    async fn presign_put_object(
        &self,
        object_path: &str,
        content_type: &str,
        content_length: i64,
        expires_in: i64,
    ) -> Result<String> {
        let _ = (object_path, content_type, content_length, expires_in);
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
    /// 分片直传单片的预签名地址。`content_length` 是这一片应有的长度，与单次 PUT 一样签进
    /// 地址：存储拒绝长度不符的分片，超大的分片在上传时就被挡下，不必等到完成时核对。
    async fn presign_upload_part(
        &self,
        object_path: &str,
        upload_id: &str,
        part_number: u32,
        content_length: i64,
        expires_in: i64,
    ) -> Result<String> {
        let _ = (
            object_path,
            upload_id,
            part_number,
            content_length,
            expires_in,
        );
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "multipart direct upload is not supported by current object store"
        ))
    }
    /// 调用方提交的分片与存储里的不符（分片没上传、ETag 不对、顺序不对）是调用方错误，
    /// 回 `InvalidParameter`；只有存储本身的故障才是不可用。
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
    /// 幂等：上传已完成或已中止（存储报 NoSuchUpload）时也返回成功，回收任务据此收尾。
    async fn abort_multipart_upload(&self, object_path: &str, upload_id: &str) -> Result<()> {
        let _ = (object_path, upload_id);
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ConfigurationError,
            "multipart direct upload is not supported by current object store"
        ))
    }
    /// 对象不存在时返回 `Ok(None)`：调用方据此区分「客户端还没写」与存储故障。
    async fn stat_object(&self, object_path: &str) -> Result<Option<ObjectStat>> {
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
    /// 已过期（`expires_at` 已过）的会话按不存在返回，但记录留给回收任务：它还要凭记录里的
    /// 对象 key、存储侧分片上传 id 与分块目录释放资源。
    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>>;
    /// 读出记录本身，不论是否过期；记录不存在时为 `None`。回收任务在释放资源前用它复查
    /// 领到的会话是否已被续期。默认退回 [`Self::get_session`]：只看得到未过期的会话，
    /// 看不到时回收任务按领取时的快照处理，结论相同。
    async fn get_session_record(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        self.get_session(upload_id).await
    }
    async fn upsert_session(&self, session: &UploadSession) -> Result<()>;
    /// 在最新记录上改几个字段：读出记录（不论是否过期）交给 `apply`，`apply` 返回
    /// `Ok(true)` 才写回；读出之后记录被别人改过就按新记录重来，不会盖掉其间的写入（例如
    /// 刚提交的分片）。记录不存在时为 `Ok(None)`；`apply` 拒绝时原样返回它的错误、不写入。
    /// 返回 `apply` 处理过的记录。
    ///
    /// 默认实现只是读改写，不防并发，供测试替身使用；Redis、内存与兜底存储都覆盖为原子
    /// 操作。
    async fn update_session(
        &self,
        upload_id: &str,
        apply: &SessionUpdate<'_>,
    ) -> Result<Option<UploadSession>> {
        let Some(mut session) = self.get_session_record(upload_id).await? else {
            return Ok(None);
        };
        if apply(&mut session)? {
            self.upsert_session(&session).await?;
        }
        Ok(Some(session))
    }
    async fn delete_session(&self, upload_id: &str) -> Result<()>;
    /// 领取 `expires_at` 早于 `expired_before` 的会话，至多 `limit` 个，并把它们租给调用方到
    /// `lease_until`：租期内其他回收者（含其他副本）领不到同一会话。资源释放成功后调用方
    /// `delete_session` 收尾；释放失败就什么也不做，租期过后会被重新领取。
    async fn claim_expired_sessions(
        &self,
        expired_before: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<UploadSession>>;
}

/// [`UploadSessionStore::update_session`] 的改写函数：返回是否写回。可能对同一会话的
/// 较新记录再调用一次，所以只能根据传入的记录做决定。
pub type SessionUpdate<'a> = dyn Fn(&mut UploadSession) -> Result<bool> + Send + Sync + 'a;

pub type MetadataStoreRef = Arc<dyn MediaMetadataStore>;
pub type MetadataCacheRef = Arc<dyn MediaMetadataCache>;
pub type ObjectRepositoryRef = Arc<dyn MediaObjectRepository>;
pub type LocalStoreRef = Arc<dyn MediaLocalStore>;
pub type ReferenceStoreRef = Arc<dyn MediaReferenceStore>;
pub type UploadSessionStoreRef = Arc<dyn UploadSessionStore>;
