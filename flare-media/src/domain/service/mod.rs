use chrono::{Duration, Utc};
use flare_server_core::context::{Context, ContextExt};
use md5::compute as md5_compute;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::instrument;
use uuid::Uuid;

use crate::domain::model::{
    DirectUploadSessionState, DirectUploadTransportKind, EXTERNAL_MEDIA_LIFECYCLE_SCOPE,
    FILE_CATEGORY_METADATA_KEY, FileAccessType, MEDIA_LIFECYCLE_SCOPE_METADATA_KEY,
    MESSAGE_MEDIA_LIFECYCLE_SCOPE, MediaAssetStatus, MediaDomainConfig, MediaFileMetadata,
    MediaReference, MediaReferenceScope, MultipartChunkPayload, MultipartUploadInit,
    MultipartUploadSession, ObjectStat, PresignedUploadPartUrl, PresignedUrl,
    STORAGE_BUCKET_METADATA_KEY, STORAGE_PATH_METADATA_KEY, UploadContext, UploadSession,
    UploadSessionStatus, UploadedPartRecord, infer_file_category, is_absolute_http_url,
};
use crate::domain::repository::{
    LocalStoreRef, MetadataCacheRef, MetadataStoreRef, ObjectRepositoryRef, ReferenceStoreRef,
    UploadSessionStoreRef,
};
use flare_im_usage::UsageRecorder;
use flare_server_core::error::{ErrorCode, Result, map_context_error, map_infra_error};
pub use reclaim::{
    UPLOAD_SESSION_RECLAIM_BATCH, UPLOAD_SESSION_RECLAIM_GRACE_SECONDS,
    UPLOAD_SESSION_RECLAIM_INTERVAL, UPLOAD_SESSION_RECLAIM_LEASE_SECONDS, UploadReclaimReport,
};
pub(crate) use tenant::{require_owner, require_tenant, validate_tenant};

const DIRECT_SINGLE_PUT_THRESHOLD_BYTES: i64 = 8 * 1024 * 1024;
const DIRECT_MULTIPART_MIN_PART_SIZE_BYTES: i64 = 8 * 1024 * 1024;

/// 元数据里的文件大小（i64）换成用量字节数；负值按 0 计。
fn usage_bytes(file_size: i64) -> u64 {
    u64::try_from(file_size).unwrap_or(0)
}

type UploadContextData<'a> = (
    String,
    String,
    HashMap<String, String>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

pub struct MediaService {
    object_repo: Option<ObjectRepositoryRef>,
    metadata_store: Option<MetadataStoreRef>,
    metadata_cache: Option<MetadataCacheRef>,
    reference_store: Option<ReferenceStoreRef>,
    upload_conversation_store: Option<UploadSessionStoreRef>,
    local_store: Option<LocalStoreRef>,
    config: MediaDomainConfig,
    /// 直传完成的领取租期；合并与登记期间按它的五分之一续租。
    direct_completion_lease: Duration,
    /// 用量计数点：新对象登记（`media.bytes_stored` added）、对象删除（removed）、对象存储文件
    /// 的下载签名或本地文件读出（`media.bytes_egress`）。去重复用已有对象不计。
    usage: UsageRecorder,
}

impl MediaService {
    pub fn new(
        object_repo: Option<ObjectRepositoryRef>,
        metadata_store: Option<MetadataStoreRef>,
        reference_store: Option<ReferenceStoreRef>,
        metadata_cache: Option<MetadataCacheRef>,
        upload_conversation_store: Option<UploadSessionStoreRef>,
        local_store: Option<LocalStoreRef>,
        config: MediaDomainConfig,
    ) -> Self {
        if let Err(err) = std::fs::create_dir_all(&config.chunk_root_dir) {
            tracing::warn!(
                error = %err,
                path = config.chunk_root_dir.display().to_string(),
                "failed to create chunk directory"
            );
        }

        Self {
            object_repo,
            metadata_store,
            metadata_cache,
            reference_store,
            upload_conversation_store,
            local_store,
            config,
            direct_completion_lease: Duration::seconds(direct::DIRECT_COMPLETION_LEASE_SECONDS),
            usage: UsageRecorder::disabled(),
        }
    }

    pub fn with_usage(mut self, usage: UsageRecorder) -> Self {
        self.usage = usage;
        self
    }

    /// 测试用：缩短直传完成的领取租期，在毫秒级观察续租。
    #[cfg(test)]
    pub(crate) fn set_direct_completion_lease(&mut self, lease: Duration) {
        self.direct_completion_lease = lease;
    }
}

#[cfg(test)]
mod cdn_url_tests;
mod direct;
mod helpers;
#[cfg(test)]
pub(crate) mod isolation_tests;
mod multipart;
mod reclaim;
mod references;
mod storage;
mod tenant;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod upload_rejection_tests;
#[cfg(test)]
mod upload_url_tests;
