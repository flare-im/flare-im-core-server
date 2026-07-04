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
    MultipartUploadSession, PresignedUploadPartUrl, PresignedUrl, STORAGE_BUCKET_METADATA_KEY,
    STORAGE_PATH_METADATA_KEY, UploadContext, UploadSession, UploadSessionStatus,
    UploadedPartRecord, infer_file_category,
};
use crate::domain::repository::{
    LocalStoreRef, MetadataCacheRef, MetadataStoreRef, ObjectRepositoryRef, ReferenceStoreRef,
    UploadSessionStoreRef,
};
use flare_server_core::error::{ErrorCode, Result, map_context_error, map_infra_error};

const DIRECT_SINGLE_PUT_THRESHOLD_BYTES: i64 = 8 * 1024 * 1024;
const DIRECT_MULTIPART_MIN_PART_SIZE_BYTES: i64 = 8 * 1024 * 1024;

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
        }
    }
}

mod direct;
mod helpers;
mod multipart;
mod references;
mod storage;
#[cfg(test)]
mod tests;
