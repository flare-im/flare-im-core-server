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

    #[instrument(skip(self, ctx, init), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        tenant_id = ctx.tenant_id().unwrap_or(""),
    ))]
    pub async fn initiate_multipart_upload(
        &self,
        ctx: &Context,
        init: MultipartUploadInit,
    ) -> Result<MultipartUploadSession> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "multipart upload is not configured"
            ));
        };

        let chunk_size = init
            .chunk_size
            .max(256 * 1024)
            .min(self.config.max_chunk_size_bytes);

        let upload_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let expires_at = now + Duration::seconds(self.config.chunk_ttl_seconds.max(60));

        let session = UploadSession {
            upload_id: upload_id.clone(),
            file_id: None,
            file_name: init.file_name,
            mime_type: init.mime_type,
            file_type: init.file_type,
            chunk_size,
            total_size: init.file_size,
            uploaded_size: 0,
            uploaded_chunks: Vec::new(),
            uploaded_parts: Vec::new(),
            transport_kind: None,
            storage_upload_id: None,
            bucket: None,
            object_key: None,
            single_part_upload_url: None,
            file_fingerprint: None,
            head_tail_sha256: None,
            full_sha256: None,
            total_parts: None,
            user_id: init.user_id,
            namespace: init.namespace,
            business_tag: init.business_tag,
            trace_id: init.trace_id,
            metadata: init.metadata,
            status: UploadSessionStatus::Pending,
            expires_at,
            created_at: now,
            updated_at: now,
        };

        self.ensure_session_dir(&upload_id).await?;

        store.create_session(&session).await?;

        Ok(MultipartUploadSession {
            upload_id,
            chunk_size,
            uploaded_size: session.uploaded_size,
            expires_at: session.expires_at,
        })
    }

    #[instrument(skip(self, ctx, chunk), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %chunk.upload_id,
        chunk_index = chunk.chunk_index,
    ))]
    pub async fn upload_multipart_chunk(
        &self,
        ctx: &Context,
        chunk: MultipartChunkPayload,
    ) -> Result<MultipartUploadSession> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "multipart upload is not configured"
            ));
        };

        let mut session = store.get_session(&chunk.upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={}", chunk.upload_id)
            )
        })?;

        if session.status != UploadSessionStatus::Pending {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "upload session is not pending"
            ));
        }

        let chunk_len = chunk.bytes.len() as i64;
        if chunk_len == 0 {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "chunk payload is empty"
            ));
        }
        if chunk_len > self.config.max_chunk_size_bytes {
            return Err(flare_server_core::flare_err_details!(
                ErrorCode::MessageTooLarge,
                "chunk size exceeds limit",
                format!(
                    "chunk_size={} max={}",
                    chunk_len, self.config.max_chunk_size_bytes
                )
            ));
        }

        let session_dir = self.ensure_session_dir(&chunk.upload_id).await?;
        let chunk_path = session_dir.join(format!("{:06}.part", chunk.chunk_index));

        if let Ok(metadata) = fs::metadata(&chunk_path).await
            && metadata.len() as i64 == chunk_len
        {
            // chunk already uploaded, refresh session expiry
            session.expires_at =
                Utc::now() + Duration::seconds(self.config.chunk_ttl_seconds.max(60));
            store.upsert_session(&session).await?;
            return Ok(MultipartUploadSession {
                upload_id: session.upload_id.clone(),
                chunk_size: session.chunk_size,
                uploaded_size: session.uploaded_size,
                expires_at: session.expires_at,
            });
        }

        let mut file = fs::File::create(&chunk_path).await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::InternalError,
                format!("failed to create chunk file {:?}", chunk_path),
            )
        })?;
        file.write_all(&chunk.bytes).await.map_err(|e| {
            map_infra_error(e, ErrorCode::InternalError, "failed to write chunk data")
        })?;
        file.flush().await.ok();

        session.add_chunk(chunk.chunk_index, chunk_len);
        session.expires_at = Utc::now() + Duration::seconds(self.config.chunk_ttl_seconds.max(60));
        session.updated_at = Utc::now();
        store.upsert_session(&session).await?;

        Ok(MultipartUploadSession {
            upload_id: session.upload_id.clone(),
            chunk_size: session.chunk_size,
            uploaded_size: session.uploaded_size,
            expires_at: session.expires_at,
        })
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn complete_multipart_upload(
        &self,
        ctx: &Context,
        upload_id: &str,
    ) -> Result<MediaFileMetadata> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "multipart upload is not configured"
            ));
        };

        let mut session = store.get_session(upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={upload_id}")
            )
        })?;

        if session.uploaded_chunks.is_empty() {
            return Err(flare_server_core::flare_err_details!(
                ErrorCode::InvalidParameter,
                "no chunks uploaded for session",
                format!("upload_id={upload_id}")
            ));
        }

        session.uploaded_chunks.sort_unstable();

        let payload = self
            .assemble_payload(upload_id, &session.uploaded_chunks)
            .await?;

        let file_size = payload.len() as i64;
        let file_id = session.upload_id.clone();
        session.total_size = Some(file_size);

        let file_category = session
            .metadata
            .get(FILE_CATEGORY_METADATA_KEY)
            .cloned()
            .unwrap_or_else(|| {
                infer_file_category(Some(session.file_type.as_str()), &session.mime_type)
            });

        let context = UploadContext {
            file_id: &file_id,
            file_name: &session.file_name,
            mime_type: &session.mime_type,
            payload: &payload,
            file_size,
            file_category,
            user_id: session.user_id.as_str(),
            trace_id: session.trace_id.as_deref(),
            namespace: session.namespace.as_deref(),
            business_tag: session.business_tag.as_deref(),
            metadata: session.metadata.clone(),
        };

        let metadata = self.store_media_file(ctx, context).await?;

        session.status = UploadSessionStatus::Completed;
        session.updated_at = Utc::now();
        store.upsert_session(&session).await.ok();

        self.cleanup_chunks(upload_id).await?;
        store.delete_session(upload_id).await.ok();

        Ok(metadata)
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn abort_multipart_upload(&self, ctx: &Context, upload_id: &str) -> Result<()> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "multipart upload is not configured"
            ));
        };

        if let Some(mut session) = store.get_session(upload_id).await? {
            session.status = UploadSessionStatus::Aborted;
            session.updated_at = Utc::now();
            store.upsert_session(&session).await.ok();
        }

        self.cleanup_chunks(upload_id).await?;
        store.delete_session(upload_id).await.ok();
        Ok(())
    }

    #[instrument(skip(self, ctx, metadata), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        file_name = %metadata.file_name,
        file_size = metadata.file_size,
    ))]
    pub async fn initiate_direct_upload(
        &self,
        ctx: &Context,
        metadata: &flare_grpc_proto::media::UploadFileMetadata,
        desired_part_size: i64,
        file_fingerprint: Option<String>,
        head_tail_sha256: Option<String>,
        full_sha256: Option<String>,
    ) -> Result<DirectUploadSessionState> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "direct upload is not configured"
            ));
        };
        let Some(object_repo) = &self.object_repo else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "object storage is required for direct upload"
            ));
        };

        let file_size = metadata.file_size.max(0);
        let file_id = Uuid::new_v4().to_string();
        let file_category = infer_file_category(
            Some(metadata.file_type().as_str_name()),
            metadata.mime_type.as_str(),
        );
        let bucket = object_repo
            .bucket_name()
            .unwrap_or_else(|| "media".to_string());
        let object_key =
            object_repo.build_object_key_for(&file_id, &metadata.file_name, &file_category);

        let now = Utc::now();
        let expires_at = now + Duration::seconds(self.config.chunk_ttl_seconds.max(300));
        let transport_kind = if file_size <= DIRECT_SINGLE_PUT_THRESHOLD_BYTES {
            DirectUploadTransportKind::SinglePut
        } else {
            DirectUploadTransportKind::MultipartPut
        };
        let part_size = if transport_kind == DirectUploadTransportKind::SinglePut {
            file_size.max(1)
        } else {
            desired_part_size
                .max(DIRECT_MULTIPART_MIN_PART_SIZE_BYTES)
                .min(
                    self.config
                        .max_chunk_size_bytes
                        .max(DIRECT_MULTIPART_MIN_PART_SIZE_BYTES),
                )
        };
        let total_parts = if transport_kind == DirectUploadTransportKind::SinglePut {
            1
        } else {
            ((file_size + part_size - 1) / part_size) as u32
        };

        let single_part_upload_url = if transport_kind == DirectUploadTransportKind::SinglePut {
            Some(
                object_repo
                    .presign_put_object(&object_key, &metadata.mime_type, self.config.default_ttl)
                    .await?,
            )
        } else {
            None
        };

        let storage_upload_id = if transport_kind == DirectUploadTransportKind::MultipartPut {
            Some(
                object_repo
                    .create_multipart_upload(&object_key, &metadata.mime_type)
                    .await?,
            )
        } else {
            None
        };

        let mut metadata_map = metadata.metadata.clone();
        metadata_map
            .entry(FILE_CATEGORY_METADATA_KEY.to_string())
            .or_insert_with(|| file_category.clone());

        let session = UploadSession {
            upload_id: Uuid::new_v4().to_string(),
            file_id: Some(file_id.clone()),
            file_name: metadata.file_name.clone(),
            mime_type: metadata.mime_type.clone(),
            file_type: metadata.file_type().as_str_name().to_string(),
            chunk_size: part_size,
            total_size: Some(file_size),
            uploaded_size: 0,
            uploaded_chunks: Vec::new(),
            uploaded_parts: Vec::new(),
            transport_kind: Some(transport_kind),
            storage_upload_id: storage_upload_id.clone(),
            bucket: Some(bucket.clone()),
            object_key: Some(object_key.clone()),
            single_part_upload_url: single_part_upload_url.clone(),
            file_fingerprint,
            head_tail_sha256,
            full_sha256,
            total_parts: Some(total_parts),
            user_id: metadata.user_id.clone(),
            namespace: if metadata.namespace.is_empty() {
                None
            } else {
                Some(metadata.namespace.clone())
            },
            business_tag: if metadata.business_tag.is_empty() {
                None
            } else {
                Some(metadata.business_tag.clone())
            },
            trace_id: if metadata.trace_id.is_empty() {
                None
            } else {
                Some(metadata.trace_id.clone())
            },
            metadata: metadata_map,
            status: UploadSessionStatus::Pending,
            expires_at,
            created_at: now,
            updated_at: now,
        };

        store.create_session(&session).await?;
        Ok(self.direct_state_from_session(&session))
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn get_direct_upload_status(
        &self,
        ctx: &Context,
        upload_id: &str,
    ) -> Result<DirectUploadSessionState> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;
        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "direct upload is not configured"
            ));
        };
        let session = store.get_session(upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={upload_id}")
            )
        })?;
        self.ensure_direct_session(&session)?;
        Ok(self.direct_state_from_session(&session))
    }

    #[instrument(skip(self, ctx, part_numbers), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn presign_direct_upload_parts(
        &self,
        ctx: &Context,
        upload_id: &str,
        part_numbers: &[u32],
        expires_in: i64,
    ) -> Result<Vec<PresignedUploadPartUrl>> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;
        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "direct upload is not configured"
            ));
        };
        let Some(object_repo) = &self.object_repo else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "object storage is required for direct upload"
            ));
        };
        let session = store.get_session(upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={upload_id}")
            )
        })?;
        self.ensure_direct_session(&session)?;
        if session.transport_kind != Some(DirectUploadTransportKind::MultipartPut) {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "presign parts is only valid for multipart direct upload"
            ));
        }
        let storage_upload_id = session.storage_upload_id.as_deref().ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::InternalError,
                "storage_upload_id missing in direct upload session"
            )
        })?;
        let object_key = session.object_key.as_deref().ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::InternalError,
                "object_key missing in direct upload session"
            )
        })?;
        let total_parts = session.total_parts.unwrap_or_default();

        let mut urls = Vec::with_capacity(part_numbers.len());
        for part_number in part_numbers {
            if *part_number == 0 || *part_number > total_parts {
                return Err(flare_server_core::flare_err_details!(
                    ErrorCode::InvalidParameter,
                    "part number out of range",
                    format!("part_number={} total_parts={}", part_number, total_parts)
                ));
            }
            let url = object_repo
                .presign_upload_part(
                    object_key,
                    storage_upload_id,
                    *part_number,
                    if expires_in > 0 {
                        expires_in
                    } else {
                        self.config.default_ttl
                    },
                )
                .await?;
            urls.push(PresignedUploadPartUrl {
                part_number: *part_number,
                upload_url: url,
                headers: HashMap::new(),
            });
        }
        Ok(urls)
    }

    #[instrument(skip(self, ctx, parts), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn commit_direct_upload_parts(
        &self,
        ctx: &Context,
        upload_id: &str,
        parts: &[UploadedPartRecord],
    ) -> Result<DirectUploadSessionState> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;
        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "direct upload is not configured"
            ));
        };
        let mut session = store.get_session(upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={upload_id}")
            )
        })?;
        self.ensure_direct_session(&session)?;
        let total_parts = session.total_parts.unwrap_or_default();
        for part in parts {
            if part.part_number == 0 || part.part_number > total_parts {
                return Err(flare_server_core::flare_err_details!(
                    ErrorCode::InvalidParameter,
                    "part number out of range",
                    format!(
                        "part_number={} total_parts={}",
                        part.part_number, total_parts
                    )
                ));
            }
            if part.etag.trim().is_empty() {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "etag is required when committing uploaded part"
                ));
            }
            session.merge_uploaded_part(part.clone());
        }
        session.updated_at = Utc::now();
        session.expires_at = Utc::now() + Duration::seconds(self.config.chunk_ttl_seconds.max(300));
        store.upsert_session(&session).await?;
        Ok(self.direct_state_from_session(&session))
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn complete_direct_upload(
        &self,
        ctx: &Context,
        upload_id: &str,
    ) -> Result<MediaFileMetadata> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;
        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "direct upload is not configured"
            ));
        };
        let Some(object_repo) = &self.object_repo else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "object storage is required for direct upload"
            ));
        };
        let mut session = store.get_session(upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={upload_id}")
            )
        })?;
        self.ensure_direct_session(&session)?;

        let object_key = session.object_key.clone().ok_or_else(|| {
            flare_server_core::flare_err!(ErrorCode::InternalError, "object_key missing in session")
        })?;
        let bucket = session.bucket.clone().unwrap_or_default();
        let file_id = session
            .file_id
            .clone()
            .unwrap_or_else(|| session.upload_id.clone());
        let file_size = session.total_size.unwrap_or_default();

        match session
            .transport_kind
            .unwrap_or(DirectUploadTransportKind::SinglePut)
        {
            DirectUploadTransportKind::SinglePut => {
                let stat = object_repo.stat_object(&object_key).await?;
                if let Some(size) = stat.size
                    && size <= 0
                {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::InternalError,
                        "single put object is missing or empty"
                    ));
                }
            }
            DirectUploadTransportKind::MultipartPut => {
                let total_parts = session.total_parts.unwrap_or_default();
                if total_parts == 0 || session.uploaded_parts.len() != total_parts as usize {
                    return Err(flare_server_core::flare_err_details!(
                        ErrorCode::InvalidParameter,
                        "not all multipart parts are committed",
                        format!(
                            "committed_parts={} total_parts={}",
                            session.uploaded_parts.len(),
                            total_parts
                        )
                    ));
                }
                let storage_upload_id = session.storage_upload_id.clone().ok_or_else(|| {
                    flare_server_core::flare_err!(
                        ErrorCode::InternalError,
                        "storage_upload_id missing in session"
                    )
                })?;
                object_repo
                    .complete_multipart_upload(
                        &object_key,
                        &storage_upload_id,
                        &session.uploaded_parts,
                    )
                    .await?;
            }
        }

        let metadata = self
            .persist_direct_upload_metadata(
                ctx,
                &session,
                &bucket,
                &object_key,
                &file_id,
                file_size,
            )
            .await?;

        session.status = UploadSessionStatus::Completed;
        session.updated_at = Utc::now();
        store.upsert_session(&session).await.ok();
        store.delete_session(upload_id).await.ok();
        Ok(metadata)
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        upload_id = %upload_id,
    ))]
    pub async fn abort_direct_upload(&self, ctx: &Context, upload_id: &str) -> Result<()> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;
        let Some(store) = &self.upload_conversation_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "direct upload is not configured"
            ));
        };

        if let Some(mut session) = store.get_session(upload_id).await? {
            if let (Some(object_repo), Some(object_key), Some(storage_upload_id), Some(kind)) = (
                self.object_repo.as_ref(),
                session.object_key.as_deref(),
                session.storage_upload_id.as_deref(),
                session.transport_kind,
            ) && kind == DirectUploadTransportKind::MultipartPut
            {
                let _ = object_repo
                    .abort_multipart_upload(object_key, storage_upload_id)
                    .await;
            }
            session.status = UploadSessionStatus::Aborted;
            session.updated_at = Utc::now();
            store.upsert_session(&session).await.ok();
        }

        store.delete_session(upload_id).await.ok();
        Ok(())
    }

    #[instrument(skip(self, ctx, context), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        file_id = context.file_id,
        file_name = context.file_name,
        file_size = context.file_size,
        user_id = context.user_id,
    ))]
    pub async fn store_media_file(
        &self,
        ctx: &Context,
        mut context: UploadContext<'_>,
    ) -> Result<MediaFileMetadata> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        tracing::trace!(
            file_id = context.file_id,
            file_name = context.file_name,
            file_size = context.file_size,
            user_id = context.user_id,
            "开始存储媒体文件"
        );

        let category = Self::ensure_file_category(&mut context);
        context
            .metadata
            .insert(FILE_CATEGORY_METADATA_KEY.to_string(), category.clone());
        let message_managed = Self::is_message_lifecycle_context(&context);
        Self::stamp_lifecycle_scope(&mut context.metadata, message_managed);

        let sha256 = self.compute_sha256(context.payload);
        tracing::trace!(
            file_id = context.file_id,
            sha256 = &sha256,
            "计算文件SHA256哈希"
        );

        let scope = self.extract_reference_scope(&context);
        tracing::trace!(file_id = context.file_id, scope = ?scope, "提取引用范围");

        if let Some(store) = &self.metadata_store {
            tracing::trace!(
                file_id = context.file_id,
                "检查数据库中是否已存在相同哈希的文件"
            );
            if let Some(mut existing) = store.load_by_hash(ctx, &sha256).await? {
                tracing::trace!(
                    file_id = context.file_id,
                    existing_file_id = existing.file_id,
                    "发现已存在的文件，使用去重机制"
                );
                existing
                    .metadata
                    .entry(FILE_CATEGORY_METADATA_KEY.to_string())
                    .or_insert_with(|| category.clone());
                if Self::can_reuse_hash_match(&existing, message_managed) {
                    if let Some(scope) = scope.as_ref() {
                        tracing::trace!(file_id = context.file_id, "为已存在的文件创建引用");
                        self.ensure_reference(ctx, &mut existing, &context, scope)
                            .await?;
                    } else {
                        tracing::trace!(
                            file_id = context.file_id,
                            existing_file_id = existing.file_id,
                            "复用非消息媒体去重结果，不变更引用生命周期"
                        );
                        if !Self::is_message_managed_asset(&existing) {
                            existing.status = MediaAssetStatus::Active;
                            existing.grace_expires_at = None;
                            existing
                                .metadata
                                .entry(MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string())
                                .or_insert_with(|| EXTERNAL_MEDIA_LIFECYCLE_SCOPE.to_string());
                            self.save_and_cache(ctx, &existing).await?;
                        }
                    }

                    if let Some(cache) = &self.metadata_cache {
                        cache.cache_metadata(ctx, &existing).await.ok();
                    }

                    tracing::trace!(
                        file_id = context.file_id,
                        existing_file_id = existing.file_id,
                        "返回已存在的文件元数据"
                    );
                    return Ok(existing);
                } else {
                    tracing::trace!(
                        file_id = context.file_id,
                        existing_file_id = existing.file_id,
                        "跳过待归档消息媒体的跨生命周期哈希复用"
                    );
                }
            } else {
                tracing::trace!(
                    file_id = context.file_id,
                    sha256 = &sha256,
                    "数据库中未找到相同哈希的文件"
                );
            }
        } else {
            tracing::warn!(file_id = context.file_id, "未配置元数据存储");
        }

        let md5 = Some(format!("{:x}", md5_compute(context.payload)));
        tracing::trace!(
            file_id = context.file_id,
            md5 = md5.as_ref().unwrap(),
            "计算文件MD5哈希"
        );

        let mut storage_bucket = self
            .object_repo
            .as_ref()
            .and_then(|repo| repo.bucket_name());

        let (url, cdn_url, storage_path) = if let Some(object_repo) = &self.object_repo {
            tracing::trace!(file_id = context.file_id, "使用对象存储存储文件");
            match object_repo.put_object(&context).await {
                Ok(path) => {
                    tracing::trace!(
                        file_id = context.file_id,
                        object_path = &path,
                        "文件已存储到对象存储"
                    );

                    let direct_base = object_repo.base_url();
                    let cdn = self
                        .config
                        .cdn_base_url
                        .clone()
                        .or_else(|| object_repo.cdn_base_url());
                    let mut primary_url = String::new();

                    if object_repo.use_presigned_urls() {
                        match object_repo
                            .presign_object(&path, self.config.default_ttl)
                            .await
                        {
                            Ok(value) => primary_url = value,
                            Err(err) => {
                                tracing::error!(
                                    object_path = &path,
                                    error = %err,
                                    "生成预签名URL失败，回退到直链"
                                );
                                if let Some(base) = &direct_base {
                                    primary_url = Self::build_full_url(base, &path);
                                }
                            }
                        }
                    } else if let Some(base) = &direct_base {
                        primary_url = Self::build_full_url(base, &path);
                    }

                    if primary_url.is_empty() {
                        primary_url = path.clone();
                    }

                    let cdn_url = cdn
                        .map(|base| Self::build_full_url(&base, &path))
                        .unwrap_or_default();

                    (primary_url, cdn_url, Some(path))
                }
                Err(err) => {
                    tracing::error!(
                        file_id = context.file_id,
                        error = ?err,
                        "上传对象到媒体存储失败，尝试回退到本地存储"
                    );

                    if let Some(local_store) = &self.local_store {
                        let path = local_store.write(&context).await?;
                        let base = local_store.base_url();
                        // 对象存储失败后已明确回退到本地文件系统，此时不能再继续返回对象存储 CDN，
                        // 否则客户端会拿到一条“本地文件已写入、但 URL 仍指向失效 bucket”的错误地址。
                        let cdn = base.clone();
                        storage_bucket = None;
                        (
                            base.map(|base| Self::build_full_url(&base, &path))
                                .unwrap_or_default(),
                            cdn.map(|base| Self::build_full_url(&base, &path))
                                .unwrap_or_default(),
                            Some(path),
                        )
                    } else {
                        return Err(err);
                    }
                }
            }
        } else if let Some(local_store) = &self.local_store {
            tracing::trace!(file_id = context.file_id, "使用本地存储存储文件");
            let path = local_store.write(&context).await?;
            tracing::trace!(
                file_id = context.file_id,
                local_path = &path,
                "文件已存储到本地存储"
            );
            let base = local_store.base_url();
            let cdn = self.config.cdn_base_url.clone().or_else(|| base.clone());
            (
                base.map(|base| Self::build_full_url(&base, &path))
                    .unwrap_or_default(),
                cdn.map(|base| Self::build_full_url(&base, &path))
                    .unwrap_or_default(),
                Some(path),
            )
        } else {
            tracing::error!(file_id = context.file_id, "未配置媒体存储后端");
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "no media storage backend configured"
            ));
        };

        if let Some(ref path) = storage_path {
            context
                .metadata
                .insert(STORAGE_PATH_METADATA_KEY.to_string(), path.clone());
        }
        if let Some(ref bucket) = storage_bucket {
            context
                .metadata
                .insert(STORAGE_BUCKET_METADATA_KEY.to_string(), bucket.clone());
        }

        tracing::trace!(
            file_id = context.file_id,
            url = &url,
            cdn_url = &cdn_url,
            "生成文件URL"
        );

        let (reference_count, status, grace_expires_at) = Self::initial_lifecycle_state(
            message_managed,
            self.reference_store.is_some(),
            self.config.orphan_grace_seconds,
        );

        let mut metadata = MediaFileMetadata {
            file_id: context.file_id.to_string(),
            file_name: context.file_name.to_string(),
            mime_type: context.mime_type.to_string(),
            file_size: context.file_size,
            url,
            cdn_url,
            md5,
            sha256: Some(sha256),
            metadata: context.metadata.clone(),
            uploaded_at: Utc::now(),
            reference_count,
            status,
            grace_expires_at,
            access_type: FileAccessType::default(), // 默认使用私有访问类型
            storage_bucket: storage_bucket.clone(),
            storage_path: storage_path.clone(),
        };

        tracing::trace!(file_id = context.file_id, "准备保存文件元数据");

        self.save_and_cache(ctx, &metadata)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "persist metadata"))?;

        tracing::trace!(file_id = context.file_id, "文件元数据已保存");

        if let (Some(scope), Some(_)) = (scope, self.reference_store.as_ref()) {
            tracing::trace!(file_id = context.file_id, "为新文件创建引用");
            self.ensure_reference(ctx, &mut metadata, &context, &scope)
                .await?;
            tracing::trace!(file_id = context.file_id, "文件引用已创建");
        }

        tracing::trace!(file_id = context.file_id, "文件存储完成");
        Ok(metadata)
    }

    #[instrument(skip(self, ctx))]
    pub async fn delete_media_file(&self, ctx: &Context, file_id: &str) -> Result<()> {
        let mut metadata = self.get_metadata(ctx, file_id).await?;

        if metadata.reference_count > 1 {
            if let Some(reference_store) = &self.reference_store {
                let _ = reference_store.delete_any_reference(ctx, file_id).await;
                let updated_count = reference_store
                    .count_references(ctx, file_id)
                    .await
                    .unwrap_or(metadata.reference_count.saturating_sub(1));
                metadata.reference_count = updated_count;
            } else {
                metadata.reference_count = metadata.reference_count.saturating_sub(1);
            }

            Self::apply_reference_lifecycle(&mut metadata, self.config.orphan_grace_seconds);

            self.save_and_cache(ctx, &metadata).await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "persist metadata reference update",
                )
            })?;

            return Ok(());
        }

        let storage_path = metadata
            .storage_path()
            .map(|s| s.to_string())
            .or_else(|| metadata.metadata.get(STORAGE_PATH_METADATA_KEY).cloned());

        if let Some(repo) = &self.object_repo {
            let target = storage_path.as_deref().unwrap_or(file_id);
            let _ = repo.delete_object(target).await;
        }

        if let Some(local) = &self.local_store {
            let target = storage_path.as_deref().unwrap_or(file_id);
            let _ = local.delete(target).await;
        }

        if let Some(reference_store) = &self.reference_store {
            let _ = reference_store.delete_all_references(ctx, file_id).await;
        }

        if let Some(store) = &self.metadata_store {
            let _ = store.delete_metadata(ctx, file_id).await;
        }

        if let Some(cache) = &self.metadata_cache {
            let _ = cache.invalidate(ctx, file_id).await;
        }

        Ok(())
    }

    pub async fn get_metadata(&self, ctx: &Context, file_id: &str) -> Result<MediaFileMetadata> {
        if let Some(cache) = &self.metadata_cache
            && let Some(metadata) = cache.get_cached_metadata(ctx, file_id).await?
        {
            return Ok(metadata);
        }

        if let Some(store) = &self.metadata_store
            && let Some(metadata) = store.load_metadata(ctx, file_id).await?
        {
            if let Some(cache) = &self.metadata_cache {
                cache.cache_metadata(ctx, &metadata).await.ok();
            }
            return Ok(metadata);
        }

        Err(flare_server_core::flare_err_details!(
            ErrorCode::MessageNotFound,
            "metadata not found",
            file_id.to_string()
        ))
    }

    pub async fn download_local_file(&self, ctx: &Context, file_id: &str) -> Result<Vec<u8>> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let metadata = self.get_metadata(ctx, file_id).await?;
        if metadata.storage_bucket().is_some() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "file is stored in object storage, use generated url instead of local download"
            ));
        }

        let target = metadata.storage_path().unwrap_or(file_id);
        let local = self.local_store.as_ref().ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "local media store is not configured"
            )
        })?;

        local.read(target).await
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        file_id = %file_id,
    ))]
    pub async fn create_presigned_url(
        &self,
        ctx: &Context,
        file_id: &str,
        expires_in: i64,
    ) -> Result<PresignedUrl> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let _tenant_id = ctx.tenant_id().unwrap_or("0");
        let metadata = self.get_metadata(ctx, file_id).await?;
        let expires_in = if expires_in > 0 {
            expires_in
        } else {
            self.config.default_ttl
        };
        let expires_at = Utc::now() + Duration::seconds(expires_in);

        let object_path = metadata
            .storage_path()
            .map(|s| s.to_string())
            .unwrap_or_else(|| metadata.file_id.clone());

        let mut url = metadata.url.clone();
        let mut cdn_url = metadata.cdn_url.clone();

        let stored_in_object_store = metadata.storage_bucket().is_some();

        if stored_in_object_store {
            if let Some(repo) = &self.object_repo {
                match metadata.access_type {
                    FileAccessType::Public => {
                        if url.is_empty()
                            && let Some(base) = repo.base_url()
                        {
                            url = Self::build_full_url(&base, &object_path);
                        }
                        if cdn_url.is_empty()
                            && let Some(cdn_base) = self
                                .config
                                .cdn_base_url
                                .clone()
                                .or_else(|| repo.cdn_base_url())
                        {
                            cdn_url = Self::build_full_url(&cdn_base, &object_path);
                        }
                    }
                    FileAccessType::Private => {
                        if repo.use_presigned_urls() {
                            match repo.presign_object(&object_path, expires_in).await {
                                Ok(presigned) => url = presigned,
                                Err(err) => {
                                    tracing::error!(file_id = file_id, error = %err, "生成预签名URL失败，回退到直链");
                                    if let Some(base) = repo.base_url() {
                                        url = Self::build_full_url(&base, &object_path);
                                    }
                                }
                            }
                        } else if let Some(base) = repo.base_url() {
                            url = Self::build_full_url(&base, &object_path);
                        }

                        if cdn_url.is_empty()
                            && let Some(cdn_base) = self
                                .config
                                .cdn_base_url
                                .clone()
                                .or_else(|| repo.cdn_base_url())
                        {
                            cdn_url = Self::build_full_url(&cdn_base, &object_path);
                        }
                    }
                }
            }
        } else if let Some(base) = self.local_store.as_ref().and_then(|store| store.base_url()) {
            if url.is_empty() {
                url = Self::build_full_url(&base, &object_path);
            }
            if cdn_url.is_empty() {
                cdn_url = Self::build_full_url(&base, &object_path);
            }
        }

        if url.is_empty() {
            if stored_in_object_store {
                if let Some(base) = self.object_repo.as_ref().and_then(|repo| repo.base_url()) {
                    url = Self::build_full_url(&base, &object_path);
                }
            } else if let Some(base) = self.local_store.as_ref().and_then(|store| store.base_url())
            {
                url = Self::build_full_url(&base, &object_path);
            }
        }
        if url.is_empty() {
            url = metadata.url.clone();
        }
        if url.is_empty() {
            url = object_path.clone();
        }

        if cdn_url.is_empty() {
            if stored_in_object_store {
                if let Some(base) = self.config.cdn_base_url.clone() {
                    cdn_url = Self::build_full_url(&base, &object_path);
                }
            } else if let Some(base) = self.local_store.as_ref().and_then(|store| store.base_url())
            {
                cdn_url = Self::build_full_url(&base, &object_path);
            }
        }
        if cdn_url.is_empty() {
            cdn_url = metadata.cdn_url.clone();
        }

        let final_cdn_url = if cdn_url.is_empty() {
            if stored_in_object_store {
                if let Some(base) = self.config.cdn_base_url.clone() {
                    Self::build_full_url(&base, &object_path)
                } else {
                    String::new()
                }
            } else if let Some(base) = self.local_store.as_ref().and_then(|store| store.base_url())
            {
                Self::build_full_url(&base, &object_path)
            } else {
                String::new()
            }
        } else {
            cdn_url
        };

        Ok(PresignedUrl {
            url,
            cdn_url: final_cdn_url,
            expires_at,
        })
    }

    pub fn generate_upload_url(
        &self,
        bucket: Option<&str>,
        object_key: Option<&str>,
    ) -> (String, String) {
        let resolved_key = object_key
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().trim_start_matches('/').to_string())
            .unwrap_or_else(|| format!("uploads/{}", Uuid::new_v4()));

        let resolved_bucket = bucket
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().to_string())
            .or_else(|| {
                self.object_repo
                    .as_ref()
                    .and_then(|repo| repo.bucket_name())
            })
            .unwrap_or_else(|| "media".to_string());

        let upload_url = if let Some(base) =
            self.object_repo.as_ref().and_then(|repo| repo.base_url())
        {
            Self::build_full_url(&base, &resolved_key)
        } else if let Some(base) = self.local_store.as_ref().and_then(|store| store.base_url()) {
            Self::build_full_url(&base, &resolved_key)
        } else {
            format!("/{}/{}", resolved_bucket, resolved_key)
        };

        (upload_url, resolved_key)
    }

    pub async fn list_objects(
        &self,
        _ctx: &Context,
        _bucket: &str,
        _prefix: &str,
    ) -> Result<Vec<MediaFileMetadata>> {
        Ok(Vec::new())
    }

    pub async fn set_object_acl(&self, ctx: &Context, file_id: &str) -> Result<()> {
        let _ = self.get_metadata(ctx, file_id).await?;
        Ok(())
    }

    pub fn describe_bucket(
        &self,
        bucket: Option<&str>,
    ) -> (String, String, String, bool, HashMap<String, String>) {
        let resolved_bucket = bucket
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().to_string())
            .or_else(|| {
                self.object_repo
                    .as_ref()
                    .and_then(|repo| repo.bucket_name())
            })
            .unwrap_or_else(|| "media".to_string());
        let storage_class = self
            .object_repo
            .as_ref()
            .and_then(|repo| repo.storage_provider())
            .unwrap_or_else(|| {
                if self.local_store.is_some() {
                    "filesystem".to_string()
                } else {
                    "unknown".to_string()
                }
            });
        let mut metadata = HashMap::new();
        metadata.insert("provider".to_string(), storage_class.clone());
        if let Some(base_url) = self.object_repo.as_ref().and_then(|repo| repo.base_url()) {
            metadata.insert("base_url".to_string(), base_url);
        } else if let Some(base_url) = self.local_store.as_ref().and_then(|store| store.base_url())
        {
            metadata.insert("base_url".to_string(), base_url);
        }

        (
            resolved_bucket,
            "us-east-1".to_string(),
            storage_class,
            false,
            metadata,
        )
    }

    pub async fn add_reference(
        &self,
        ctx: &Context,
        file_id: &str,
        scope: MediaReferenceScope,
        metadata: HashMap<String, String>,
    ) -> Result<MediaFileMetadata> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let mut file_metadata = self.get_metadata(ctx, file_id).await?;

        if let Some(reference_store) = &self.reference_store {
            if reference_store
                .reference_exists(
                    ctx,
                    file_id,
                    &scope.namespace,
                    &scope.owner_id,
                    scope.business_tag.as_deref(),
                )
                .await?
            {
                return Ok(file_metadata);
            }

            let reference = MediaReference {
                tenant_id: tenant_id.to_string(),
                reference_id: Self::deterministic_reference_id(tenant_id, file_id, &scope),
                file_id: file_id.to_string(),
                namespace: scope.namespace.clone(),
                owner_id: scope.owner_id.clone(),
                business_tag: scope.business_tag.clone(),
                metadata,
                created_at: Utc::now(),
                expires_at: None,
            };

            if reference_store.create_reference(&reference).await? {
                file_metadata.reference_count =
                    reference_store.count_references(ctx, file_id).await?;
            }
        } else {
            file_metadata.reference_count = file_metadata.reference_count.saturating_add(1);
        }

        Self::stamp_lifecycle_scope(
            &mut file_metadata.metadata,
            Self::is_message_lifecycle_value(&scope.namespace)
                || scope
                    .business_tag
                    .as_deref()
                    .map(Self::is_message_lifecycle_value)
                    .unwrap_or(false),
        );
        Self::apply_reference_lifecycle(&mut file_metadata, self.config.orphan_grace_seconds);

        self.save_and_cache(ctx, &file_metadata).await?;

        Ok(file_metadata)
    }

    pub async fn remove_reference(
        &self,
        ctx: &Context,
        file_id: &str,
        reference_id: Option<&str>,
    ) -> Result<MediaFileMetadata> {
        let _tenant_id = ctx.tenant_id().unwrap_or("0");
        let mut file_metadata = self.get_metadata(ctx, file_id).await?;

        if let Some(reference_store) = &self.reference_store {
            let removed = if let Some(reference_id) = reference_id {
                reference_store.delete_reference(ctx, reference_id).await?
            } else {
                reference_store
                    .delete_any_reference(ctx, file_id)
                    .await?
                    .is_some()
            };

            if removed {
                file_metadata.reference_count =
                    reference_store.count_references(ctx, file_id).await?;
            }
        } else {
            file_metadata.reference_count = file_metadata.reference_count.saturating_sub(1);
        }

        Self::apply_reference_lifecycle(&mut file_metadata, self.config.orphan_grace_seconds);

        self.save_and_cache(ctx, &file_metadata).await?;

        Ok(file_metadata)
    }

    pub async fn list_references(
        &self,
        ctx: &Context,
        file_id: &str,
    ) -> Result<Vec<MediaReference>> {
        if let Some(reference_store) = &self.reference_store {
            reference_store.list_references(ctx, file_id).await
        } else {
            Ok(vec![])
        }
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
    ))]
    pub async fn cleanup_orphaned_assets(&self, ctx: &Context) -> Result<Vec<String>> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;

        let Some(store) = &self.metadata_store else {
            return Ok(vec![]);
        };

        let expired = store.list_orphaned_assets(Utc::now()).await.map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "list orphaned media assets")
        })?;

        for asset in &expired {
            if !Self::is_message_managed_asset(asset) {
                tracing::trace!(
                    file_id = asset.file_id,
                    "跳过非消息生命周期媒体的自动归档清理"
                );
                continue;
            }

            let storage_path = asset
                .storage_path()
                .map(|s| s.to_string())
                .or_else(|| asset.metadata.get(STORAGE_PATH_METADATA_KEY).cloned())
                .unwrap_or_else(|| asset.file_id.clone());

            // 从 metadata 中提取 tenant_id，如果没有则使用默认值
            let _tenant_id = asset
                .metadata
                .get("tenant_id")
                .map(|s| s.as_str())
                .unwrap_or("0");

            if let Some(repo) = &self.object_repo {
                let _ = repo.delete_object(&storage_path).await;
            }
            if let Some(local) = &self.local_store {
                let _ = local.delete(&storage_path).await;
            }
            if let Some(reference_store) = &self.reference_store {
                let _ = reference_store
                    .delete_all_references(ctx, &asset.file_id)
                    .await;
            }
            let _ = store.delete_metadata(ctx, &asset.file_id).await;
            if let Some(cache) = &self.metadata_cache {
                let _ = cache.invalidate(ctx, &asset.file_id).await;
            }
        }

        Ok(expired
            .into_iter()
            .filter(Self::is_message_managed_asset)
            .map(|asset| asset.file_id)
            .collect())
    }

    fn compute_sha256(&self, payload: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(payload);
        format!("{:x}", hasher.finalize())
    }

    fn session_dir(&self, upload_id: &str) -> PathBuf {
        self.config.chunk_root_dir.join(upload_id)
    }

    async fn ensure_session_dir(&self, upload_id: &str) -> Result<PathBuf> {
        let dir = self.session_dir(upload_id);
        fs::create_dir_all(&dir).await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::InternalError,
                format!("failed to prepare session directory {:?}", dir),
            )
        })?;
        Ok(dir)
    }

    async fn assemble_payload(&self, upload_id: &str, chunks: &[u32]) -> Result<Vec<u8>> {
        let dir = self.session_dir(upload_id);
        let mut payload = Vec::new();

        for index in chunks {
            let chunk_path = dir.join(format!("{:06}.part", index));
            let mut file = fs::File::open(&chunk_path).await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    format!("missing chunk file {:?}", chunk_path),
                )
            })?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    format!("failed to read chunk {:?}", chunk_path),
                )
            })?;
            payload.extend_from_slice(&buffer);
        }

        Ok(payload)
    }

    async fn cleanup_chunks(&self, upload_id: &str) -> Result<()> {
        let dir = self.session_dir(upload_id);
        if fs::metadata(&dir).await.is_ok() {
            fs::remove_dir_all(&dir).await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    format!("failed to cleanup chunk directory {:?}", dir),
                )
            })?;
        }
        Ok(())
    }

    fn ensure_direct_session(&self, session: &UploadSession) -> Result<()> {
        if session.transport_kind.is_none() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "upload session is not a direct upload session"
            ));
        }
        Ok(())
    }

    fn direct_state_from_session(&self, session: &UploadSession) -> DirectUploadSessionState {
        DirectUploadSessionState {
            upload_id: session.upload_id.clone(),
            file_id: session
                .file_id
                .clone()
                .unwrap_or_else(|| session.upload_id.clone()),
            transport_kind: session
                .transport_kind
                .unwrap_or(DirectUploadTransportKind::SinglePut),
            bucket: session.bucket.clone().unwrap_or_default(),
            object_key: session.object_key.clone().unwrap_or_default(),
            storage_upload_id: session.storage_upload_id.clone(),
            part_size: session.chunk_size,
            total_size: session.total_size.unwrap_or_default(),
            total_parts: session.total_parts.unwrap_or(1),
            upload_url: session.single_part_upload_url.clone(),
            uploaded_parts: session.uploaded_parts.clone(),
            uploaded_size: session.uploaded_size,
            expires_at: session.expires_at,
        }
    }

    async fn persist_direct_upload_metadata(
        &self,
        ctx: &Context,
        session: &UploadSession,
        bucket: &str,
        object_key: &str,
        file_id: &str,
        file_size: i64,
    ) -> Result<MediaFileMetadata> {
        let mut metadata_map = session.metadata.clone();
        metadata_map.insert(STORAGE_BUCKET_METADATA_KEY.to_string(), bucket.to_string());
        metadata_map.insert(
            STORAGE_PATH_METADATA_KEY.to_string(),
            object_key.to_string(),
        );
        if let Some(fingerprint) = session.file_fingerprint.as_ref() {
            metadata_map
                .entry("file_fingerprint".to_string())
                .or_insert_with(|| fingerprint.clone());
        }
        if let Some(head_tail_sha256) = session.head_tail_sha256.as_ref() {
            metadata_map
                .entry("head_tail_sha256".to_string())
                .or_insert_with(|| head_tail_sha256.clone());
        }
        let empty = [];
        let scope_file_category = session
            .metadata
            .get(FILE_CATEGORY_METADATA_KEY)
            .cloned()
            .unwrap_or_else(|| {
                infer_file_category(Some(session.file_type.as_str()), &session.mime_type)
            });
        let mut scope_context = UploadContext {
            file_id,
            file_name: &session.file_name,
            mime_type: &session.mime_type,
            file_size,
            payload: &empty,
            file_category: scope_file_category,
            user_id: session.user_id.as_str(),
            trace_id: session.trace_id.as_deref(),
            namespace: session.namespace.as_deref(),
            business_tag: session.business_tag.as_deref(),
            metadata: metadata_map.clone(),
        };
        let message_managed = Self::is_message_lifecycle_context(&scope_context);
        Self::stamp_lifecycle_scope(&mut metadata_map, message_managed);
        scope_context.metadata = metadata_map.clone();

        let direct_base = self.object_repo.as_ref().and_then(|repo| repo.base_url());
        let cdn_base = self.config.cdn_base_url.clone().or_else(|| {
            self.object_repo
                .as_ref()
                .and_then(|repo| repo.cdn_base_url())
        });
        let url = direct_base
            .map(|base| Self::build_full_url(&base, object_key))
            .unwrap_or_else(|| object_key.to_string());
        let cdn_url = cdn_base
            .map(|base| Self::build_full_url(&base, object_key))
            .unwrap_or_default();

        let (reference_count, status, grace_expires_at) = Self::initial_lifecycle_state(
            message_managed,
            self.reference_store.is_some(),
            self.config.orphan_grace_seconds,
        );

        let mut metadata = MediaFileMetadata {
            file_id: file_id.to_string(),
            file_name: session.file_name.clone(),
            mime_type: session.mime_type.clone(),
            file_size,
            url,
            cdn_url,
            md5: None,
            sha256: session
                .full_sha256
                .clone()
                .or(session.head_tail_sha256.clone()),
            metadata: metadata_map,
            uploaded_at: Utc::now(),
            reference_count,
            status,
            grace_expires_at,
            access_type: FileAccessType::default(),
            storage_bucket: Some(bucket.to_string()),
            storage_path: Some(object_key.to_string()),
        };

        self.save_and_cache(ctx, &metadata).await?;

        if let (Some(scope), Some(_)) = (
            self.extract_reference_scope(&scope_context),
            self.reference_store.as_ref(),
        ) {
            self.ensure_reference(ctx, &mut metadata, &scope_context, &scope)
                .await?;
        }

        Ok(metadata)
    }

    fn ensure_file_category(context: &mut UploadContext<'_>) -> String {
        if !context.file_category.is_empty() {
            return context.file_category.clone();
        }

        let hint = context
            .metadata
            .get("file_type")
            .map(|value| value.as_str())
            .or_else(|| {
                context
                    .metadata
                    .get(FILE_CATEGORY_METADATA_KEY)
                    .map(|value| value.as_str())
            });

        let category = infer_file_category(hint, context.mime_type);
        context.file_category = category.clone();
        category
    }

    fn build_full_url(base: &str, path: &str) -> String {
        let trimmed_base = base.trim_end_matches('/');
        let trimmed_path = path.trim_start_matches('/');

        if trimmed_base.is_empty() {
            trimmed_path.to_string()
        } else if trimmed_path.is_empty() {
            trimmed_base.to_string()
        } else {
            format!("{}/{}", trimmed_base, trimmed_path)
        }
    }

    fn normalized_metadata_value<'a>(
        metadata: &'a HashMap<String, String>,
        keys: &[&str],
    ) -> Option<&'a str> {
        keys.iter()
            .find_map(|key| metadata.get(*key))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    }

    fn is_message_lifecycle_value(value: &str) -> bool {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            MESSAGE_MEDIA_LIFECYCLE_SCOPE | "messages" | "im_message" | "im-message"
        )
    }

    fn is_message_lifecycle_context(context: &UploadContext<'_>) -> bool {
        let scope = Self::normalized_metadata_value(
            &context.metadata,
            &[
                MEDIA_LIFECYCLE_SCOPE_METADATA_KEY,
                "lifecycle_scope",
                "media_scope",
                "media_usage",
                "usage",
            ],
        );

        scope.map(Self::is_message_lifecycle_value).unwrap_or(false)
            || context
                .namespace
                .map(Self::is_message_lifecycle_value)
                .unwrap_or(false)
            || context
                .business_tag
                .map(Self::is_message_lifecycle_value)
                .unwrap_or(false)
            || context
                .metadata
                .get("namespace")
                .map(|value| Self::is_message_lifecycle_value(value))
                .unwrap_or(false)
            || context
                .metadata
                .get("business_tag")
                .map(|value| Self::is_message_lifecycle_value(value))
                .unwrap_or(false)
    }

    fn is_message_managed_asset(metadata: &MediaFileMetadata) -> bool {
        let lifecycle = Self::normalized_metadata_value(
            &metadata.metadata,
            &[
                MEDIA_LIFECYCLE_SCOPE_METADATA_KEY,
                "lifecycle_scope",
                "media_scope",
                "media_usage",
                "usage",
            ],
        );

        lifecycle
            .map(Self::is_message_lifecycle_value)
            .unwrap_or(false)
            || metadata
                .metadata
                .get("namespace")
                .map(|value| Self::is_message_lifecycle_value(value))
                .unwrap_or(false)
            || metadata
                .metadata
                .get("business_tag")
                .map(|value| Self::is_message_lifecycle_value(value))
                .unwrap_or(false)
    }

    fn stamp_lifecycle_scope(metadata: &mut HashMap<String, String>, message_managed: bool) {
        if message_managed {
            metadata.insert(
                MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string(),
                MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string(),
            );
        } else {
            metadata
                .entry(MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string())
                .or_insert_with(|| EXTERNAL_MEDIA_LIFECYCLE_SCOPE.to_string());
        }
    }

    fn initial_lifecycle_state(
        message_managed: bool,
        has_reference_store: bool,
        orphan_grace_seconds: i64,
    ) -> (u64, MediaAssetStatus, Option<chrono::DateTime<Utc>>) {
        if message_managed && has_reference_store {
            (
                0,
                MediaAssetStatus::Pending,
                Some(Utc::now() + Duration::seconds(orphan_grace_seconds)),
            )
        } else {
            (1, MediaAssetStatus::Active, None)
        }
    }

    fn apply_reference_lifecycle(metadata: &mut MediaFileMetadata, orphan_grace_seconds: i64) {
        if metadata.reference_count > 0 {
            metadata.status = MediaAssetStatus::Active;
            metadata.grace_expires_at = None;
        } else if Self::is_message_managed_asset(metadata) {
            metadata.status = MediaAssetStatus::Pending;
            metadata.grace_expires_at = Some(Utc::now() + Duration::seconds(orphan_grace_seconds));
        } else {
            metadata.status = MediaAssetStatus::Active;
            metadata.grace_expires_at = None;
        }
    }

    fn can_reuse_hash_match(existing: &MediaFileMetadata, message_managed: bool) -> bool {
        message_managed || !Self::is_message_managed_asset(existing) || existing.reference_count > 0
    }

    fn deterministic_reference_id(
        tenant_id: &str,
        file_id: &str,
        scope: &MediaReferenceScope,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(tenant_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(file_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(scope.namespace.as_bytes());
        hasher.update(b"\0");
        hasher.update(scope.owner_id.as_bytes());
        hasher.update(b"\0");
        if let Some(tag) = scope.business_tag.as_deref() {
            hasher.update(tag.as_bytes());
        }
        let digest = format!("{:x}", hasher.finalize());
        format!("ref_{}", &digest[..32])
    }

    fn extract_reference_scope(&self, context: &UploadContext<'_>) -> Option<MediaReferenceScope> {
        if context.user_id.is_empty() || !Self::is_message_lifecycle_context(context) {
            return None;
        }

        let namespace = context
            .namespace
            .map(|value| value.to_string())
            .or_else(|| context.metadata.get("namespace").cloned())
            .unwrap_or_else(|| MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string());

        let business_tag = context
            .business_tag
            .map(|value| value.to_string())
            .or_else(|| context.metadata.get("business_tag").cloned())
            .or_else(|| Some(MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string()));

        Some(MediaReferenceScope {
            namespace,
            owner_id: context.user_id.to_string(),
            business_tag,
        })
    }

    fn reference_payload_from_context(
        &self,
        context: &UploadContext<'_>,
    ) -> HashMap<String, String> {
        let mut payload = context.metadata.clone();
        if !context.user_id.is_empty() {
            payload
                .entry("owner_id".to_string())
                .or_insert_with(|| context.user_id.to_string());
        }
        if let Some(trace_id) = context.trace_id {
            payload
                .entry("trace_id".to_string())
                .or_insert_with(|| trace_id.to_string());
        }
        if let Some(namespace) = context.namespace {
            payload
                .entry("namespace".to_string())
                .or_insert_with(|| namespace.to_string());
        }
        if let Some(business_tag) = context.business_tag {
            payload
                .entry("business_tag".to_string())
                .or_insert_with(|| business_tag.to_string());
        }
        payload
    }

    async fn ensure_reference(
        &self,
        ctx: &Context,
        metadata: &mut MediaFileMetadata,
        context: &UploadContext<'_>,
        scope: &MediaReferenceScope,
    ) -> Result<()> {
        let Some(reference_store) = &self.reference_store else {
            metadata.reference_count = metadata.reference_count.saturating_add(1);
            metadata.status = MediaAssetStatus::Active;
            metadata.grace_expires_at = None;
            self.save_and_cache(ctx, metadata).await?;
            return Ok(());
        };

        let reference = MediaReference {
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            reference_id: Self::deterministic_reference_id(
                ctx.tenant_id().unwrap_or("0"),
                &metadata.file_id,
                scope,
            ),
            file_id: metadata.file_id.clone(),
            namespace: scope.namespace.clone(),
            owner_id: scope.owner_id.clone(),
            business_tag: scope.business_tag.clone(),
            metadata: self.reference_payload_from_context(context),
            created_at: Utc::now(),
            expires_at: None,
        };

        if reference_store.create_reference(&reference).await? {
            metadata.reference_count = reference_store
                .count_references(ctx, &metadata.file_id)
                .await?;
        }

        Self::apply_reference_lifecycle(metadata, self.config.orphan_grace_seconds);

        self.save_and_cache(ctx, metadata).await?;

        Ok(())
    }

    async fn save_and_cache(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
        if let Some(store) = &self.metadata_store {
            store
                .save_metadata(metadata)
                .await
                .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "persist metadata"))?;
        }

        if let Some(cache) = &self.metadata_cache {
            cache.cache_metadata(ctx, metadata).await.ok();
        }

        Ok(())
    }

    /// 准备上传上下文数据（从 protobuf 元数据提取并处理业务逻辑）
    ///
    /// 这是领域服务方法，负责将应用层的 protobuf 类型转换为领域模型所需的数据
    /// 返回包含所有数据的结构，调用者需要构建 UploadContext（因为 UploadContext 需要生命周期参数）
    pub fn prepare_upload_context_data<'a>(
        &self,
        metadata: &'a flare_grpc_proto::media::UploadFileMetadata,
    ) -> UploadContextData<'a> {
        let file_id = Uuid::new_v4().to_string();

        let mut extra_metadata = metadata.metadata.clone();

        if !metadata.namespace.is_empty() {
            extra_metadata
                .entry("namespace".to_string())
                .or_insert_with(|| metadata.namespace.clone());
        }

        if !metadata.business_tag.is_empty() {
            extra_metadata
                .entry("business_tag".to_string())
                .or_insert_with(|| metadata.business_tag.clone());
        }

        let trace_id = if metadata.trace_id.is_empty() {
            None
        } else {
            Some(metadata.trace_id.as_str())
        };

        let namespace = if metadata.namespace.is_empty() {
            None
        } else {
            Some(metadata.namespace.as_str())
        };

        let business_tag = if metadata.business_tag.is_empty() {
            None
        } else {
            Some(metadata.business_tag.as_str())
        };

        let file_category = infer_file_category(
            Some(metadata.file_type().as_str_name()),
            metadata.mime_type.as_str(),
        );
        extra_metadata
            .entry(FILE_CATEGORY_METADATA_KEY.to_string())
            .or_insert_with(|| file_category.clone());

        (
            file_id,
            file_category,
            extra_metadata,
            trace_id,
            namespace,
            business_tag,
        )
    }

    /// 准备分片上传初始化（从 protobuf 请求构建 MultipartUploadInit）
    ///
    /// 这是领域服务方法，负责将应用层的 protobuf 类型转换为领域模型
    pub fn prepare_multipart_upload_init(
        &self,
        request: &flare_grpc_proto::media::InitiateMultipartUploadRequest,
    ) -> Result<MultipartUploadInit> {
        let metadata = request.metadata.as_ref().ok_or_else(|| {
            flare_server_core::flare_err!(ErrorCode::InvalidParameter, "multipart metadata missing")
        })?;

        let desired_chunk_size = if request.desired_chunk_size > 0 {
            request.desired_chunk_size
        } else {
            256 * 1024 // 默认 256KB，更适合 HTTP JSON 分片上传
        };

        let file_category = infer_file_category(
            Some(metadata.file_type().as_str_name()),
            metadata.mime_type.as_str(),
        );

        let mut metadata_map = metadata.metadata.clone();
        metadata_map
            .entry(FILE_CATEGORY_METADATA_KEY.to_string())
            .or_insert_with(|| file_category.clone());

        Ok(MultipartUploadInit {
            file_name: metadata.file_name.clone(),
            mime_type: metadata.mime_type.clone(),
            file_size: if metadata.file_size > 0 {
                Some(metadata.file_size)
            } else {
                None
            },
            file_type: metadata.file_type().as_str_name().to_string(),
            chunk_size: desired_chunk_size,
            user_id: metadata.user_id.clone(),
            namespace: if metadata.namespace.is_empty() {
                None
            } else {
                Some(metadata.namespace.clone())
            },
            business_tag: if metadata.business_tag.is_empty() {
                None
            } else {
                Some(metadata.business_tag.clone())
            },
            trace_id: if metadata.trace_id.is_empty() {
                None
            } else {
                Some(metadata.trace_id.clone())
            },
            metadata: metadata_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn service() -> MediaService {
        MediaService::new(
            None,
            None,
            None,
            None,
            None,
            None,
            MediaDomainConfig::new(
                3600,
                None,
                60,
                std::env::temp_dir().join("flare-media-service-tests"),
                3600,
                8 * 1024 * 1024,
            ),
        )
    }

    #[test]
    fn default_user_upload_is_external_and_not_reference_managed() {
        let service = service();
        let payload = [];
        let context = UploadContext {
            file_id: "file-1",
            file_name: "avatar.png",
            mime_type: "image/png",
            file_size: 0,
            payload: &payload,
            file_category: "image".to_string(),
            user_id: "user-1",
            trace_id: None,
            namespace: None,
            business_tag: None,
            metadata: HashMap::new(),
        };

        assert!(!MediaService::is_message_lifecycle_context(&context));
        assert!(service.extract_reference_scope(&context).is_none());

        let (reference_count, status, grace_expires_at) =
            MediaService::initial_lifecycle_state(false, true, 60);
        assert_eq!(reference_count, 1);
        assert_eq!(status, MediaAssetStatus::Active);
        assert!(grace_expires_at.is_none());
    }

    #[test]
    fn message_lifecycle_metadata_extracts_reference_scope() {
        let service = service();
        let payload = [];
        let mut metadata = HashMap::new();
        metadata.insert(
            MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string(),
            MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string(),
        );
        let context = UploadContext {
            file_id: "file-1",
            file_name: "message.png",
            mime_type: "image/png",
            file_size: 0,
            payload: &payload,
            file_category: "image".to_string(),
            user_id: "user-1",
            trace_id: None,
            namespace: None,
            business_tag: None,
            metadata,
        };

        let scope = service
            .extract_reference_scope(&context)
            .expect("message media should create a reference scope");
        assert_eq!(scope.namespace, MESSAGE_MEDIA_LIFECYCLE_SCOPE);
        assert_eq!(
            scope.business_tag.as_deref(),
            Some(MESSAGE_MEDIA_LIFECYCLE_SCOPE)
        );

        let ref_id = MediaService::deterministic_reference_id("tenant-a", "file-1", &scope);
        assert_eq!(
            ref_id,
            MediaService::deterministic_reference_id("tenant-a", "file-1", &scope)
        );
        assert_ne!(
            ref_id,
            MediaService::deterministic_reference_id("tenant-b", "file-1", &scope)
        );
    }

    #[test]
    fn zero_reference_lifecycle_archives_message_media_only() {
        let mut external = MediaFileMetadata {
            file_id: "external-file".to_string(),
            status: MediaAssetStatus::Pending,
            reference_count: 0,
            ..Default::default()
        };
        MediaService::stamp_lifecycle_scope(&mut external.metadata, false);
        MediaService::apply_reference_lifecycle(&mut external, 60);
        assert_eq!(external.status, MediaAssetStatus::Active);
        assert!(external.grace_expires_at.is_none());

        let mut message = MediaFileMetadata {
            file_id: "message-file".to_string(),
            status: MediaAssetStatus::Active,
            reference_count: 0,
            ..Default::default()
        };
        MediaService::stamp_lifecycle_scope(&mut message.metadata, true);
        MediaService::apply_reference_lifecycle(&mut message, 60);
        assert_eq!(message.status, MediaAssetStatus::Pending);
        assert!(message.grace_expires_at.is_some());
    }
}
