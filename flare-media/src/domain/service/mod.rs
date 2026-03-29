use anyhow::{Context as AnyhowContext, Result, anyhow, bail};
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
    FILE_CATEGORY_METADATA_KEY, FileAccessType, MediaAssetStatus, MediaDomainConfig,
    MediaFileMetadata, MediaReference, MediaReferenceScope, MultipartChunkPayload,
    MultipartUploadInit, MultipartUploadSession, PresignedUrl, STORAGE_BUCKET_METADATA_KEY,
    STORAGE_PATH_METADATA_KEY, UploadContext, UploadSession, UploadSessionStatus,
    infer_file_category,
};
use crate::domain::repository::{
    LocalStoreRef, MetadataCacheRef, MetadataStoreRef, ObjectRepositoryRef, ReferenceStoreRef,
    UploadSessionStoreRef,
};

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
        ctx.ensure_not_cancelled()?;

        let Some(store) = &self.upload_conversation_store else {
            bail!("multipart upload is not configured");
        };

        let chunk_size = init
            .chunk_size
            .max(1_048_576)
            .min(self.config.max_chunk_size_bytes);

        let upload_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let expires_at = now + Duration::seconds(self.config.chunk_ttl_seconds.max(60));

        let session = UploadSession {
            upload_id: upload_id.clone(),
            file_name: init.file_name,
            mime_type: init.mime_type,
            file_type: init.file_type,
            chunk_size,
            total_size: init.file_size,
            uploaded_size: 0,
            uploaded_chunks: Vec::new(),
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
        ctx.ensure_not_cancelled()?;

        let Some(store) = &self.upload_conversation_store else {
            bail!("multipart upload is not configured");
        };

        let mut session = store
            .get_session(&chunk.upload_id)
            .await?
            .ok_or_else(|| anyhow!("upload session not found: {}", chunk.upload_id))?;

        if session.status != UploadSessionStatus::Pending {
            bail!("upload session is not pending");
        }

        let chunk_len = chunk.bytes.len() as i64;
        if chunk_len == 0 {
            bail!("chunk payload is empty");
        }
        if chunk_len > self.config.max_chunk_size_bytes {
            bail!(
                "chunk size {} exceeds limit {}",
                chunk_len,
                self.config.max_chunk_size_bytes
            );
        }

        let session_dir = self.ensure_session_dir(&chunk.upload_id).await?;
        let chunk_path = session_dir.join(format!("{:06}.part", chunk.chunk_index));

        if let Ok(metadata) = fs::metadata(&chunk_path).await {
            if metadata.len() as i64 == chunk_len {
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
        }

        let mut file = fs::File::create(&chunk_path)
            .await
            .with_context(|| format!("failed to create chunk file {:?}", chunk_path))?;
        file.write_all(&chunk.bytes)
            .await
            .context("failed to write chunk data")?;
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
        ctx.ensure_not_cancelled()?;

        let Some(store) = &self.upload_conversation_store else {
            bail!("multipart upload is not configured");
        };

        let mut session = store
            .get_session(upload_id)
            .await?
            .ok_or_else(|| anyhow!("upload session not found: {}", upload_id))?;

        if session.uploaded_chunks.is_empty() {
            bail!("no chunks uploaded for session {upload_id}");
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
        ctx.ensure_not_cancelled()?;

        let Some(store) = &self.upload_conversation_store else {
            bail!("multipart upload is not configured");
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
        ctx.ensure_not_cancelled()?;

        tracing::debug!(
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

        let sha256 = self.compute_sha256(context.payload);
        tracing::debug!(
            file_id = context.file_id,
            sha256 = &sha256,
            "计算文件SHA256哈希"
        );

        let scope = self.extract_reference_scope(&context);
        tracing::debug!(file_id = context.file_id, scope = ?scope, "提取引用范围");

        if let Some(store) = &self.metadata_store {
            tracing::debug!(
                file_id = context.file_id,
                "检查数据库中是否已存在相同哈希的文件"
            );
            if let Some(mut existing) = store.load_by_hash(&sha256).await? {
                tracing::debug!(
                    file_id = context.file_id,
                    existing_file_id = existing.file_id,
                    "发现已存在的文件，使用去重机制"
                );
                if let Some(scope) = scope.as_ref() {
                    tracing::debug!(file_id = context.file_id, "为已存在的文件创建引用");
                    self.ensure_reference(ctx, &mut existing, &context, scope)
                        .await?;
                } else {
                    tracing::debug!(file_id = context.file_id, "增加已存在文件的引用计数");
                    existing.reference_count = existing.reference_count.saturating_add(1);
                    existing.status = MediaAssetStatus::Active;
                    existing.grace_expires_at = None;
                    self.save_and_cache(&existing).await?;
                }

                existing
                    .metadata
                    .entry(FILE_CATEGORY_METADATA_KEY.to_string())
                    .or_insert_with(|| category.clone());

                if let Some(cache) = &self.metadata_cache {
                    cache.cache_metadata(&existing).await.ok();
                }

                tracing::debug!(
                    file_id = context.file_id,
                    existing_file_id = existing.file_id,
                    "返回已存在的文件元数据"
                );
                return Ok(existing);
            } else {
                tracing::debug!(
                    file_id = context.file_id,
                    sha256 = &sha256,
                    "数据库中未找到相同哈希的文件"
                );
            }
        } else {
            tracing::warn!(file_id = context.file_id, "未配置元数据存储");
        }

        let md5 = Some(format!("{:x}", md5_compute(context.payload)));
        tracing::debug!(
            file_id = context.file_id,
            md5 = md5.as_ref().unwrap(),
            "计算文件MD5哈希"
        );

        let storage_bucket = self
            .object_repo
            .as_ref()
            .and_then(|repo| repo.bucket_name());

        let (url, cdn_url, storage_path) = if let Some(object_repo) = &self.object_repo {
            tracing::debug!(file_id = context.file_id, "使用对象存储存储文件");
            let path = object_repo.put_object(&context).await.map_err(|err| {
                tracing::error!(file_id = context.file_id, error = ?err, "上传对象到媒体存储失败");
                err
            })?;
            tracing::debug!(
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
                        tracing::error!(object_path = &path, error = %err, "生成预签名URL失败，回退到直链");
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
        } else if let Some(local_store) = &self.local_store {
            tracing::debug!(file_id = context.file_id, "使用本地存储存储文件");
            let path = local_store.write(&context).await?;
            tracing::debug!(
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
            return Err(anyhow!("no media storage backend configured"));
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

        tracing::debug!(
            file_id = context.file_id,
            url = &url,
            cdn_url = &cdn_url,
            "生成文件URL"
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
            reference_count: if self.reference_store.is_some() { 0 } else { 1 },
            status: if self.reference_store.is_some() {
                MediaAssetStatus::Pending
            } else {
                MediaAssetStatus::Active
            },
            grace_expires_at: if self.reference_store.is_some() {
                Some(Utc::now() + Duration::seconds(self.config.orphan_grace_seconds))
            } else {
                None
            },
            access_type: FileAccessType::default(), // 默认使用私有访问类型
            storage_bucket: storage_bucket.clone(),
            storage_path: storage_path.clone(),
        };

        tracing::debug!(file_id = context.file_id, "准备保存文件元数据");

        self.save_and_cache(&metadata)
            .await
            .context("persist metadata")?;

        tracing::debug!(file_id = context.file_id, "文件元数据已保存");

        if let (Some(scope), Some(_)) = (scope, self.reference_store.as_ref()) {
            tracing::debug!(file_id = context.file_id, "为新文件创建引用");
            self.ensure_reference(ctx, &mut metadata, &context, &scope)
                .await?;
            tracing::debug!(file_id = context.file_id, "文件引用已创建");
        }

        tracing::debug!(file_id = context.file_id, "文件存储完成");
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

            if metadata.reference_count == 0 {
                metadata.status = MediaAssetStatus::Pending;
                metadata.grace_expires_at =
                    Some(Utc::now() + Duration::seconds(self.config.orphan_grace_seconds));
            } else {
                metadata.status = MediaAssetStatus::Active;
                metadata.grace_expires_at = None;
            }

            self.save_and_cache(&metadata)
                .await
                .context("persist metadata reference update")?;

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
            let _ = store.delete_metadata(file_id).await;
        }

        if let Some(cache) = &self.metadata_cache {
            let _ = cache.invalidate(file_id).await;
        }

        Ok(())
    }

    pub async fn get_metadata(&self, ctx: &Context, file_id: &str) -> Result<MediaFileMetadata> {
        if let Some(cache) = &self.metadata_cache {
            if let Some(metadata) = cache.get_cached_metadata(file_id).await? {
                return Ok(metadata);
            }
        }

        if let Some(store) = &self.metadata_store {
            if let Some(metadata) = store.load_metadata(ctx, file_id).await? {
                if let Some(cache) = &self.metadata_cache {
                    cache.cache_metadata(&metadata).await.ok();
                }
                return Ok(metadata);
            }
        }

        Err(anyhow!("metadata not found: {file_id}"))
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
        ctx.ensure_not_cancelled()?;

        let _tenant_id = ctx
            .tenant_id()
            .ok_or_else(|| anyhow::anyhow!("tenant_id is required in context"))?;
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

        if let Some(repo) = &self.object_repo {
            match metadata.access_type {
                FileAccessType::Public => {
                    if url.is_empty() {
                        if let Some(base) = repo.base_url() {
                            url = Self::build_full_url(&base, &object_path);
                        }
                    }
                    if cdn_url.is_empty() {
                        if let Some(cdn_base) = self
                            .config
                            .cdn_base_url
                            .clone()
                            .or_else(|| repo.cdn_base_url())
                        {
                            cdn_url = Self::build_full_url(&cdn_base, &object_path);
                        }
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

                    if cdn_url.is_empty() {
                        if let Some(cdn_base) = self
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
        }

        if url.is_empty() {
            if let Some(base) = self.object_repo.as_ref().and_then(|repo| repo.base_url()) {
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
            if let Some(base) = self.config.cdn_base_url.clone() {
                cdn_url = Self::build_full_url(&base, &object_path);
            }
        }
        if cdn_url.is_empty() {
            cdn_url = metadata.cdn_url.clone();
        }

        let final_cdn_url = if cdn_url.is_empty() {
            if let Some(base) = self.config.cdn_base_url.clone() {
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
                reference_id: Uuid::new_v4().to_string(),
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

        file_metadata.status = MediaAssetStatus::Active;
        file_metadata.grace_expires_at = None;

        self.save_and_cache(&file_metadata).await?;

        Ok(file_metadata)
    }

    pub async fn remove_reference(
        &self,
        ctx: &Context,
        file_id: &str,
        reference_id: Option<&str>,
    ) -> Result<MediaFileMetadata> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let mut file_metadata = self.get_metadata(ctx, file_id).await?;

        if let Some(reference_store) = &self.reference_store {
            let removed = if let Some(reference_id) = reference_id {
                reference_store.delete_reference(reference_id).await?
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

        if file_metadata.reference_count == 0 {
            file_metadata.status = MediaAssetStatus::Pending;
            file_metadata.grace_expires_at =
                Some(Utc::now() + Duration::seconds(self.config.orphan_grace_seconds));
        } else {
            file_metadata.status = MediaAssetStatus::Active;
            file_metadata.grace_expires_at = None;
        }

        self.save_and_cache(&file_metadata).await?;

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
        ctx.ensure_not_cancelled()?;

        let Some(store) = &self.metadata_store else {
            return Ok(vec![]);
        };

        let expired = store
            .list_orphaned_assets(Utc::now())
            .await
            .context("list orphaned media assets")?;

        for asset in &expired {
            let storage_path = asset
                .storage_path()
                .map(|s| s.to_string())
                .or_else(|| asset.metadata.get(STORAGE_PATH_METADATA_KEY).cloned())
                .unwrap_or_else(|| asset.file_id.clone());

            // 从 metadata 中提取 tenant_id，如果没有则使用默认值
            let tenant_id = asset
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
            let _ = store.delete_metadata(&asset.file_id).await;
            if let Some(cache) = &self.metadata_cache {
                let _ = cache.invalidate(&asset.file_id).await;
            }
        }

        Ok(expired.into_iter().map(|asset| asset.file_id).collect())
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
        fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("failed to prepare session directory {:?}", dir))?;
        Ok(dir)
    }

    async fn assemble_payload(&self, upload_id: &str, chunks: &[u32]) -> Result<Vec<u8>> {
        let dir = self.session_dir(upload_id);
        let mut payload = Vec::new();

        for index in chunks {
            let chunk_path = dir.join(format!("{:06}.part", index));
            let mut file = fs::File::open(&chunk_path)
                .await
                .with_context(|| format!("missing chunk file {:?}", chunk_path))?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)
                .await
                .with_context(|| format!("failed to read chunk {:?}", chunk_path))?;
            payload.extend_from_slice(&buffer);
        }

        Ok(payload)
    }

    async fn cleanup_chunks(&self, upload_id: &str) -> Result<()> {
        let dir = self.session_dir(upload_id);
        if fs::metadata(&dir).await.is_ok() {
            fs::remove_dir_all(&dir)
                .await
                .with_context(|| format!("failed to cleanup chunk directory {:?}", dir))?;
        }
        Ok(())
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

    fn extract_reference_scope(&self, context: &UploadContext<'_>) -> Option<MediaReferenceScope> {
        if context.user_id.is_empty() {
            return None;
        }

        let namespace = context
            .namespace
            .map(|value| value.to_string())
            .or_else(|| context.metadata.get("namespace").cloned())
            .unwrap_or_else(|| context.user_id.to_string());

        let business_tag = context
            .business_tag
            .map(|value| value.to_string())
            .or_else(|| context.metadata.get("business_tag").cloned());

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
            self.save_and_cache(metadata).await?;
            return Ok(());
        };

        if reference_store
            .reference_exists(
                ctx,
                &metadata.file_id,
                &scope.namespace,
                &scope.owner_id,
                scope.business_tag.as_deref(),
            )
            .await?
        {
            metadata.reference_count = reference_store
                .count_references(ctx, &metadata.file_id)
                .await?;
            metadata.status = MediaAssetStatus::Active;
            metadata.grace_expires_at = None;
            self.save_and_cache(metadata).await?;
            return Ok(());
        }

        let reference = MediaReference {
            reference_id: Uuid::new_v4().to_string(),
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

        metadata.status = if metadata.reference_count > 0 {
            MediaAssetStatus::Active
        } else {
            MediaAssetStatus::Pending
        };
        metadata.grace_expires_at = if metadata.reference_count > 0 {
            None
        } else {
            Some(Utc::now() + Duration::seconds(self.config.orphan_grace_seconds))
        };

        self.save_and_cache(metadata).await?;

        Ok(())
    }

    async fn save_and_cache(&self, metadata: &MediaFileMetadata) -> Result<()> {
        if let Some(store) = &self.metadata_store {
            store
                .save_metadata(metadata)
                .await
                .context("persist metadata")?;
        }

        if let Some(cache) = &self.metadata_cache {
            cache.cache_metadata(metadata).await.ok();
        }

        Ok(())
    }

    /// 准备上传上下文数据（从 protobuf 元数据提取并处理业务逻辑）
    ///
    /// 这是领域服务方法，负责将应用层的 protobuf 类型转换为领域模型所需的数据
    /// 返回包含所有数据的结构，调用者需要构建 UploadContext（因为 UploadContext 需要生命周期参数）
    pub fn prepare_upload_context_data<'a>(
        &self,
        metadata: &'a flare_proto::media::UploadFileMetadata,
    ) -> (
        String,
        String,
        HashMap<String, String>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
    ) {
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
        request: &flare_proto::media::InitiateMultipartUploadRequest,
    ) -> Result<MultipartUploadInit> {
        let metadata = request
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow!("multipart metadata missing"))?;

        let desired_chunk_size = if request.desired_chunk_size > 0 {
            request.desired_chunk_size
        } else {
            8 * 1024 * 1024 // 默认 8MB
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
