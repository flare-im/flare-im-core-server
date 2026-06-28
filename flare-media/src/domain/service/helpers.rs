use super::*;

impl MediaService {
    pub(super) fn compute_sha256(&self, payload: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(payload);
        format!("{:x}", hasher.finalize())
    }

    pub(super) fn session_dir(&self, upload_id: &str) -> PathBuf {
        self.config.chunk_root_dir.join(upload_id)
    }

    pub(super) async fn ensure_session_dir(&self, upload_id: &str) -> Result<PathBuf> {
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

    pub(super) async fn assemble_payload(&self, upload_id: &str, chunks: &[u32]) -> Result<Vec<u8>> {
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

    pub(super) async fn cleanup_chunks(&self, upload_id: &str) -> Result<()> {
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

    pub(super) fn ensure_direct_session(&self, session: &UploadSession) -> Result<()> {
        if session.transport_kind.is_none() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "upload session is not a direct upload session"
            ));
        }
        Ok(())
    }

    pub(super) fn direct_state_from_session(&self, session: &UploadSession) -> DirectUploadSessionState {
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

    pub(super) async fn persist_direct_upload_metadata(
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

    pub(super) fn ensure_file_category(context: &mut UploadContext<'_>) -> String {
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

    pub(super) fn build_full_url(base: &str, path: &str) -> String {
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

    pub(super) fn normalized_metadata_value<'a>(
        metadata: &'a HashMap<String, String>,
        keys: &[&str],
    ) -> Option<&'a str> {
        keys.iter()
            .find_map(|key| metadata.get(*key))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    }

    pub(super) fn is_message_lifecycle_value(value: &str) -> bool {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            MESSAGE_MEDIA_LIFECYCLE_SCOPE | "messages" | "im_message" | "im-message"
        )
    }

    pub(super) fn is_message_lifecycle_context(context: &UploadContext<'_>) -> bool {
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

    pub(super) fn is_message_managed_asset(metadata: &MediaFileMetadata) -> bool {
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

    pub(super) fn stamp_lifecycle_scope(metadata: &mut HashMap<String, String>, message_managed: bool) {
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

    pub(super) fn initial_lifecycle_state(
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

    pub(super) fn apply_reference_lifecycle(metadata: &mut MediaFileMetadata, orphan_grace_seconds: i64) {
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

    pub(super) fn can_reuse_hash_match(existing: &MediaFileMetadata, message_managed: bool) -> bool {
        message_managed || !Self::is_message_managed_asset(existing) || existing.reference_count > 0
    }

    pub(super) fn deterministic_reference_id(
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

    pub(super) fn extract_reference_scope(&self, context: &UploadContext<'_>) -> Option<MediaReferenceScope> {
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

    pub(super) fn reference_payload_from_context(
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

    pub(super) async fn ensure_reference(
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

    pub(super) async fn save_and_cache(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
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
