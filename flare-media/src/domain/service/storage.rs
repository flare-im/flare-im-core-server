use super::*;

impl MediaService {
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

}
