use super::*;
use crate::domain::model::CompletionClaim;
use chrono::DateTime;

impl MediaService {
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
        let tenant_id = require_tenant(ctx)?;
        let owner_id = tenant::require_owner(ctx)?;

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
        let object_key = object_repo.build_object_key_for(
            tenant_id,
            &file_id,
            &metadata.file_name,
            &file_category,
        );

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
            // 声明的大小签进地址；地址不比会话活得久，会话过期回收后不会再有 PUT 落进来。
            let session_ttl = (expires_at - now).num_seconds();
            Some(
                object_repo
                    .presign_put_object(
                        &object_key,
                        &metadata.mime_type,
                        file_size,
                        self.config.default_ttl.min(session_ttl).max(1),
                    )
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
        metadata_map.insert("tenant_id".to_string(), tenant_id.to_string());
        metadata_map
            .entry(FILE_CATEGORY_METADATA_KEY.to_string())
            .or_insert_with(|| file_category.clone());

        let session = UploadSession {
            upload_id: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.to_string(),
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
            user_id: owner_id.to_string(),
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
            completion: None,
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
        Self::authorize_upload_session(ctx, &session)?;
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
        Self::authorize_upload_session(ctx, &session)?;
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
            // 每片的长度签进地址：除最后一片外都是 part_size，最后一片是余下的字节。
            let content_length = Self::direct_part_length(&session, *part_number)?;
            let url = object_repo
                .presign_upload_part(
                    object_key,
                    storage_upload_id,
                    *part_number,
                    content_length,
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
        let session = store.get_session(upload_id).await?.ok_or_else(|| {
            flare_server_core::flare_err_details!(
                ErrorCode::MessageNotFound,
                "upload session not found",
                format!("upload_id={upload_id}")
            )
        })?;
        Self::authorize_upload_session(ctx, &session)?;
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
        }
        // 在最新记录上合并分片并续期：同时到达的其他提交与完成的领取都不会被这次写入盖掉。
        let ttl = Duration::seconds(self.config.chunk_ttl_seconds.max(300));
        let session = store
            .update_session(upload_id, &|current| {
                let now = Utc::now();
                if current.expires_at < now {
                    return Err(Self::session_not_found(upload_id));
                }
                for part in parts {
                    current.merge_uploaded_part(part.clone());
                }
                current.updated_at = now;
                current.expires_at = now + ttl;
                Ok(true)
            })
            .await?
            .ok_or_else(|| Self::session_not_found(upload_id))?;
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
        Self::authorize_upload_session(ctx, &session)?;
        self.ensure_direct_session(&session)?;
        if session.status == UploadSessionStatus::Aborted {
            // 大小不符被拒、对象又没删掉的分片会话：留给回收任务删对象，不能再完成。
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "upload session is no longer active; start a new upload"
            ));
        }

        let object_key = session.object_key.clone().ok_or_else(|| {
            flare_server_core::flare_err!(ErrorCode::InternalError, "object_key missing in session")
        })?;
        let bucket = session.bucket.clone().unwrap_or_default();
        let file_id = session
            .file_id
            .clone()
            .unwrap_or_else(|| session.upload_id.clone());
        let file_size = session.total_size.unwrap_or_default();

        // 完成可以重试：上一次已登记文件、只是没删掉会话（或答复丢了）时，原样答复已登记的
        // 文件，不再碰存储，也不重写登记（重写会把其间加上的引用计数清掉）。
        if let Some(metadata_store) = &self.metadata_store
            && let Some(registered) = metadata_store.load_metadata(ctx, &file_id).await?
        {
            Self::finish_direct_session(store.as_ref(), &mut session).await;
            return Ok(registered);
        }

        let transport_kind = session
            .transport_kind
            .unwrap_or(DirectUploadTransportKind::SinglePut);
        // 先做不写会话的校验：单次 PUT 的对象已写入且大小与声明一致，分片会话已提交齐全部
        // 分片。通不过的完成不领取、不续期，反复调用也不能让会话一直续命。
        if transport_kind == DirectUploadTransportKind::SinglePut {
            // 对象不存在：调用方还没按地址 PUT 就来完成，是调用方错误。声明大小为 0 的上传
            // 写入 0 字节对象即可完成，与经 Media 中转的上传一致；其余大小由下面核对。
            let stat = object_repo.stat_object(&object_key).await?.ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "single put object is missing; upload it before completing"
                )
            })?;
            // 地址已签了长度；存储不校验 signed headers 时这里兜底。删掉写错的对象，
            // 会话保留，调用方可以在地址有效期内按声明大小重传。删除失败不改变答复：
            // 会话未完成、文件未登记，对象由回收任务在会话过期后删除。
            if let Err(err) = Self::ensure_declared_size(&stat, file_size) {
                if let Err(delete_err) = object_repo.delete_object(&object_key).await {
                    tracing::warn!(
                        error = %delete_err,
                        "failed to delete a single put object of the wrong size; reclaim will retry"
                    );
                }
                return Err(err);
            }
        } else {
            Self::ensure_all_parts_committed(&session)?;
        }

        // 领取完成并续期：合并与登记期间会话不会过期，回收任务领不到它，不会删掉正在合并的
        // 对象；同一会话的另一次完成在租期内得到可重试的拒绝，不会再合并、再登记一次。
        let claim = match self
            .claim_direct_completion(store.as_ref(), upload_id)
            .await
        {
            Ok(claim) => claim,
            // 另一次完成恰好在上面的检查之后登记完并结束了会话（会话已删除或已标为完成）：
            // 按已登记的文件答复，而不是报会话不存在。
            Err(err)
                if matches!(
                    err.code(),
                    Some(ErrorCode::MessageNotFound | ErrorCode::InvalidParameter)
                ) =>
            {
                if let Some(metadata_store) = &self.metadata_store
                    && let Some(registered) = metadata_store.load_metadata(ctx, &file_id).await?
                {
                    return Ok(registered);
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let mut session = claim.session.clone();

        let outcome = match transport_kind {
            DirectUploadTransportKind::SinglePut => Ok(()),
            DirectUploadTransportKind::MultipartPut => {
                self.merge_direct_parts(store.as_ref(), &mut session, &object_key, file_size)
                    .await
            }
        };
        let outcome = match outcome {
            Ok(()) => {
                self.persist_direct_upload_metadata(
                    ctx,
                    &session,
                    &bucket,
                    &object_key,
                    &file_id,
                    file_size,
                )
                .await
            }
            Err(err) => Err(err),
        };
        match outcome {
            Ok(metadata) => {
                Self::finish_direct_session(store.as_ref(), &mut session).await;
                Ok(metadata)
            }
            Err(err) => {
                Self::release_direct_completion(store.as_ref(), &claim, &err).await;
                Err(err)
            }
        }
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
            Self::authorize_upload_session(ctx, &session)?;
            self.ensure_direct_session(&session)?;
            // 中止也要释放存储侧资源：分片上传 abort，单次 PUT 已写入的对象删除。
            // 释放失败不挡中止，会话记录随后删除，残留由存储侧生命周期规则兜底。
            if let Err(err) = self.release_upload_resources(&session).await {
                tracing::warn!(error = %err, "failed to release aborted direct upload resources");
            }
            session.status = UploadSessionStatus::Aborted;
            session.updated_at = Utc::now();
            store.upsert_session(&session).await.ok();
        }

        store.delete_session(upload_id).await.ok();
        Ok(())
    }
}

/// 把分片会话标为中止的写入次数上限（含第一次）。
pub(super) const ABORT_MARK_ATTEMPTS: u32 = 3;

/// 一次直传完成的领取租期：领取者中途退出时，这么久之后另一次完成才能领取。合并与登记
/// 通常几秒内结束。
pub(super) const DIRECT_COMPLETION_LEASE_SECONDS: i64 = 300;

/// 领到的一次直传完成。
pub(super) struct DirectCompletionClaim {
    /// 领取写入后的会话记录（含其间提交的分片）。
    pub(super) session: UploadSession,
    /// 这次完成的幂等键，交还领取时凭它认出自己的领取。
    pub(super) token: String,
    /// 领取前的有效期：完成因调用方错误失败时交还续出的那段。
    pub(super) expires_before: DateTime<Utc>,
}

impl MediaService {
    fn session_not_found(upload_id: &str) -> flare_server_core::error::FlareError {
        flare_server_core::flare_err_details!(
            ErrorCode::MessageNotFound,
            "upload session not found",
            format!("upload_id={upload_id}")
        )
    }

    /// 分片会话必须先提交齐全部分片才能完成。
    fn ensure_all_parts_committed(session: &UploadSession) -> Result<()> {
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
        Ok(())
    }

    /// 在最新会话记录上领取这次完成并续期，只改领取与有效期两个字段。会话已过期、已结束、
    /// 分片没提交齐，或另一次完成的领取仍在租期内时拒绝，不写入。
    async fn claim_direct_completion(
        &self,
        store: &dyn crate::domain::repository::UploadSessionStore,
        upload_id: &str,
    ) -> Result<DirectCompletionClaim> {
        let token = Uuid::new_v4().to_string();
        let ttl = Duration::seconds(self.config.chunk_ttl_seconds.max(300));
        let lease = Duration::seconds(DIRECT_COMPLETION_LEASE_SECONDS);
        let expires_before = std::sync::Mutex::new(None);
        let session = store
            .update_session(upload_id, &|current| {
                let now = Utc::now();
                if current.expires_at < now {
                    return Err(Self::session_not_found(upload_id));
                }
                if current.status != UploadSessionStatus::Pending {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::InvalidParameter,
                        "upload session is no longer active; start a new upload"
                    ));
                }
                if current
                    .completion
                    .as_ref()
                    .is_some_and(|claim| claim.lease_until > now)
                {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::ServiceUnavailable,
                        "completion of this upload is already in progress; retry later"
                    ));
                }
                if current.transport_kind == Some(DirectUploadTransportKind::MultipartPut) {
                    Self::ensure_all_parts_committed(current)?;
                }
                *expires_before
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(current.expires_at);
                current.completion = Some(CompletionClaim {
                    token: token.clone(),
                    lease_until: now + lease,
                });
                current.expires_at = current.expires_at.max(now + ttl);
                current.updated_at = now;
                Ok(true)
            })
            .await?
            .ok_or_else(|| Self::session_not_found(upload_id))?;
        let expires_before = expires_before
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unwrap_or(session.expires_at);
        Ok(DirectCompletionClaim {
            session,
            token,
            expires_before,
        })
    }

    /// 完成失败后交还领取，另一次完成随即可以领取。失败是调用方错误（参数错误）时连这次
    /// 续出的有效期一并交还，反复失败的完成不能让会话续命；存储或数据库暂时不可用时保留
    /// 续期，客户端在有效期内重试。会话已被删除、标为中止或已被别的领取取代时不动。
    async fn release_direct_completion(
        store: &dyn crate::domain::repository::UploadSessionStore,
        claim: &DirectCompletionClaim,
        err: &flare_server_core::error::FlareError,
    ) {
        let restore = err.code() == Some(ErrorCode::InvalidParameter);
        let renewed_until = claim.session.expires_at;
        let result = store
            .update_session(&claim.session.upload_id, &|current| {
                if current.completion.as_ref().map(|c| c.token.as_str())
                    != Some(claim.token.as_str())
                {
                    return Ok(false);
                }
                current.completion = None;
                if restore && current.expires_at == renewed_until {
                    current.expires_at = claim.expires_before;
                }
                current.updated_at = Utc::now();
                Ok(true)
            })
            .await;
        if let Err(release_err) = result {
            tracing::warn!(
                error = %release_err,
                upload_id = %claim.session.upload_id,
                "failed to release a direct upload completion; it expires with its lease"
            );
        }
    }

    /// 合并分片并核对合并出的对象大小。不符时删除对象并结束会话；删除失败则把会话标为中止。
    async fn merge_direct_parts(
        &self,
        store: &dyn crate::domain::repository::UploadSessionStore,
        session: &mut UploadSession,
        object_key: &str,
        file_size: i64,
    ) -> Result<()> {
        let Some(object_repo) = &self.object_repo else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "object storage is required for direct upload"
            ));
        };
        let storage_upload_id = session.storage_upload_id.clone().ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::InternalError,
                "storage_upload_id missing in session"
            )
        })?;
        let merged = match object_repo
            .complete_multipart_upload(object_key, &storage_upload_id, &session.uploaded_parts)
            .await
        {
            Ok(()) => None,
            // 上一次完成已合并、却没登记上（或合并的答复丢了）：存储里已没有这个分片
            // 上传（NoSuchUpload），合并出的对象却在。对象 key 只属于这个会话，存在
            // 即说明已合并过，接着核对大小并登记。对象也不在时报原来的错误。
            Err(err) => match object_repo.stat_object(object_key).await {
                Ok(Some(stat)) => {
                    tracing::info!(
                        error = %err,
                        "merge failed but the merged object exists; registering it"
                    );
                    Some(stat)
                }
                _ => return Err(err),
            },
        };
        // 每片地址已签了长度；存储不校验 signed headers 时，合并后的对象在这里按声明
        // 大小兜底核对。分片上传已合并、无法重来，不符时删除对象并结束会话。
        let stat = match merged {
            Some(stat) => stat,
            None => object_repo.stat_object(object_key).await?.ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::InternalError,
                    "merged multipart object is missing"
                )
            })?,
        };
        if let Err(err) = Self::ensure_declared_size(&stat, file_size) {
            match object_repo.delete_object(object_key).await {
                Ok(()) => {
                    store.delete_session(&session.upload_id).await.ok();
                }
                Err(delete_err) => {
                    // 删除失败不改变答复。会话标为中止并保留，到期后由回收任务删对象。
                    tracing::warn!(
                        error = %delete_err,
                        "failed to delete a merged object of the wrong size; reclaim will retry"
                    );
                    session.status = UploadSessionStatus::Aborted;
                    session.updated_at = Utc::now();
                    Self::mark_session_aborted(store, session).await;
                }
            }
            return Err(err);
        }
        Ok(())
    }
    /// 完成收尾：标为已完成并删除会话记录。两步都尽力而为：记录残留时，重试完成按已登记的
    /// 文件答复，回收任务也不删已登记文件的对象。
    async fn finish_direct_session(
        store: &dyn crate::domain::repository::UploadSessionStore,
        session: &mut UploadSession,
    ) {
        session.status = UploadSessionStatus::Completed;
        session.updated_at = Utc::now();
        // 只改状态：记录已被另一次完成删掉时不重建它，也不拿手里的快照盖掉其间的写入。
        let marked = store
            .update_session(&session.upload_id, &|current| {
                current.status = UploadSessionStatus::Completed;
                current.completion = None;
                current.updated_at = Utc::now();
                Ok(true)
            })
            .await;
        if let Err(err) = marked {
            tracing::warn!(
                error = %err,
                upload_id = %session.upload_id,
                "failed to mark a direct upload session completed"
            );
        }
        if let Err(err) = store.delete_session(&session.upload_id).await {
            tracing::warn!(
                error = %err,
                upload_id = %session.upload_id,
                "failed to delete a completed direct upload session; reclaim removes the record"
            );
        }
    }

    /// 合并后大小不符、对象又没删掉的分片会话标为中止，之后不能再完成。写入失败时短暂退避
    /// 重试；仍失败只记 WARN，答复不变：会话停在待完成，再次完成会重新发现合并出的对象
    /// 大小不符（仍是 400、再删一次），回收任务到期后照样删掉这个未登记的对象。
    async fn mark_session_aborted(
        store: &dyn crate::domain::repository::UploadSessionStore,
        session: &UploadSession,
    ) {
        for attempt in 1..=ABORT_MARK_ATTEMPTS {
            // 只改状态与领取两个字段：不拿完成开始时的快照盖掉其间的写入。
            let marked = store
                .update_session(&session.upload_id, &|current| {
                    current.status = UploadSessionStatus::Aborted;
                    current.completion = None;
                    current.updated_at = Utc::now();
                    Ok(true)
                })
                .await;
            match marked {
                Ok(_) => return,
                Err(err) if attempt == ABORT_MARK_ATTEMPTS => tracing::warn!(
                    error = %err,
                    upload_id = %session.upload_id,
                    attempts = attempt,
                    "failed to mark a multipart session aborted; completion stays refused by size and reclaim deletes the object"
                ),
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt)))
                        .await;
                }
            }
        }
    }

    /// 分片直传第 `part_number` 片（从 1 起）应有的长度：除最后一片外都是 part_size，
    /// 最后一片是余下的字节。客户端按同样的规则切分（会话回给它 part_size 与 total_parts）。
    pub(super) fn direct_part_length(session: &UploadSession, part_number: u32) -> Result<i64> {
        let total_size = session.total_size.unwrap_or_default();
        let part_size = session.chunk_size;
        let offset = i64::from(part_number.saturating_sub(1)).saturating_mul(part_size);
        let length = total_size.saturating_sub(offset).min(part_size);
        if part_number == 0 || length <= 0 {
            return Err(flare_server_core::flare_err_details!(
                ErrorCode::InternalError,
                "direct upload session has no bytes for this part",
                format!("part_number={part_number} part_size={part_size} total_size={total_size}")
            ));
        }
        Ok(length)
    }

    /// 存储里的对象大小必须等于会话声明的大小。存储没报大小时按不符处理：
    /// 核对不了就不登记。
    pub(super) fn ensure_declared_size(stat: &ObjectStat, declared: i64) -> Result<()> {
        if stat.size == Some(declared) {
            return Ok(());
        }
        Err(flare_server_core::flare_err_details!(
            ErrorCode::InvalidParameter,
            "uploaded object size does not match the declared size",
            format!("declared={declared} actual={:?}", stat.size)
        ))
    }
}
