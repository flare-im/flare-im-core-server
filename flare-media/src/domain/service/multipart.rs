use super::*;

impl MediaService {
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

}
