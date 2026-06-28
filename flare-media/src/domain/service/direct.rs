use super::*;

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

}
