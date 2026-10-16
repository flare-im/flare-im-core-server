use super::*;

impl MediaService {
    pub async fn add_reference(
        &self,
        ctx: &Context,
        file_id: &str,
        scope: MediaReferenceScope,
        metadata: HashMap<String, String>,
    ) -> Result<MediaFileMetadata> {
        let tenant_id = require_tenant(ctx)?;
        let mut file_metadata = self.get_metadata(ctx, file_id).await?;
        Self::authorize_file_write(ctx, &file_metadata)?;

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
        require_tenant(ctx)?;
        let mut file_metadata = self.get_metadata(ctx, file_id).await?;
        Self::authorize_file_write(ctx, &file_metadata)?;

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
        self.get_metadata(ctx, file_id).await?;
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
    pub async fn cleanup_orphaned_assets(&self, ctx: &Context, limit: u32) -> Result<Vec<String>> {
        ctx.ensure_not_cancelled().map_err(map_context_error)?;
        let tenant = require_tenant(ctx)?;
        let limit = if limit == 0 { 100 } else { limit.min(1000) };

        let Some(store) = &self.metadata_store else {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ServiceUnavailable,
                "media metadata store is not configured"
            ));
        };
        if self.object_repo.is_none() && self.local_store.is_none() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ServiceUnavailable,
                "media object storage is not configured"
            ));
        }

        let now = Utc::now();
        let candidates = store
            .list_orphaned_assets(ctx, now, limit)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "list orphaned media assets")
            })?;

        let mut cleaned = Vec::new();
        for candidate in candidates {
            if !Self::is_message_managed_asset(&candidate) {
                tracing::trace!(
                    file_id = candidate.file_id,
                    "跳过非消息生命周期媒体的自动归档清理"
                );
                continue;
            }

            // 候选列表只是快照：删除前在行锁内重新确认仍是孤儿。期间已被引用的资产会被放弃；
            // 认领持有期间的新引用会等待，认领提交后因资产不存在而失败。
            let Some(claim) = store
                .claim_orphaned_asset(ctx, &candidate.file_id, now)
                .await?
            else {
                tracing::debug!(
                    file_id = candidate.file_id,
                    "孤儿候选在清理前已被引用或变更，跳过"
                );
                continue;
            };
            let asset = claim.asset();
            let file_id = asset.file_id.clone();
            let removed_bytes = usage_bytes(asset.file_size);
            let storage_path = asset
                .storage_path()
                .map(|s| s.to_string())
                .or_else(|| asset.metadata.get(STORAGE_PATH_METADATA_KEY).cloned())
                .unwrap_or_else(|| asset.file_id.clone());

            // 对象删除失败时丢弃认领（回滚），元数据原样保留供下次重试；删除对象是幂等的。
            if let Some(repo) = &self.object_repo {
                repo.delete_object(&storage_path).await?;
            }
            if let Some(local) = &self.local_store {
                local.delete(&storage_path).await?;
            }
            claim.commit().await?;
            self.usage.media_removed(tenant, removed_bytes);
            if let Some(cache) = &self.metadata_cache {
                cache.invalidate(ctx, &file_id).await?;
            }
            cleaned.push(file_id);
        }

        Ok(cleaned)
    }
}
