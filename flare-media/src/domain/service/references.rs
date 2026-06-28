use super::*;

impl MediaService {
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

}
