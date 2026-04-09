//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::application::handlers::{MediaCommandHandler, MediaQueryHandler};
use crate::config::MediaConfig;
use crate::domain::model::MediaDomainConfig;
use crate::domain::repository::{
    LocalStoreRef, MetadataCacheRef, MetadataStoreRef, ObjectRepositoryRef, ReferenceStoreRef,
    UploadSessionStoreRef,
};
use crate::domain::service::MediaService;
use crate::infrastructure::cache::redis_metadata::RedisMetadataCache;
use crate::infrastructure::conversation::fallback_session::FallbackUploadSessionStore;
use crate::infrastructure::conversation::memory_session::MemoryUploadSessionStore;
use crate::infrastructure::conversation::redis_session::RedisUploadSessionStore;
use crate::infrastructure::local::filesystem::FilesystemMediaStore;
use crate::infrastructure::object_store::adapter::build_object_store;
use crate::infrastructure::persistence::postgres_metadata::PostgresMetadataStore;
use crate::interface::grpc::MediaGrpcHandler;
use tracing::warn;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub handler: MediaGrpcHandler,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// # 参数
/// * `app_config` - 应用配置
///
/// # 返回
/// * `ApplicationContext` - 构建好的应用上下文
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    // 1. 加载配置
    let media_config = MediaConfig::from_app_config(app_config);

    // 2. 构建媒体服务
    let media_service = build_media_service(&media_config)
        .await
        .context("Failed to build media service")?;

    // 3. 构建命令处理器
    let command_handler = Arc::new(MediaCommandHandler::new(media_service.clone()));

    // 4. 构建查询处理器
    let query_handler = Arc::new(MediaQueryHandler::new(media_service));

    // 5. 构建 gRPC 处理器
    let handler = MediaGrpcHandler::new(command_handler, query_handler);

    Ok(ApplicationContext { handler })
}

/// 构建媒体服务
async fn build_media_service(config: &MediaConfig) -> Result<Arc<MediaService>> {
    let object_repo: Option<ObjectRepositoryRef> =
        build_object_store(config.object_store.as_ref()).await?;

    let (metadata_store, reference_store): (Option<MetadataStoreRef>, Option<ReferenceStoreRef>) =
        match config.postgres_url() {
            Some(url) => {
                let store = PostgresMetadataStore::new(url).await?;
                let metadata_store: MetadataStoreRef = Arc::new(store.clone());
                let reference_store: ReferenceStoreRef = Arc::new(store.clone());
                (Some(metadata_store), Some(reference_store))
            }
            None => (None, None),
        };

    let metadata_cache: Option<MetadataCacheRef> = match config.redis_url() {
        Some(url) => Some(
            Arc::new(RedisMetadataCache::new(url, &config.redis_namespace).await?)
                as MetadataCacheRef,
        ),
        None => None,
    };

    let memory_fallback_store: UploadSessionStoreRef =
        Arc::new(MemoryUploadSessionStore::new()) as UploadSessionStoreRef;
    let upload_conversation_store: Option<UploadSessionStoreRef> =
        match config.upload_session_redis_url() {
            Some(url) => {
                match RedisUploadSessionStore::new(
                    url,
                    &config.upload_session_namespace,
                    config.chunk_ttl_seconds,
                )
                .await
                {
                    Ok(store) => {
                        warn!("multipart upload session store mode: redis(primary)+memory(fallback)");
                        let primary: UploadSessionStoreRef = Arc::new(store) as UploadSessionStoreRef;
                        Some(
                            Arc::new(FallbackUploadSessionStore::new(primary, memory_fallback_store))
                                as UploadSessionStoreRef,
                        )
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            "failed to initialize redis upload session store, fallback to in-memory store"
                        );
                        Some(memory_fallback_store)
                    }
                }
            }
            None => {
                warn!(
                    "upload_session_redis_url is not configured, fallback to in-memory multipart session store"
                );
                Some(memory_fallback_store)
            }
        };

    let local_store: Option<LocalStoreRef> = match config.local_storage_dir.as_deref() {
        Some(dir) => Some(Arc::new(FilesystemMediaStore::new(
            dir,
            config.local_base_url.clone(),
        )?)),
        None => None,
    };

    // 构建领域配置值对象
    let domain_config = MediaDomainConfig::new(
        config.redis_ttl_seconds,
        config.cdn_base_url.clone(),
        config.orphan_grace_seconds,
        std::path::PathBuf::from(&config.chunk_upload_dir),
        config.chunk_ttl_seconds,
        config.max_chunk_size_bytes,
    );

    Ok(Arc::new(MediaService::new(
        object_repo,
        metadata_store,
        reference_store,
        metadata_cache,
        upload_conversation_store,
        local_store,
        domain_config,
    )))
}
