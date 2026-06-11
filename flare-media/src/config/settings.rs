use flare_im_service_kit::config::{ObjectStoreConfig, PostgresInstanceConfig, RedisPoolConfig};

#[derive(Clone, Debug)]
pub struct MediaConfig {
    pub redis: Option<RedisPoolConfig>,
    pub redis_namespace: String,
    pub redis_ttl_seconds: i64,
    pub upload_session_redis: Option<RedisPoolConfig>,
    pub upload_session_namespace: String,
    pub postgres: Option<PostgresInstanceConfig>,
    pub object_store: Option<ObjectStoreConfig>,
    pub local_storage_dir: Option<String>,
    pub local_base_url: Option<String>,
    pub cdn_base_url: Option<String>,
    pub orphan_grace_seconds: i64,
    pub chunk_upload_dir: String,
    pub chunk_ttl_seconds: i64,
    pub max_chunk_size_bytes: i64,
}

impl MediaConfig {
    pub fn from_app_config(app: &flare_im_service_kit::config::FlareAppConfig) -> Self {
        let service = app.media_service();

        let redis_profile = service
            .metadata_cache
            .as_deref()
            .and_then(|name| app.redis_profile(name))
            .cloned();

        let redis_namespace = redis_profile
            .as_ref()
            .and_then(|cfg| cfg.namespace.clone())
            .unwrap_or_else(|| "flare:media".to_string());

        let redis_ttl_seconds = service
            .redis_ttl_seconds
            .map(|ttl| ttl.max(0))
            .or_else(|| {
                redis_profile
                    .as_ref()
                    .and_then(|cfg| cfg.ttl_seconds.map(|ttl| ttl as i64))
            })
            .unwrap_or(3600);

        let postgres = service
            .metadata_store
            .as_deref()
            .and_then(|name| app.postgres_profile(name))
            .cloned();

        // 使用配置管理器选择对象存储配置
        let object_store = service
            .object_store
            .as_deref()
            .and_then(|name| {
                flare_im_service_kit::config::ConfigManager::select_object_store_config(app, name)
            })
            .or_else(|| {
                // 如果没有找到指定的配置，使用默认配置
                app.object_store_profile("default").cloned()
            });

        let cdn_base_url = service.cdn_base_url.clone().or_else(|| {
            object_store
                .as_ref()
                .and_then(|store| store.cdn_base_url.clone())
        });

        let orphan_grace_seconds = service.orphan_grace_seconds.unwrap_or(86_400).max(0);

        let upload_session_profile = service
            .upload_session_store
            .as_deref()
            .and_then(|name| app.redis_profile(name))
            .cloned()
            .or_else(|| redis_profile.clone());

        let upload_session_namespace = upload_session_profile
            .as_ref()
            .and_then(|cfg| cfg.namespace.clone())
            .unwrap_or_else(|| format!("{}:upload", redis_namespace));

        let chunk_upload_dir = service
            .chunk_upload_dir
            .clone()
            .unwrap_or_else(|| "./data/media/chunks".to_string());

        let chunk_ttl_seconds = service.chunk_ttl_seconds.unwrap_or(172_800).max(0);

        let max_chunk_size_bytes = service
            .max_chunk_size_bytes
            .unwrap_or(50 * 1024 * 1024)
            .max(1_048_576);

        Self {
            redis: redis_profile,
            redis_namespace,
            redis_ttl_seconds,
            upload_session_redis: upload_session_profile,
            upload_session_namespace,
            postgres,
            object_store,
            local_storage_dir: service.local_storage_dir,
            local_base_url: service.local_base_url,
            cdn_base_url,
            orphan_grace_seconds,
            chunk_upload_dir,
            chunk_ttl_seconds,
            max_chunk_size_bytes,
        }
    }

    pub fn redis_url(&self) -> Option<&str> {
        self.redis.as_ref().map(|cfg| cfg.url.as_str())
    }

    pub fn postgres_url(&self) -> Option<&str> {
        self.postgres.as_ref().map(|cfg| cfg.url.as_str())
    }

    pub fn upload_session_redis_url(&self) -> Option<&str> {
        self.upload_session_redis
            .as_ref()
            .map(|cfg| cfg.url.as_str())
    }
}
