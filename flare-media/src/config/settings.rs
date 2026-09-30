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

/// 空串一律当成「没配」。
///
/// TOML 里 `${VAR:-}` 在环境变量缺省时会展开成空串而不是消失，直接当成有效值用会
/// 拼出相对路径这种半可用的地址——比彻底没配更难查。
pub(crate) fn normalize_optional_url(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// 服务级 `cdn_base_url` 优先，其次对象存储档案里的；两处的空白值都按没配处理。
///
/// base.toml 写的是 `cdn_base_url = "${FLARE_S3_CDN_BASE_URL:-}"`，环境变量不设时
/// 展开成空串。此前这里原样拷成 `Some("")`，领域层 `build_full_url("", key)` 于是
/// 返回裸对象 key 并落进 `cdn_url`，`/files/{id}` 的 307 就成了相对地址。
fn resolve_cdn_base_url(
    service: &Option<String>,
    object_store: Option<&Option<String>>,
) -> Option<String> {
    normalize_optional_url(service).or_else(|| object_store.and_then(normalize_optional_url))
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

        let cdn_base_url = resolve_cdn_base_url(
            &service.cdn_base_url,
            object_store.as_ref().map(|store| &store.cdn_base_url),
        );

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

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_service_kit::config::FlareAppConfig;

    /// 与 base.toml 同形：两处 `cdn_base_url` 都是 `${FLARE_S3_CDN_BASE_URL:-}` 展开后的值。
    fn app_config(service_cdn: &str, store_cdn: &str) -> FlareAppConfig {
        toml::from_str(&format!(
            r#"
            [service]
            name = "flare-media"
            version = "0.0.0"

            [server]
            address = "127.0.0.1"
            port = 1

            [object_storage.default]
            profile_type = "s3"
            cdn_base_url = "{store_cdn}"

            [services.media]
            object_store = "default"
            cdn_base_url = "{service_cdn}"
            "#
        ))
        .expect("minimal media config")
    }

    /// 环境变量没设时两处都展开成空串，不能变成 `Some("")` 传给领域层。
    #[test]
    fn blank_cdn_base_url_from_env_expansion_is_unset() {
        for blank in ["", "   "] {
            let config = MediaConfig::from_app_config(&app_config(blank, blank));
            assert_eq!(config.cdn_base_url, None, "blank {blank:?}");
        }
    }

    /// 服务级空白时回落到对象存储档案里真配的 CDN；两处都有时服务级优先，且去掉首尾空白。
    #[test]
    fn service_level_cdn_wins_and_blank_falls_back_to_the_store_profile() {
        let config = MediaConfig::from_app_config(&app_config("", "https://cdn.example.com/m"));
        assert_eq!(
            config.cdn_base_url.as_deref(),
            Some("https://cdn.example.com/m")
        );
        let config = MediaConfig::from_app_config(&app_config(
            " https://svc.example.com ",
            "https://cdn.example.com/m",
        ));
        assert_eq!(
            config.cdn_base_url.as_deref(),
            Some("https://svc.example.com")
        );
    }
}
