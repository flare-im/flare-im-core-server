use flare_im_service_kit::config::FlareAppConfig;
use flare_server_core::error::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct OnlineConfig {
    pub redis_url: String,
    pub redis_ttl_seconds: u64,
    pub presence_prefix: String,
}

impl OnlineConfig {
    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(app: &FlareAppConfig) -> Result<Self> {
        let service_config = app.signaling_online_service();

        // 解析 Redis 配置引用
        let redis_url = env::var("SIGNALING_ONLINE_REDIS_URL")
            .ok()
            .or_else(|| {
                if let Some(redis_name) = &service_config.redis {
                    app.redis_profile(redis_name)
                        .map(|profile| profile.url.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());

        let redis_ttl_seconds = env::var("SIGNALING_ONLINE_REDIS_TTL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .or(service_config.online_ttl_seconds)
            .unwrap_or(3600);

        let presence_prefix = env::var("SIGNALING_ONLINE_PRESENCE_PREFIX")
            .ok()
            .or_else(|| service_config.presence_prefix.clone())
            .unwrap_or_else(|| "presence:user".to_string());

        Ok(Self {
            redis_url,
            redis_ttl_seconds,
            presence_prefix,
        })
    }
}
