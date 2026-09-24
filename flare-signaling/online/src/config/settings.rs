use flare_im_service_kit::config::FlareAppConfig;
use flare_server_core::error::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct OnlineConfig {
    pub redis_url: String,
    pub redis_ttl_seconds: u64,
    pub presence_prefix: String,
    /// 撤销即断的 kick 频道所在 Redis（与网关订阅的 token_store profile 同一实例）。
    pub kick_redis_url: String,
    /// kick 频道名：`{token_store namespace}:kick`，与 signaling-gateway `revoke_subscriber` 一致。
    pub kick_channel: String,
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

        // KickTenant 踢连接要让网关真的关 socket：复用 api-gateway / signaling-gateway 已有的
        // `{namespace}:kick` 频道（token_store profile）。
        let token_store = app.redis_profile("token_store");
        let kick_redis_url = env::var("SIGNALING_ONLINE_KICK_REDIS_URL")
            .ok()
            .or_else(|| token_store.map(|profile| profile.url.clone()))
            .unwrap_or_else(|| redis_url.clone());
        let kick_channel = env::var("SIGNALING_ONLINE_KICK_CHANNEL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                let namespace = token_store
                    .and_then(|profile| profile.namespace.clone())
                    .unwrap_or_else(|| "flare".to_string());
                format!("{namespace}:kick")
            });

        Ok(Self {
            redis_url,
            redis_ttl_seconds,
            presence_prefix,
            kick_redis_url,
            kick_channel,
        })
    }
}
