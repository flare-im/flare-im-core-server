//! 配置管理器 - 负责处理不同环境下的配置选择和覆盖
//!
//! 该模块提供了配置管理功能，包括：
//! - 根据环境变量选择对象存储配置
//! - 加载环境特定配置
//! - 合并配置值（对象存储、`[mq]` 消息队列后端等）

use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

use anyhow::{Context as AnyhowContext, Result};
use toml::Value;

use super::{FlareAppConfig, MqBackendConfig, ObjectStoreConfig};

/// 配置管理器
pub struct ConfigManager;

impl ConfigManager {
    /// 根据环境变量或配置选择对象存储配置
    ///
    /// 优先级：
    /// 1. 环境变量 FLARE_OBJECT_STORE_PROFILE 指定的配置
    /// 2. 配置文件中指定的配置
    ///
    /// # 参数
    /// * `config` - 应用配置
    /// * `profile_name` - 配置文件中指定的配置名称
    ///
    /// # 返回
    /// 返回选中的对象存储配置，如果未找到则返回 None
    pub fn select_object_store_config(
        config: &FlareAppConfig,
        profile_name: &str,
    ) -> Option<ObjectStoreConfig> {
        if let Ok(env_profile) = env::var("FLARE_OBJECT_STORE_PROFILE")
            && let Some(store_config) = config.object_store_profile(&env_profile)
        {
            return Some(store_config.clone());
        }

        // 如果环境变量未设置或无效，则使用配置文件中指定的配置
        config.object_store_profile(profile_name).cloned()
    }

    /// 获取当前环境名称
    ///
    /// 从环境变量 FLARE_ENV 获取当前环境名称，
    /// 如果未设置则默认为 "development"
    ///
    /// # 返回
    /// 返回当前环境名称
    pub fn get_environment() -> String {
        env::var("FLARE_ENV").unwrap_or_else(|_| "development".to_string())
    }

    /// 根据环境加载特定配置
    ///
    /// 加载 config/environments/{environment}.toml 文件中的配置，
    /// 并将其合并到基础配置中
    ///
    /// # 参数
    /// * `base_config` - 基础配置，将被修改以包含环境特定配置
    ///
    /// # 返回
    /// 成功时返回 Ok(())，失败时返回错误信息
    pub fn load_environment_config(base_config: &mut FlareAppConfig) -> Result<()> {
        let env = Self::get_environment();
        let env_config_path = format!("config/environments/{}.toml", env);

        if Path::new(&env_config_path).exists() {
            let env_config_content = fs::read_to_string(&env_config_path)
                .with_context(|| format!("无法读取环境配置文件: {}", env_config_path))?;
            let env_config: Value = toml::from_str(&env_config_content)
                .with_context(|| format!("无效的环境配置格式: {}", env_config_path))?;

            // 合并环境配置到基础配置中
            Self::merge_config_values(&mut base_config.object_storage, &env_config);
            Self::merge_mq_from_environment(&mut base_config.mq, &env_config);
        }

        Ok(())
    }

    /// 合并配置值
    ///
    /// 将环境配置中的对象存储配置合并到基础配置中
    ///
    /// # 参数
    /// * `object_storage` - 基础对象存储配置映射，将被修改
    /// * `env_config` - 环境配置值
    fn merge_config_values(
        object_storage: &mut HashMap<String, ObjectStoreConfig>,
        env_config: &Value,
    ) {
        // 检查环境配置中是否包含对象存储配置
        if let Some(env_object_storage) = env_config.get("object_storage") {
            // 如果包含对象存储配置表，则遍历处理每个配置项
            if let Some(tables) = env_object_storage.as_table() {
                for (key, value) in tables {
                    // 只有当配置包含 profile_type 时才处理
                    if let Some(profile_type) = value.get("profile_type").and_then(|v| v.as_str()) {
                        let mut config = ObjectStoreConfig {
                            profile_type: profile_type.to_string(),
                            ..Default::default()
                        };

                        // 逐个字段处理配置值
                        if let Some(endpoint) = value.get("endpoint").and_then(|v| v.as_str()) {
                            config.endpoint = Some(endpoint.to_string());
                        }
                        if let Some(access_key) = value.get("access_key").and_then(|v| v.as_str()) {
                            config.access_key = Some(access_key.to_string());
                        }
                        if let Some(secret_key) = value.get("secret_key").and_then(|v| v.as_str()) {
                            config.secret_key = Some(secret_key.to_string());
                        }
                        if let Some(bucket) = value.get("bucket").and_then(|v| v.as_str()) {
                            config.bucket = Some(bucket.to_string());
                        }
                        if let Some(region) = value.get("region").and_then(|v| v.as_str()) {
                            config.region = Some(region.to_string());
                        }
                        if let Some(use_ssl) = value.get("use_ssl").and_then(|v| v.as_bool()) {
                            config.use_ssl = Some(use_ssl);
                        }
                        if let Some(cdn_base_url) =
                            value.get("cdn_base_url").and_then(|v| v.as_str())
                        {
                            config.cdn_base_url = Some(cdn_base_url.to_string());
                        }
                        if let Some(upload_prefix) =
                            value.get("upload_prefix").and_then(|v| v.as_str())
                        {
                            config.upload_prefix = Some(upload_prefix.to_string());
                        }

                        object_storage.insert(key.clone(), config);
                    }
                }
            }
        }
    }

    /// 从 `config/environments/*.toml` 根表合并 `[mq]`（与 `FLARE_MQ_*` 环境变量相比优先级更低，见 `apply_mq_env_overrides`）
    fn merge_mq_from_environment(mq: &mut MqBackendConfig, env_config: &Value) {
        let Some(tbl) = env_config.get("mq").and_then(|v| v.as_table()) else {
            return;
        };
        if let Some(raw) = tbl.get("default_backend").and_then(|v| v.as_str()) {
            let t = raw.trim();
            if !t.is_empty() {
                mq.default_backend = t.to_ascii_lowercase();
            }
        }
        if let Some(v) = tbl.get("allow_kafka_fallback") {
            if let Some(b) = v.as_bool() {
                mq.allow_kafka_fallback = b;
            } else if let Some(s) = v.as_str() {
                let u = s.trim();
                mq.allow_kafka_fallback = matches!(u, "1" | "true" | "yes");
            }
        }
    }
}
