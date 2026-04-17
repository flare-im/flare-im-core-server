//! # Hook配置监听器
//!
//! 监听配置变更并自动重新加载配置

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::domain::model::HookConfig;
use crate::infrastructure::config::loader::{ConfigLoaderItem, ConfigMerger, ConfigValidator};

/// 配置监听器
///
/// 监听配置变更并自动重新加载配置
pub struct ConfigWatcher {
    loaders: Vec<Arc<ConfigLoaderItem>>,
    current_config: Arc<RwLock<HookConfig>>,
    refresh_interval: Duration,
}

impl ConfigWatcher {
    pub fn new(loaders: Vec<Arc<ConfigLoaderItem>>, refresh_interval: Duration) -> Self {
        Self {
            loaders,
            current_config: Arc::new(RwLock::new(HookConfig::default())),
            refresh_interval,
        }
    }

    /// 获取当前配置
    pub async fn get_config(&self) -> HookConfig {
        self.current_config.read().await.clone()
    }

    /// 启动配置监听
    pub async fn start(&self) -> Result<()> {
        // 初始加载
        self.reload().await?;

        // 启动定时刷新任务
        let config = Arc::clone(&self.current_config);
        let loaders = self.loaders.clone();
        let interval = self.refresh_interval;

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;

                match Self::load_all(&loaders).await {
                    Ok(new_config) => {
                        // 验证配置
                        if let Err(e) = ConfigValidator::validate(&new_config) {
                            error!(error = %e, "Failed to validate hook config");
                            continue;
                        }

                        // 更新配置
                        *config.write().await = new_config;
                        info!("Hook config reloaded successfully");
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to reload hook config");
                    }
                }
            }
        });

        Ok(())
    }

    /// 重新加载配置
    pub async fn reload(&self) -> Result<()> {
        let new_config = Self::load_all(&self.loaders).await?;
        ConfigValidator::validate(&new_config)?;
        *self.current_config.write().await = new_config;
        Ok(())
    }

    async fn load_all(loaders: &[Arc<ConfigLoaderItem>]) -> Result<HookConfig> {
        let mut configs = Vec::new();

        for loader in loaders {
            match loader.load().await {
                Ok(config) => configs.push(config),
                Err(e) => {
                    warn!(error = %e, "Failed to load config from loader");
                }
            }
        }

        Ok(ConfigMerger::merge(configs))
    }
}
