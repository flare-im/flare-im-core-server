//! 辅助工具函数模块
//!
//! 提供配置加载、服务初始化等常用辅助函数

use crate::config::{FlareAppConfig, ServiceRuntimeConfig};
use flare_server_core::error::{AnyhowContext, Result};
use std::net::SocketAddr;

/// 服务启动辅助函数
pub struct ServiceHelper;

impl ServiceHelper {
    /// 加载配置并验证
    ///
    /// # 参数
    /// * `config_path` - 配置路径
    /// * `strict` - 是否严格验证配置引用
    ///
    /// # 返回
    /// 返回加载的配置实例
    pub fn load_config(config_path: Option<&str>, strict: bool) -> Result<&'static FlareAppConfig> {
        let config = crate::config::load_config(config_path);

        // 使用 guard clause 减少嵌套
        if strict {
            config
                .validate_references()
                .with_context(|| "configuration validation failed")?;
            return Ok(config);
        }

        // 非严格模式下，即使验证失败也继续运行，只记录警告日志
        if let Err(e) = config.validate_references() {
            tracing::warn!("configuration reference validation failed: {}", e);
        }

        Ok(config)
    }

    /// 从服务配置中解析服务器地址
    ///
    /// # 参数
    /// * `config` - 应用配置
    /// * `runtime` - 服务运行时配置
    /// * `fallback_name` - 服务名称（如果运行时配置未指定）
    ///
    /// # 返回
    /// 返回解析的 SocketAddr
    pub fn parse_server_addr(
        config: &FlareAppConfig,
        runtime: &ServiceRuntimeConfig,
        fallback_name: &str,
    ) -> Result<SocketAddr> {
        let service_config = config.compose_service_config(runtime, fallback_name);
        let addr = format!(
            "{}:{}",
            service_config.server.address, service_config.server.port
        )
        .parse()
        .with_context(|| {
            format!(
                "invalid server address: {}:{}",
                service_config.server.address, service_config.server.port
            )
        })?;
        Ok(addr)
    }

    /// 从服务配置中解析服务器地址（使用默认地址）
    ///
    /// # 参数
    /// * `config` - 应用配置
    ///
    /// # 返回
    /// 返回解析的 SocketAddr
    pub fn parse_default_server_addr(config: &FlareAppConfig) -> Result<SocketAddr> {
        let addr = format!("{}:{}", config.core.server.address, config.core.server.port)
            .parse()
            .with_context(|| {
                format!(
                    "invalid default server address: {}:{}",
                    config.core.server.address, config.core.server.port
                )
            })?;
        Ok(addr)
    }
}
