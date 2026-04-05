//! 服务注册发现初始化（**保留在 flare-im-core**）
//!
//! 依赖 [crate::config::FlareAppConfig] 与全局 `app_config()`，与 IM 多服务 TOML 强绑定；不宜迁入 `flare-server-core`
//!（避免 server-core 反向依赖应用配置形状）。底层注册/发现类型仍来自 `flare_server_core::discovery`。

use std::net::SocketAddr;
use uuid::Uuid;

use crate::config::FlareAppConfig;
use crate::RegistryConfig;
use super::{
    BackendType, DiscoveryFactory, ServiceDiscover, ServiceDiscoverUpdater, ServiceInstance,
    ServiceRegistry, TagFilter,
};

/// 将注册中心类型字符串转换为 BackendType
///
/// 这是一个内部辅助函数，用于统一处理 backend type 的转换逻辑
fn parse_backend_type(registry_type: &str) -> Result<BackendType, String> {
    match registry_type.to_lowercase().as_str() {
        "etcd" => Ok(BackendType::Etcd),
        "consul" => Ok(BackendType::Consul),
        "dns" => Ok(BackendType::Dns),
        "mesh" => Ok(BackendType::Mesh),
        _ => Err(format!("Unsupported registry type: {}", registry_type)),
    }
}

/// 生成实例 ID（如果未提供）
///
/// 生成格式: `{service_type}-{uuid_short}`
/// 例如: `session-a1b2c3d4`
fn generate_instance_id(service_type: &str, instance_id: Option<String>) -> String {
    instance_id.unwrap_or_else(|| format!("{}-{}", service_type, &Uuid::new_v4().to_string()[..8]))
}

/// 从全局配置自动初始化服务注册发现（服务注册 + 发现）
///
/// 使用 `app_config()` 获取全局配置并自动初始化
/// 这是最便捷的初始化方式，推荐在应用启动时调用
///
/// # 参数
/// * `service_type` - 服务类型（如 "session", "signaling-online"）
/// * `service_address` - 服务地址（SocketAddr）
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
///
/// # 返回
/// 返回 `(ServiceRegistry, ServiceDiscover, ServiceDiscoverUpdater)` 元组（如果配置了 registry），否则返回 None
///
/// # 示例
/// ```rust,ignore
/// use flare_im_core::discovery::init_from_app_config;
/// use std::net::SocketAddr;
///
/// // 在应用启动时调用
/// let address: SocketAddr = "127.0.0.1:8080".parse()?;
/// if let Some((registry, discover, updater)) = init_from_app_config("session", address, None).await? {
///     // registry 会自动处理心跳续期（每 30 秒）
///     // 当 registry 被 drop 时，会自动注销服务
///     // 使用 discover 进行服务发现
///     let _registry = registry;
/// }
/// ```
pub async fn init_from_app_config(
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<
    Option<(ServiceRegistry, ServiceDiscover, ServiceDiscoverUpdater)>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    // 获取全局配置
    use crate::config::app_config;
    let app_config = app_config();
    // 从配置初始化服务注册发现
    init_from_config(app_config, service_type, service_address, instance_id).await
}

/// 从应用配置自动初始化服务注册发现（服务注册 + 发现）
///
/// 如果配置中启用了 registry，会自动创建服务注册和发现实例
/// 如果未配置 registry，返回 None
///
/// # 参数
/// * `app_config` - 应用配置
/// * `service_type` - 服务类型（如 "session", "signaling-online"）
/// * `service_address` - 服务地址（SocketAddr）
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
///
/// # 返回
/// 返回 `(ServiceRegistry, ServiceDiscover, ServiceDiscoverUpdater)` 元组（如果配置了 registry），否则返回 None
pub async fn init_from_config(
    app_config: &FlareAppConfig,
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<
    Option<(ServiceRegistry, ServiceDiscover, ServiceDiscoverUpdater)>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    // 检查是否配置了注册中心
    if let Some(registry_config) = &app_config.core.registry {
        // 如果配置了注册中心，则初始化服务注册发现
        init_from_registry_config(registry_config, service_type, service_address, instance_id)
            .await
            .map(Some)
    } else {
        // 如果未配置注册中心，则返回 None
        Ok(None)
    }
}

/// 从注册中心配置初始化服务注册发现
///
/// # 参数
/// * `registry_config` - 注册中心配置
/// * `service_type` - 服务类型
/// * `service_address` - 服务地址
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
///
/// # 返回
/// 返回 `(ServiceRegistry, ServiceDiscover, ServiceDiscoverUpdater)` 元组
pub async fn init_from_registry_config(
    registry_config: &RegistryConfig,
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<
    (ServiceRegistry, ServiceDiscover, ServiceDiscoverUpdater),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // 解析注册中心类型
    let backend_type = parse_backend_type(&registry_config.registry_type)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::from(e) })?;

    // 生成实例 ID
    let instance_id = generate_instance_id(service_type, instance_id);

    // 创建服务实例
    let mut instance = ServiceInstance::new(service_type, instance_id, service_address);

    // 设置命名空间（如果配置了）
    if !registry_config.namespace.is_empty() {
        instance = instance.with_namespace(&registry_config.namespace);
    }

    // 使用 DiscoveryFactory::register_and_discover 创建服务注册和发现
    let (registry, discover, updater) = DiscoveryFactory::register_and_discover(
        backend_type,
        registry_config.endpoints.clone(),
        service_type.to_string(),
        instance,
    )
    .await
    .map_err(|e| format!("Failed to register and discover service: {}", e))?;

    tracing::info!(
        service_type = %service_type,
        registry_type = %registry_config.registry_type,
        "✅ Service registered and discovery initialized"
    );

    Ok((registry, discover, updater))
}

/// 只注册服务（不进行服务发现）
///
/// 用于在 gRPC 服务启动完成后再进行服务注册的场景
///
/// # 参数
/// * `service_type` - 服务类型（如 "session", "signaling-online"）
/// * `service_address` - 服务地址（SocketAddr）
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
///
/// # 返回
/// 返回 `ServiceRegistry`（如果配置了 registry），否则返回 None
///
/// # 示例
/// ```rust,ignore
/// use flare_im_core::discovery::register_service_only;
/// use std::net::SocketAddr;
///
/// // 在 gRPC 服务启动后调用
/// let address: SocketAddr = "127.0.0.1:8080".parse()?;
/// if let Some(registry) = register_service_only("session", address, None).await? {
///     // registry 会自动处理心跳续期（每 30 秒）
///     // 当 registry 被 drop 时，会自动注销服务
///     let _registry = registry;
/// }
/// ```
pub async fn register_service_only(
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<Option<ServiceRegistry>, Box<dyn std::error::Error + Send + Sync>> {
    register_service_only_with_metadata(service_type, service_address, instance_id, None).await
}

/// 仅注册服务并适配为 runtime 注册器
///
/// 用于 `flare_core_runtime::ServiceRuntime::run_with_registration`，避免各服务重复写
/// `register_service_only + adapt_registry` 样板代码。
pub async fn register_runtime_service_only(
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<
    Option<Box<dyn flare_core_runtime::registry::ServiceRegistry>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let registry = register_service_only(service_type, service_address, instance_id).await?;
    Ok(crate::discovery::adapt_registry(registry))
}

/// 注册服务（支持元数据）
///
/// # 参数
/// * `service_type` - 服务类型
/// * `service_address` - 服务地址
/// * `instance_id` - 可选的实例 ID
/// * `metadata` - 可选的元数据 HashMap（用于服务发现时的过滤）
///
/// # 返回
/// 返回 `ServiceRegistry`（如果配置了 registry），否则返回 None
pub async fn register_service_only_with_metadata(
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
    metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<Option<ServiceRegistry>, Box<dyn std::error::Error + Send + Sync>> {
    // 获取全局配置
    use crate::config::app_config;
    let app_config = app_config();
    // 从配置注册服务
    register_service_from_config_with_metadata(
        app_config,
        service_type,
        service_address,
        instance_id,
        metadata,
    )
    .await
}

/// 注册服务（带元数据）并适配为 runtime 注册器
pub async fn register_runtime_service_only_with_metadata(
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
    metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<
    Option<Box<dyn flare_core_runtime::registry::ServiceRegistry>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let registry =
        register_service_only_with_metadata(service_type, service_address, instance_id, metadata)
            .await?;
    Ok(crate::discovery::adapt_registry(registry))
}

/// 从应用配置只注册服务（不进行服务发现）
///
/// # 参数
/// * `app_config` - 应用配置
/// * `service_type` - 服务类型
/// * `service_address` - 服务地址
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
///
/// # 返回
/// 返回 `ServiceRegistry`（如果配置了 registry），否则返回 None
pub async fn register_service_from_config(
    app_config: &FlareAppConfig,
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<Option<ServiceRegistry>, Box<dyn std::error::Error + Send + Sync>> {
    register_service_from_config_with_metadata(
        app_config,
        service_type,
        service_address,
        instance_id,
        None,
    )
    .await
}

/// 从应用配置只注册服务（不进行服务发现，支持元数据）
///
/// # 参数
/// * `app_config` - 应用配置
/// * `service_type` - 服务类型
/// * `service_address` - 服务地址
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
/// * `metadata` - 可选的元数据 HashMap（用于服务发现时的过滤）
///
/// # 返回
/// 返回 `ServiceRegistry`（如果配置了 registry），否则返回 None
pub async fn register_service_from_config_with_metadata(
    app_config: &FlareAppConfig,
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
    metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<Option<ServiceRegistry>, Box<dyn std::error::Error + Send + Sync>> {
    // 如果配置了 registry，只注册服务
    if let Some(registry_config) = &app_config.core.registry {
        register_service_from_registry_config_with_metadata(
            registry_config,
            service_type,
            service_address,
            instance_id,
            metadata,
        )
        .await
        .map(Some)
    } else {
        Ok(None)
    }
}

/// 从注册中心配置只注册服务（不进行服务发现）
///
/// # 参数
/// * `registry_config` - 注册中心配置
/// * `service_type` - 服务类型
/// * `service_address` - 服务地址
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
///
/// # 返回
/// 返回 `ServiceRegistry`
pub async fn register_service_from_registry_config(
    registry_config: &RegistryConfig,
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
) -> Result<ServiceRegistry, Box<dyn std::error::Error + Send + Sync>> {
    register_service_from_registry_config_with_metadata(
        registry_config,
        service_type,
        service_address,
        instance_id,
        None,
    )
    .await
}

/// 从注册中心配置只注册服务（不进行服务发现，支持元数据）
///
/// # 参数
/// * `registry_config` - 注册中心配置
/// * `service_type` - 服务类型
/// * `service_address` - 服务地址
/// * `instance_id` - 可选的实例 ID（如果不提供，会自动生成）
/// * `metadata` - 可选的元数据 HashMap（key-value 对，会添加到 tags 和 metadata.custom 中）
///   常用的 key 包括：
///   - `svid`: 业务系统标识符（如 "svid.im"），用于服务发现过滤
///   - `server_id`: 服务器 ID，用于标识服务实例
///   - `shard_id`: 分片 ID（如果使用分片）
///   - `region`: 区域标识
///   - `az`: 可用区标识
///
/// # 返回
/// 返回 `ServiceRegistry`
pub async fn register_service_from_registry_config_with_metadata(
    registry_config: &RegistryConfig,
    service_type: &str,
    service_address: SocketAddr,
    instance_id: Option<String>,
    metadata: Option<std::collections::HashMap<String, String>>,
) -> Result<ServiceRegistry, Box<dyn std::error::Error + Send + Sync>> {
    let backend_type = parse_backend_type(&registry_config.registry_type)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::from(e) })?;

    // 创建后端配置
    use serde_json::json;
    use std::collections::HashMap;
    let mut backend_config = HashMap::new();
    match backend_type {
        BackendType::Etcd => {
            backend_config.insert("endpoints".to_string(), json!(registry_config.endpoints));
            backend_config.insert("service_type".to_string(), json!(service_type));
            backend_config.insert("ttl".to_string(), json!(90)); // TTL = 心跳间隔 * 3
        }
        BackendType::Consul => {
            backend_config.insert(
                "url".to_string(),
                json!(
                    registry_config
                        .endpoints
                        .first()
                        .unwrap_or(&"http://localhost:8500".to_string())
                ),
            );
            backend_config.insert("service_type".to_string(), json!(service_type));
        }
        _ => {
            return Err("DNS 和 Mesh 后端不支持服务注册".into());
        }
    }

    use crate::discovery::{DiscoveryConfig, HealthCheckConfig, LoadBalanceStrategy};
    let config = DiscoveryConfig {
        backend: backend_type,
        backend_config,
        namespace: None,
        version: None,
        tag_filters: vec![],
        load_balance: LoadBalanceStrategy::ConsistentHash,
        health_check: Some(HealthCheckConfig {
            interval: 10,
            timeout: 5,
            failure_threshold: 3,
            success_threshold: 2,
            path: Some("/health".to_string()),
        }),
        refresh_interval: Some(30),
    };

    // 创建后端
    let backend = DiscoveryFactory::create_backend(&config).await?;

    // 创建服务实例
    let instance_id = generate_instance_id(service_type, instance_id);

    let mut instance = ServiceInstance::new(service_type, instance_id, service_address);

    // 设置命名空间（如果配置了）
    if !registry_config.namespace.is_empty() {
        instance = instance.with_namespace(&registry_config.namespace);
    }

    // 添加元数据（如果提供了）
    if let Some(metadata) = metadata {
        for (key, value) in metadata {
            // 同时添加到 tags 和 metadata.custom
            // tags 用于 Consul/etcd 的标签过滤
            // metadata.custom 用于其他场景（如 Service Mesh）
            instance = instance.with_tag(key.clone(), value.clone());
            instance.metadata.custom.insert(key, value);
        }
    }

    // 注册服务实例
    backend
        .register(instance.clone())
        .await
        .map_err(|e| format!("Failed to register service: {}", e))?;

    // 记录注册时的 tags 和 metadata，用于调试
    let tags_info: Vec<String> = instance
        .tags
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();
    let metadata_info: Vec<String> = instance
        .metadata
        .custom
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();

    tracing::info!(
        service_type = %service_type,
        instance_id = %instance.instance_id,
        address = %instance.address,
        registry_type = %registry_config.registry_type,
        tags = ?tags_info,
        metadata = ?metadata_info,
        "✅ Service registered with tags and metadata"
    );

    // 创建服务注册器（自动处理心跳和注销）
    // 心跳机制通过 DiscoveryBackend::heartbeat() 统一处理，各后端自行实现
    // 心跳间隔可通过环境变量调整，默认 20 秒（平衡检测速度和开销）
    let heartbeat_interval = std::env::var("SERVICE_HEARTBEAT_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(20); // 默认 20 秒（平衡模式）

    let registry = ServiceRegistry::new(backend, instance, heartbeat_interval);

    Ok(registry)
}

/// 创建服务发现器（只用于服务发现，不进行服务注册）
///
/// 用于客户端调用其他服务的场景，只需要服务发现，不需要注册自己
///
/// # 参数
/// * `service_type` - 要发现的服务类型（如 "signaling-online", "storage-reader"）
///
/// # 返回
/// 返回 `ServiceDiscover`（如果配置了 registry），否则返回 None
///
/// # 示例
/// ```rust,ignore
/// use flare_im_core::discovery::create_discover;
/// use flare_im_core::ServiceClient;
///
/// // 创建服务发现器
/// if let Some(discover) = create_discover("signaling-online").await? {
///     // 使用 ServiceClient 进行服务发现
///     let mut client = ServiceClient::new(discover);
///     let channel = client.get_channel().await?;
///     // 创建 gRPC 客户端
///     // let mut grpc_client = SignalingServiceClient::new(channel);
/// }
/// ```
pub async fn create_discover(
    service_type: &str,
) -> Result<Option<ServiceDiscover>, Box<dyn std::error::Error + Send + Sync>> {
    use crate::config::app_config;
    let app_config = app_config();
    create_discover_from_config(app_config, service_type).await
}

/// 从应用配置创建服务发现器（只用于服务发现，不进行服务注册）
///
/// # 参数
/// * `app_config` - 应用配置
/// * `service_type` - 要发现的服务类型
///
/// # 返回
/// 返回 `ServiceDiscover`（如果配置了 registry），否则返回 None
pub async fn create_discover_from_config(
    app_config: &FlareAppConfig,
    service_type: &str,
) -> Result<Option<ServiceDiscover>, Box<dyn std::error::Error + Send + Sync>> {
    // 如果配置了 registry，创建服务发现器
    if let Some(registry_config) = &app_config.core.registry {
        create_discover_from_registry_config(registry_config, service_type)
            .await
            .map(Some)
    } else {
        Ok(None)
    }
}

/// 从注册中心配置创建服务发现器（只用于服务发现，不进行服务注册）
///
/// # 参数
/// * `registry_config` - 注册中心配置
/// * `service_type` - 要发现的服务类型
///
/// # 返回
/// 返回 `ServiceDiscover`
pub async fn create_discover_from_registry_config(
    registry_config: &RegistryConfig,
    service_type: &str,
) -> Result<ServiceDiscover, Box<dyn std::error::Error + Send + Sync>> {
    create_discover_from_registry_config_with_filters(registry_config, service_type, None).await
}

/// 从注册中心配置创建服务发现器（支持标签过滤）
///
/// # 参数
/// * `registry_config` - 注册中心配置
/// * `service_type` - 要发现的服务类型
/// * `tag_filters` - 可选的标签过滤器 HashMap（用于根据标签过滤服务实例）
///   例如：`{"svid": "svid.im"}` 只会返回 svid=svid.im 的服务实例
///
/// # 返回
/// 返回 `ServiceDiscover`
pub async fn create_discover_from_registry_config_with_filters(
    registry_config: &RegistryConfig,
    service_type: &str,
    tag_filters: Option<std::collections::HashMap<String, String>>,
) -> Result<ServiceDiscover, Box<dyn std::error::Error + Send + Sync>> {
    // 将 RegistryConfig 转换为 BackendType
    let backend_type = match registry_config.registry_type.to_lowercase().as_str() {
        "etcd" => BackendType::Etcd,
        "consul" => BackendType::Consul,
        "dns" => BackendType::Dns,
        "mesh" => BackendType::Mesh,
        _ => {
            return Err(format!(
                "Unsupported registry type: {}",
                registry_config.registry_type
            )
            .into());
        }
    };

    // 如果有标签过滤器，使用 DiscoveryConfig 创建服务发现器
    if let Some(filters) = tag_filters {
        use serde_json::json;
        use std::collections::HashMap;
        let mut backend_config = HashMap::new();
        match backend_type {
            BackendType::Etcd => {
                backend_config.insert("endpoints".to_string(), json!(registry_config.endpoints));
                backend_config.insert("service_type".to_string(), json!(service_type));
            }
            BackendType::Consul => {
                backend_config.insert(
                    "url".to_string(),
                    json!(
                        registry_config
                            .endpoints
                            .first()
                            .unwrap_or(&"http://localhost:8500".to_string())
                    ),
                );
                backend_config.insert("service_type".to_string(), json!(service_type));
            }
            _ => {
                // DNS 和 Mesh 后端不支持标签过滤，使用默认创建方式
                let (discover, _updater) = DiscoveryFactory::create_with_defaults(
                    backend_type,
                    registry_config.endpoints.clone(),
                    service_type.to_string(),
                )
                .await
                .map_err(|e| format!("Failed to create service discover: {}", e))?;

                tracing::debug!(
                    service_type = %service_type,
                    registry_type = %registry_config.registry_type,
                    "✅ Service discover created (without tag filters, backend doesn't support it)"
                );

                return Ok(discover);
            }
        }

        use crate::discovery::{DiscoveryConfig, LoadBalanceStrategy};
        // 将 HashMap 转换为 TagFilter 向量
        let tag_filters: Vec<TagFilter> = filters
            .into_iter()
            .map(|(key, value)| TagFilter {
                key,
                value: Some(value),
                pattern: Some("exact".to_string()),
            })
            .collect();

        let config = DiscoveryConfig {
            backend: backend_type,
            backend_config,
            namespace: None,
            version: None,
            tag_filters,
            load_balance: LoadBalanceStrategy::ConsistentHash,
            health_check: None,
            refresh_interval: Some(30),
        };

        let (discover, _updater) = DiscoveryFactory::create_discover(config)
            .await
            .map_err(|e| format!("Failed to create service discover with filters: {}", e))?;

        tracing::debug!(
            service_type = %service_type,
            registry_type = %registry_config.registry_type,
            "✅ Service discover created with tag filters"
        );

        Ok(discover)
    } else {
        // 没有标签过滤器，使用默认创建方式
        let (discover, _updater) = DiscoveryFactory::create_with_defaults(
            backend_type,
            registry_config.endpoints.clone(),
            service_type.to_string(),
        )
        .await
        .map_err(|e| format!("Failed to create service discover: {}", e))?;

        tracing::debug!(
            service_type = %service_type,
            registry_type = %registry_config.registry_type,
            "✅ Service discover created"
        );

        Ok(discover)
    }
}
