# Flare IM Core 服务发现模块

## 概述

本模块提供统一的服务发现和注册功能，基于 `flare-server-core` 的通用服务发现接口，从配置文件中自动读取配置并构建服务注册和发现实例。

## 特性

- ✅ **配置驱动**: 从 `base.toml` 配置文件自动读取服务发现配置
- ✅ **自动初始化**: 一键完成服务注册和发现初始化
- ✅ **多后端支持**: etcd、consul、DNS、Service Mesh
- ✅ **自动心跳**: 服务注册器自动处理心跳续期
- ✅ **优雅关闭**: 服务停止时自动注销
- ✅ **Tower 兼容**: 完全兼容 tower 生态系统

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│             业务服务层 (flare-conversation, etc.)              │
│             使用 init_from_app_config 初始化              │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│         flare-im-core::discovery (业务层封装)              │
│  ┌────────────────────────────────────────────────────┐  │
│  │  从 base.toml 读取配置                               │  │
│  │  调用 DiscoveryFactory::register_and_discover      │  │
│  └────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│         flare-server-core::discovery (通用层)             │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │  Etcd    │  Consul  │   DNS    │   Mesh   │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
└─────────────────────────────────────────────────────────┘
```

## 配置方式

在 `base.toml` 或服务配置文件中添加 `registry` 配置：

```toml
[registry]
# 注册中心类型：etcd, consul, mesh
registry_type = "consul"
# 注册中心端点列表
endpoints = ["http://localhost:28500"]
# 命名空间
namespace = "flare"
# TTL（秒）
ttl = 30
# 负载均衡策略：round_robin, random, consistent_hash, least_connections
load_balance_strategy = "consistent_hash"
```

## 使用方式

### 1. 快速初始化（推荐）

在服务启动时，使用 `init_from_app_config` 一键完成服务注册和发现初始化：

```rust
use flare_im_core::discovery::init_from_app_config;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析服务地址
    let address: SocketAddr = "127.0.0.1:8080".parse()?;

    // 从配置自动初始化服务注册和发现
    if let Some((registry, discover, updater)) = init_from_app_config(
        "conversation",  // 服务类型
        address,    // 服务地址
        None,       // 实例 ID（可选，不提供则自动生成）
    ).await? {
        // registry 会自动处理心跳续期（每 30 秒）
        // 当 registry 被 drop 时，会自动注销服务
        
        // 使用 discover 进行服务发现
        use flare_im_core::ServiceClient;
        let mut client = ServiceClient::new(discover);
        let channel = client.get_channel().await?;

        // 创建 gRPC 客户端并调用
        // let mut grpc_client = YourServiceClient::new(channel);
        // let response = grpc_client.your_method(request).await?;

        // 保持 registry 存活直到服务关闭
        // 在实际应用中，应该将 registry 保存到服务结构体中
        let _registry = registry;
    } else {
        // 未配置服务发现
        tracing::info!("Service discovery not configured");
    }
    
    // 启动服务...
    
    Ok(())
}
```

### 2. 在服务结构体中使用

```rust
use std::net::SocketAddr;
use flare_im_core::discovery::init_from_app_config;
use flare_im_core::{ServiceDiscover, ServiceDiscoverUpdater, ServiceRegistry};

pub struct ConversationServiceApp {
    handler: ConversationGrpcHandler,
    address: SocketAddr,
    /// 服务注册器（如果启用了服务发现）
    _registry: Option<ServiceRegistry>,
    /// 服务发现器（用于发现其他服务）
    _discover: Option<ServiceDiscover>,
}

impl ConversationServiceApp {
    pub async fn new() -> Result<Self> {
        // ... 其他初始化代码 ...
        
        let address: SocketAddr = /* 解析地址 */;
        
        // 初始化服务注册和发现
        let (registry, discover, _updater) = if let Some((r, d, u)) = init_from_app_config(
            "conversation",
            address,
            None,
        ).await? {
            (Some(r), Some(d), Some(u))
        } else {
            (None, None, None)
        };
        
        Ok(Self {
            handler,
            address,
            _registry: registry,
            _discover: discover,
        })
    }
    
    pub async fn run(&self) -> Result<()> {
        // 启动服务...
        // ServiceRegistry 会在 drop 时自动注销服务
        Ok(())
    }
}
```

### 3. 手动初始化（使用 RegistryConfig）

如果需要更细粒度的控制，可以手动传入 `RegistryConfig`：

```rust
use flare_im_core::discovery::init_from_registry_config;
use flare_im_core::RegistryConfig;
use std::net::SocketAddr;

let registry_config = RegistryConfig {
    registry_type: "consul".to_string(),
    endpoints: vec!["http://localhost:8500".to_string()],
    namespace: "flare".to_string(),
    ttl: 30,
    load_balance_strategy: "consistent_hash".to_string(),
};

let address: SocketAddr = "127.0.0.1:8080".parse()?;

let (registry, discover, updater) = init_from_registry_config(
    &registry_config,
    "conversation",
    address,
    Some("custom-instance-id".to_string()),
).await?;

// 使用 registry 和 discover...
```

## 默认配置

使用 `init_from_app_config` 或 `init_from_registry_config` 时，会自动使用以下最优默认配置：

- ✅ **心跳间隔**: 30 秒（平衡网络开销和故障检测速度）
- ✅ **TTL**: 90 秒（心跳间隔的 3 倍，确保网络抖动时不会误判）
- ✅ **刷新间隔**: 30 秒（与服务发现同步）
- ✅ **健康检查**: 启用，间隔 10 秒，超时 5 秒
- ✅ **负载均衡**: 一致性哈希（适合大多数场景）
- ✅ **失败阈值**: 3 次
- ✅ **成功阈值**: 2 次

## 服务发现使用

初始化后，可以使用 `ServiceClient` 进行服务发现：

```rust
use flare_im_core::ServiceClient;

// 假设已经初始化了 discover
let mut client = ServiceClient::new(discover);

// 获取 Channel（自动负载均衡和缓存）
let channel = client.get_channel().await?;

// 创建 gRPC 客户端并调用
let mut grpc_client = YourServiceClient::new(channel);
let response = grpc_client.your_method(request).await?;
```

## 生命周期管理

- **服务启动**: 调用 `init_from_app_config` 自动注册服务
- **运行期间**: `ServiceRegistry` 自动发送心跳（每 30 秒）
- **服务停止**: `ServiceRegistry` 被 drop 时自动注销服务

## 最佳实践

1. **生产环境**: 使用 etcd 或 consul，启用健康检查和版本控制
2. **开发环境**: 可以不配置 registry，返回 None 即可
3. **服务注册**: 在服务启动时调用 `init_from_app_config`
4. **服务发现**: 使用 `ServiceClient` 进行服务发现和调用
5. **生命周期**: 将 `ServiceRegistry` 保存到服务结构体中，确保服务停止时自动注销

## 注意事项

- DNS 后端不支持服务注册（只读）
- Service Mesh 模式下，服务注册由 sidecar 处理
- 一致性哈希需要提供 key（如 user_id）才能生效
- 健康检查需要服务提供 `/health` 端点
- 配置中的 `ttl` 字段会被自动调整为 90 秒（心跳间隔的 3 倍）

## 相关文档

- [flare-server-core 服务发现文档](../../flare-server-core/src/discovery/README.md)
- [配置文档](../config/README.md)
