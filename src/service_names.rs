//! 微服务服务名定义模块
//!
//! 所有服务注册和发现必须使用此模块中定义的常量，确保一致性。
//! 这是微服务架构中的单一数据源（Single Source of Truth）。
//!
//! ## 使用方式
//!
//! ```rust,ignore
//! use flare_im_core::service_names::*;
//!
//! // 服务注册
//! register_service_only(CONVERSATION, addr, None).await?;
//!
//! // 服务发现
//! let discover = create_discover(CONVERSATION).await?;
//!
//! // 配置加载时使用常量作为默认值
//! let conversation_service = env::var("CONVERSATION_SERVICE")
//!     .unwrap_or_else(|_| CONVERSATION.to_string());
//! ```
//!
//! ## 命名规范
//!
//! 服务名命名规范：
//! - 格式: `flare-{service-name}`
//! - 使用小写字母和连字符
//! - 服务名必须与代码仓库中的服务目录名对应
//! - 注册时使用的服务名必须与发现时使用的服务名完全一致
//!
//! ## 环境变量覆盖
//!
//! 虽然建议使用常量，但为了支持不同环境部署，可以通过环境变量覆盖：
//! - 格式: `{SERVICE_NAME}_SERVICE` 或 `{SERVICE_NAME}_SERVICE_NAME`
//! - 例如: `CONVERSATION_SERVICE=flare-conversation-dev` （开发环境）
//! - 例如: `CONVERSATION_SERVICE=flare-conversation-prod` （生产环境）
//!
//! **注意**：即使使用环境变量覆盖，服务名也必须遵循命名规范，且注册和发现必须使用相同的服务名。

/// Flare IM 微服务服务名定义
#[allow(clippy::module_inception)]
pub mod service_names {
    /// Conversation 服务名
    ///
    /// 用于会话管理、参与者查询等功能
    /// 注册路径: `flare/flare-conversation`
    pub const CONVERSATION: &str = "flare-conversation";

    /// Signaling Online 服务名
    ///
    /// 用于在线状态管理、用户登录等功能
    /// 注册路径: `flare/flare-signaling-online`
    pub const SIGNALING_ONLINE: &str = "flare-signaling-online";

    /// Signaling Route 服务名
    ///
    /// 用于消息路由、服务路由等功能
    /// 注册路径: `flare/flare-signaling-route`
    pub const SIGNALING_ROUTE: &str = "flare-signaling-route";

    /// Push Server 服务名
    ///
    /// 用于消息推送、在线推送等功能
    /// 注册路径: `flare/flare-push-server`
    pub const PUSH_SERVER: &str = "flare-push-server";

    /// Push Worker 服务名
    ///
    /// 用于推送任务处理、离线推送等功能
    /// 注册路径: `flare/flare-push-worker`
    pub const PUSH_WORKER: &str = "flare-push-worker";

    /// Push Proxy 服务名
    ///
    /// 暴露 PushService gRPC，将推送/ACK 请求写入 Kafka，由 Push Server 消费
    /// 注册路径: `flare/flare-push-proxy`
    pub const PUSH_PROXY: &str = "flare-push-proxy";

    /// Access Gateway 服务名
    ///
    /// 用于客户端接入、长连接管理等功能
    /// 注册路径: `flare/flare-signaling-gateway`
    pub const ACCESS_GATEWAY: &str = "flare-signaling-gateway";

    /// Core Gateway 服务名
    ///
    /// 用于统一网关、API 网关等功能
    /// 注册路径: `flare/flare-core-gateway`
    pub const CORE_GATEWAY: &str = "flare-core-gateway";

    /// Orchestrator 服务名（消息编排，后续可扩展推送入队/ACK）
    ///
    /// 注册路径: `flare/flare-orchestrator`
    pub const ORCHESTRATOR: &str = "flare-orchestrator";

    /// Message Orchestrator 服务名（与 ORCHESTRATOR 同体，别名供路由等使用）
    pub const MESSAGE_ORCHESTRATOR: &str = "flare-orchestrator";

    /// Storage Writer 服务名
    ///
    /// 用于消息存储、消息持久化等功能
    /// 注册路径: `flare/flare-storage-writer`
    pub const STORAGE_WRITER: &str = "flare-storage-writer";

    /// Storage Reader 服务名
    ///
    /// 用于消息查询、历史消息等功能
    /// 注册路径: `flare/flare-storage-reader`
    pub const STORAGE_READER: &str = "flare-storage-reader";

    /// Sync Orchestrator 服务名
    ///
    /// 用于统一会话/消息同步编排能力
    pub const SYNC_ORCHESTRATOR: &str = "flare-sync-orchestrator";

    /// Media 服务名
    ///
    /// 用于媒体文件管理、文件上传下载等功能
    /// 注册路径: `flare/flare-media`
    pub const MEDIA: &str = "flare-media";

    /// Capability 服务名（Hook 扩展 + 业务能力插件）
    ///
    /// 对应工作区目录 `flare-capability/`，注册与发现必须使用此字符串，与包名、二进制名一致。
    /// 注册路径: `flare/flare-capability`
    pub const CAPABILITY: &str = "flare-capability";

    /// 与 [`CAPABILITY`] 同义，命名便于与业务侧 `FLARE_CAPABILITY_*` 环境变量对齐
    pub const FLARE_CAPABILITY: &str = CAPABILITY;
}

/// 服务名验证函数
///
/// 验证服务名是否有效（是否在常量定义中）
pub fn validate_service_name(name: &str) -> bool {
    matches!(
        name,
        service_names::CONVERSATION
            | service_names::SIGNALING_ONLINE
            | service_names::SIGNALING_ROUTE
            | service_names::PUSH_SERVER
            | service_names::PUSH_WORKER
            | service_names::PUSH_PROXY
            | service_names::ACCESS_GATEWAY
            | service_names::CORE_GATEWAY
            | service_names::ORCHESTRATOR
            | service_names::STORAGE_WRITER
            | service_names::STORAGE_READER
            | service_names::SYNC_ORCHESTRATOR
            | service_names::MEDIA
            | service_names::CAPABILITY
    )
}

/// 获取服务名的环境变量名称
///
/// 用于从环境变量读取服务名（支持覆盖）
pub fn service_name_env_var(service_name: &str) -> String {
    // 将 "flare-conversation" 转换为 "CONVERSATION_SERVICE"
    let upper = service_name.to_uppercase().replace("FLARE-", "");
    format!("{}_SERVICE", upper.replace("-", "_"))
}

/// 从环境变量或常量获取服务名
///
/// 优先使用环境变量，如果没有则使用常量默认值
pub fn get_service_name(constant_name: &str) -> String {
    std::env::var(service_name_env_var(constant_name)).unwrap_or_else(|_| constant_name.to_string())
}

// 重新导出，方便使用
pub use service_names::*;
