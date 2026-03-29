/// 定义网关常量
pub const SYSTEM_COMMAND_TYPE_CONNECT: i32 = 1;
pub const SYSTEM_COMMAND_TYPE_PING: i32 = 2;

// --- 认证相关（auth_handler 使用）---
/// 用户元数据 key：用户 ID
pub const METADATA_KEY_USER_ID: &str = "user_id";
/// 用户元数据 key：租户 ID
pub const METADATA_KEY_TENANT_ID: &str = "tenant_id";
/// 用户元数据 key：设备 ID
pub const METADATA_KEY_DEVICE_ID: &str = "device_id";
/// 默认租户 ID 的环境变量名
pub const ENV_DEFAULT_TENANT_ID: &str = "ACCESS_GATEWAY_DEFAULT_TENANT_ID";
/// 默认租户 ID（未配置时的回退值）
pub const DEFAULT_TENANT_ID: &str = "0";
/// Token 验证失败时的提示文案
pub const AUTH_FAILURE_MSG_TOKEN_INVALID: &str = "Token 无效或已过期";

/// Router `RouteMessage` / `RouteEvent` / `RouteAck` / `RouteData` 默认 SVID（与 `router.proto` 约定一致）
pub const DEFAULT_ROUTE_SVID: &str = "svid.im";
