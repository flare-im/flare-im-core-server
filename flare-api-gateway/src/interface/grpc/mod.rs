//! gRPC 接口层。
//!
//! `flare-api-gateway` 默认面向三方暴露 HTTP/OpenAPI。需要高性能可信接入时，
//! 这里应实现版本化 gRPC facade，例如 `flare.api_gateway.v1.ApiGatewayPublicService`
//! 和 `ApiGatewayAdminService`。
//!
//! 设计约束：
//! - 不直接暴露内部所有 gRPC service。
//! - 不实现 token 签发、刷新、撤销或会话存储。
//! - 不绕过 orchestrator、storage reader/writer、capability 等下游边界。
//! - Admin facade 必须依赖下沉认证 provider、mTLS/service token、allowlist 和审计。
