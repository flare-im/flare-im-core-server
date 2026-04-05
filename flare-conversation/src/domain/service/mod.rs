pub mod conversation_domain_service;
pub mod thread_domain_service;

pub use conversation_domain_service::ConversationDomainService;
pub use thread_domain_service::ThreadDomainService;

/// 默认的会话领域服务类型（使用 Postgres 会话仓储 + Redis 在线状态）
///
/// 这是一个类型别名，使用具体的 Redis 实现类型
/// 由于 Rust 2024 原生 async fn 不支持 dyn 兼容性，我们使用泛型 + 具体类型的方式
/// 在 wire.rs 中根据配置选择不同的具体实现
///
/// 性能优势：
/// - 零开销的静态分发
/// - 编译时类型检查
/// - 更好的内联优化
pub type DefaultConversationDomainService = ConversationDomainService<
    crate::infrastructure::persistence::postgres_repository::PostgresConversationRepository,
    crate::infrastructure::persistence::redis_presence::RedisPresenceRepository,
    crate::infrastructure::rpc::StorageReaderClient,
>;

/// 默认的话题领域服务类型（使用 Postgres 实现）
pub type DefaultThreadDomainService = ThreadDomainService<
    crate::infrastructure::persistence::thread_repository::PostgresThreadRepository,
>;
