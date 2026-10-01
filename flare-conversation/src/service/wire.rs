//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use sqlx::postgres::PgPoolOptions;

use crate::application::ConversationAdminQueryHandler;
use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};
use crate::config::ConversationConfig;
use crate::domain::model::ConversationDomainConfig;
use crate::domain::service::{ConversationDomainService, DefaultConversationDomainService};
use crate::infrastructure::event_consumer::{
    ConversationEnsureEventConsumer, ReadReceiptEventConsumer,
};
use crate::infrastructure::persistence::PostgresConversationAdminRepository;
use crate::infrastructure::persistence::message_cache::RedisMessageCacheInvalidator;
use crate::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use crate::infrastructure::rpc::StorageReaderClient;
use crate::interface::grpc::{ConversationAdminGrpcHandler, ConversationGrpcHandler};

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub handler: ConversationGrpcHandler,
    /// 管理员只读（`ConversationAdminService`）：只认 admin-gateway 转发的 admin 令牌。
    pub admin_handler: ConversationAdminGrpcHandler<PostgresConversationAdminRepository>,
    /// 配置了 JetStream 且初始化成功时存在，由 bootstrap 后台 `run`（`ctx` 在 MQ 侧重建）
    pub read_receipt_consumer: Option<ReadReceiptEventConsumer>,
    pub conversation_ensure_consumer: Option<ConversationEnsureEventConsumer>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// 注意：由于 Rust 2024 原生 async fn 不支持 dyn 兼容性，
/// 我们使用泛型 + 具体类型的方式，在编译期确定实现
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    // 1. 加载会话配置
    let conversation_config = Arc::new(ConversationConfig::from_app_config(app_config).map_err(
        |e| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "Failed to load conversation service configuration",
            )
            .details(e.to_string())
            .build_error()
        },
    )?);

    // 2. 创建 Redis 客户端
    let redis_client = Arc::new(
        redis::Client::open(conversation_config.redis_url.clone()).map_err(|e| {
            ErrorBuilder::new(ErrorCode::NetworkError, "Failed to create Redis client")
                .details(e.to_string())
                .build_error()
        })?,
    );

    // 3. 创建 PostgreSQL 连接池（可选）
    let postgres_pool = if let Some(ref postgres_url) = conversation_config.postgres_url {
        let max_connections = conversation_config.postgres_max_connections.max(1);
        let min_connections = conversation_config
            .postgres_min_connections
            .min(max_connections);
        Arc::new(
            PgPoolOptions::new()
                .max_connections(max_connections)
                .min_connections(min_connections)
                .acquire_timeout(Duration::from_secs(
                    conversation_config.postgres_acquire_timeout_seconds.max(1),
                ))
                .idle_timeout(Duration::from_secs(
                    conversation_config.postgres_idle_timeout_seconds.max(1),
                ))
                .max_lifetime(Duration::from_secs(
                    conversation_config.postgres_max_lifetime_seconds.max(1),
                ))
                .connect(postgres_url)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(ErrorCode::DatabaseError, "Failed to connect to PostgreSQL")
                        .details(e.to_string())
                        .build_error()
                })?,
        )
    } else {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "postgres config is required for conversation service",
        )
        .build_error());
    };

    // 管理员只读仓储与会话仓储共用连接池，不另开连接。
    let admin_repository = Arc::new(PostgresConversationAdminRepository::new(Arc::clone(
        &postgres_pool,
    )));

    // 4. 创建会话仓储（硬切到 Postgres，会话元数据必须来自持久化读模型）
    let mut conversation_repo =
        crate::infrastructure::persistence::PostgresConversationRepository::new(
            postgres_pool,
            conversation_config.clone(),
        );
    // storage-reader 读消息热缓存时，清除历史要在同一个 Redis 上失效被删的消息。
    if let Some(cache_url) = RedisMessageCacheInvalidator::storage_reader_redis_url(app_config) {
        let cache_client = redis::Client::open(cache_url).map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::NetworkError,
                "Failed to create storage cache Redis client",
            )
            .details(e.to_string())
            .build_error()
        })?;
        conversation_repo = conversation_repo.with_message_cache_invalidator(Arc::new(
            RedisMessageCacheInvalidator::new(Arc::new(cache_client)),
        ));
    }
    let conversation_repo = Arc::new(conversation_repo);

    // 5. 创建在线状态仓储
    let presence_repo: Arc<RedisPresenceRepository> = Arc::new(RedisPresenceRepository::new(
        redis_client.clone(),
        conversation_config.clone(),
    ));

    // 6. 创建消息提供者（可选）
    let message_provider: Option<Arc<StorageReaderClient>> = {
        use flare_im_contracts::service_names::{STORAGE_READER, get_service_name};
        let storage_reader_service = get_service_name(STORAGE_READER);
        let explicit_url = explicit_storage_reader_url(
            std::env::var("CONVERSATION_STORAGE_READER_SERVICE_URL").ok(),
        )?;
        let discover_name = storage_reader_service.clone();
        let provider =
            build_message_provider(explicit_url, storage_reader_service, async move || {
                flare_im_service_kit::discovery::create_discover(&discover_name)
                    .await
                    .map_err(|e| {
                        ErrorBuilder::new(
                            ErrorCode::NetworkError,
                            format!(
                                "Failed to create storage reader service discover for {}: {}",
                                discover_name, e
                            ),
                        )
                        .build_error()
                    })
            })
            .await?;

        Some(Arc::new(provider))
    };

    // 7. 构建领域配置
    let domain_config = ConversationDomainConfig::new(conversation_config.recent_message_limit);

    // 8. 构建领域服务（使用泛型参数以获得更好的性能）
    let domain_service: Arc<DefaultConversationDomainService> =
        Arc::new(ConversationDomainService::new(
            conversation_repo.clone(),
            presence_repo.clone(),
            message_provider.clone(),
            domain_config,
        ));

    // 8. 构建命令处理器
    let command_handler = Arc::new(ConversationCommandHandler::new(domain_service.clone()));

    // 9. 构建查询处理器
    let query_handler = Arc::new(ConversationQueryHandler::new(
        conversation_repo,
        message_provider,
        domain_service.clone(),
    ));

    // 10. 构建 gRPC 处理器
    let grpc_handler = ConversationGrpcHandler::new(command_handler, query_handler);
    // 管理员只读只认 Console 公钥验签的 admin 令牌；没配公钥时 fail closed（UNAUTHENTICATED）。
    let control_verifier = flare_im_service_kit::control_auth::ConsoleControlVerifier::from_env()
        .map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::ConfigurationError,
            "invalid FLARE_CONSOLE_PUBLIC_KEY for conversation",
        )
        .build_error()
    })?;
    if control_verifier.is_none() {
        tracing::warn!("Console control public key missing; conversation admin RPCs fail closed");
    }
    let admin_handler = ConversationAdminGrpcHandler::new(
        Arc::new(ConversationAdminQueryHandler::new(admin_repository)),
        control_verifier,
    );

    let mq_consumers_enabled =
        conversation_config.mq_backend == "kafka" || conversation_config.jetstream_url.is_some();
    let read_receipt_consumer = if mq_consumers_enabled {
        match ReadReceiptEventConsumer::new(conversation_config.as_ref(), domain_service.clone())
            .await
        {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "ReadReceipt JetStream consumer init failed, skipping");
                None
            }
        }
    } else {
        None
    };

    let conversation_ensure_consumer = if mq_consumers_enabled {
        match ConversationEnsureEventConsumer::new(
            conversation_config.as_ref(),
            domain_service.clone(),
        )
        .await
        {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "ConversationEnsure JetStream consumer init failed, skipping");
                None
            }
        }
    } else {
        None
    };

    Ok(ApplicationContext {
        handler: grpc_handler,
        admin_handler,
        read_receipt_consumer,
        conversation_ensure_consumer,
    })
}

/// `CONVERSATION_STORAGE_READER_SERVICE_URL`：Storage Reader 的显式地址。设了就直连、不做服务发现，
/// 单容器 static 注册模式由 `release/supervise-core.toml` 写成容器内的监听地址。只收
/// `http://`、`https://`；空值等于没设。
fn explicit_storage_reader_url(raw: Option<String>) -> Result<Option<String>> {
    let Some(url) = raw.map(|value| value.trim().to_string()) else {
        return Ok(None);
    };
    if url.is_empty() {
        return Ok(None);
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        Ok(Some(url))
    } else {
        Err(ErrorBuilder::new(
            ErrorCode::ConfigurationError,
            "CONVERSATION_STORAGE_READER_SERVICE_URL must be an http:// or https:// URL",
        )
        .build_error())
    }
}

/// 消息提供者：有显式地址就直连（不调用 `discover`，也不打印回退告警）；否则按服务发现，
/// 发现未配置时才告警并改走环境变量回退。
async fn build_message_provider<D>(
    explicit_url: Option<String>,
    service_name: String,
    discover: D,
) -> Result<StorageReaderClient>
where
    D: AsyncFnOnce() -> Result<Option<flare_im_service_kit::ServiceDiscover>>,
{
    if let Some(url) = explicit_url {
        tracing::info!(url = %url, "Storage Reader at its explicit address; service discovery not used");
        return StorageReaderClient::with_static_endpoint(&url);
    }
    Ok(match discover().await? {
        Some(discover) => StorageReaderClient::with_service_client(
            flare_server_core::discovery::ServiceClient::new(discover),
        ),
        None => {
            // Fallback: construct provider with service name; provider will try env direct connect
            tracing::warn!("Storage Reader service discovery not configured, using env fallback");
            StorageReaderClient::new(service_name)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    const FALLBACK_WARNING: &str = "Storage Reader service discovery not configured";

    /// 收集本线程上的日志输出。
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Write for Captured {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    fn capture() -> (Captured, tracing::subscriber::DefaultGuard) {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        (captured, tracing::subscriber::set_default(subscriber))
    }

    #[test]
    fn explicit_url_accepts_http_schemes_and_treats_blank_as_unset() {
        assert_eq!(explicit_storage_reader_url(None).unwrap(), None);
        assert_eq!(
            explicit_storage_reader_url(Some("  ".into())).unwrap(),
            None
        );
        assert_eq!(
            explicit_storage_reader_url(Some(" http://127.0.0.1:60083 ".into())).unwrap(),
            Some("http://127.0.0.1:60083".to_string())
        );
        assert!(
            explicit_storage_reader_url(Some("https://reader.internal:443".into()))
                .unwrap()
                .is_some()
        );
        assert!(explicit_storage_reader_url(Some("127.0.0.1:60083".into())).is_err());
    }

    #[tokio::test]
    async fn an_explicit_url_skips_discovery_and_the_fallback_warning() {
        let (captured, _guard) = capture();
        let provider = build_message_provider(
            Some("http://127.0.0.1:60083".into()),
            "storage-reader".into(),
            async || -> Result<Option<flare_im_service_kit::ServiceDiscover>> {
                panic!("discovery must not run when the address is explicit")
            },
        )
        .await
        .unwrap();
        // The lazy channel answers without discovery or a connection attempt.
        assert!(provider.client().await.is_ok());
        let logs = captured.text();
        assert!(!logs.contains(FALLBACK_WARNING), "{logs}");
        assert!(
            logs.contains("Storage Reader at its explicit address"),
            "{logs}"
        );
    }

    #[tokio::test]
    async fn without_an_address_or_discovery_the_fallback_is_still_reported() {
        let (captured, _guard) = capture();
        build_message_provider(None, "storage-reader".into(), async || Ok(None))
            .await
            .unwrap();
        let logs = captured.text();
        assert!(logs.contains(FALLBACK_WARNING), "{logs}");
    }
}
