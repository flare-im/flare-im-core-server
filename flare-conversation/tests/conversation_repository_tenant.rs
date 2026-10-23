//! 会话仓储的租户来源（W11-T）。
//!
//! `PostgresConversationRepository` 的 14 个读写方法以前在上下文没有租户时按租户 `"0"` 读写；
//! 单聊会话 ID 不含租户，这会读写租户 0 的同 ID 会话。现在缺租户即 `InvalidParameter`，在发出
//! 任何 SQL 之前返回——这里的连接池是 `connect_lazy` 指向一个不监听的回环端口，真要查库就会
//! 得到连接错误而不是参数错误。显式租户 "0" 的读写由各真库用例（如
//! `conversation_event_tenant_pg.rs`）覆盖。

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, Conversation, ConversationDomainConfig, ConversationFilter,
    ConversationLifecycleState, ConversationParticipant, ConversationPolicy, ConversationSort,
    ConversationType, ConversationVisibility, HistoryPurgeBoundary,
    UpdateConversationUserSettingsPatch,
};
use flare_conversation::domain::repository::ConversationRepository;
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
use flare_conversation::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorCode, FlareError};
use sqlx::postgres::PgPoolOptions;

fn test_config() -> Arc<ConversationConfig> {
    Arc::new(ConversationConfig {
        mq_backend: "nats".into(),
        redis_url: "redis://127.0.0.1:9/".into(),
        postgres_url: None,
        postgres_max_connections: 1,
        postgres_min_connections: 0,
        postgres_acquire_timeout_seconds: 1,
        postgres_idle_timeout_seconds: 60,
        postgres_max_lifetime_seconds: 600,
        conversation_state_prefix: "test:state".into(),
        conversation_unread_prefix: "test:unread".into(),
        user_cursor_prefix: "test:cursor".into(),
        presence_prefix: "test:presence".into(),
        large_conversation_precise_unread_threshold: 500,
        storage_reader_service: None,
        recent_message_limit: 20,
        default_policy: ConversationPolicy {
            conflict_resolution: ConflictResolutionPolicy::Coexist,
            max_devices: 5,
            allow_anonymous: false,
            allow_history_sync: true,
            metadata: Default::default(),
        },
        jetstream_url: None,
        jetstream_operation_subject: None,
        jetstream_events_subject: None,
        jetstream_group: None,
        jetstream_stream_specs: vec![],
        kafka_brokers: vec![],
        kafka_client_id: "test".into(),
        jetstream_ensure_subject: None,
        jetstream_ensure_group: None,
    })
}

fn lazy_pool() -> sqlx::PgPool {
    // 端口 1 不监听：任何真正发出的查询都会是连接错误，不会是参数错误。
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(1))
        .connect_lazy("postgres://flare@127.0.0.1:1/none")
        .expect("lazy pool")
}

fn repository() -> PostgresConversationRepository {
    PostgresConversationRepository::new(Arc::new(lazy_pool()), test_config())
}

fn tenantless_contexts() -> [Context; 3] {
    [
        Context::with_request_id("req").with_user_id("alice"),
        Context::with_request_id("req")
            .with_user_id("alice")
            .with_tenant_id(""),
        Context::with_request_id("req")
            .with_user_id("alice")
            .with_tenant_id("  "),
    ]
}

const CID: &str = "1AW11T0000000000AB";

fn conversation() -> Conversation {
    Conversation {
        tenant_id: String::new(),
        conversation_id: CID.to_string(),
        conversation_type: ConversationType::Single,
        business_type: "im".to_string(),
        channel_id: String::new(),
        display_name: None,
        attributes: HashMap::new(),
        participants: Vec::new(),
        visibility: ConversationVisibility::Private,
        lifecycle_state: ConversationLifecycleState::Active,
        policy: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn participant() -> ConversationParticipant {
    ConversationParticipant {
        user_id: "bob".to_string(),
        roles: Vec::new(),
        muted: false,
        pinned: false,
        attributes: HashMap::new(),
        visible_from_seq: 0,
        mention_only: false,
    }
}

fn assert_tenant_required<T: std::fmt::Debug>(method: &str, result: Result<T, FlareError>) {
    let error = result.expect_err(method);
    assert_eq!(
        error.code(),
        Some(ErrorCode::InvalidParameter),
        "{method}: {error}"
    );
    assert!(
        error.reason().contains("tenant required"),
        "{method}: {}",
        error.reason()
    );
}

#[tokio::test]
async fn every_repository_method_requires_a_context_tenant() {
    let repo = repository();
    for ctx in [
        Context::with_request_id("req").with_user_id("alice"),
        Context::with_request_id("req")
            .with_user_id("alice")
            .with_tenant_id(""),
        Context::with_request_id("req")
            .with_user_id("alice")
            .with_tenant_id("  "),
    ] {
        assert_tenant_required(
            "load_bootstrap",
            repo.load_bootstrap(&ctx, &HashMap::new(), 0).await,
        );
        assert_tenant_required("update_cursor", repo.update_cursor(&ctx, CID, 1).await);
        assert_tenant_required(
            "create_conversation",
            repo.create_conversation(&ctx, &conversation()).await,
        );
        assert_tenant_required("get_conversation", repo.get_conversation(&ctx, CID).await);
        assert_tenant_required(
            "update_conversation",
            repo.update_conversation(&ctx, &conversation()).await,
        );
        assert_tenant_required(
            "delete_conversation",
            repo.delete_conversation(&ctx, CID, false).await,
        );
        assert_tenant_required(
            "purge_history",
            repo.purge_history(
                &ctx,
                CID,
                &HistoryPurgeBoundary {
                    before: Some(Utc::now()),
                    epoch_fence: None,
                },
            )
            .await,
        );
        assert_tenant_required(
            "manage_participants",
            repo.manage_participants(&ctx, CID, &[participant()], &[], &[])
                .await,
        );
        assert_tenant_required(
            "list_conversation_participants",
            repo.list_conversation_participants(
                &ctx,
                CID,
                None,
                10,
                false,
                false,
                Default::default(),
            )
            .await,
        );
        assert_tenant_required(
            "find_conversation_members",
            repo.find_conversation_members(&ctx, CID, &["bob".to_string()])
                .await,
        );
        assert_tenant_required(
            "search_conversations",
            repo.search_conversations(
                &ctx,
                &[ConversationFilter {
                    conversation_type: None,
                    business_type: None,
                    lifecycle_state: None,
                    visibility: None,
                    participant_user_id: Some("alice".to_string()),
                }],
                &[ConversationSort {
                    field: "updated_at".to_string(),
                    ascending: false,
                }],
                10,
                0,
            )
            .await,
        );
        assert_tenant_required("mark_as_read", repo.mark_as_read(&ctx, CID, 1).await);
        assert_tenant_required(
            "update_user_settings",
            repo.update_user_settings(
                &ctx,
                &UpdateConversationUserSettingsPatch {
                    conversation_id: CID.to_string(),
                    is_mention_only: None,
                    is_pinned: Some(true),
                    is_muted: None,
                    is_archived: None,
                    draft: None,
                    base_settings_version: 0,
                },
            )
            .await,
        );
        assert_tenant_required(
            "apply_message_event",
            repo.apply_message_event(&ctx, CID, "alice", 1, 0).await,
        );
        assert_tenant_required("get_unread_count", repo.get_unread_count(&ctx, CID).await);
    }
}

/// 对照：有租户（含显式的 "0"）时方法越过租户检查去查库，这里得到的是连接错误。
#[tokio::test]
async fn a_present_tenant_passes_the_check_and_reaches_the_database() {
    let repo = repository();
    for tenant in ["tenant-a", "0"] {
        let ctx = Context::with_request_id("req")
            .with_user_id("alice")
            .with_tenant_id(tenant);
        let error = repo
            .get_unread_count(&ctx, CID)
            .await
            .expect_err("no database behind the lazy pool");
        assert!(
            !error.reason().contains("tenant required"),
            "{tenant}: {}",
            error.reason()
        );
    }
}

/// 领域服务建会话只认上下文租户（W12-T）：缺租户在读写仓储之前就以 `InvalidParameter` 拒绝，
/// 不再生成租户为 "0" 的会话、不再用 "0" 作系统会话的 system_id。以前缺租户时建出 `tenant_id`
/// 为 "0" 的会话，只靠仓储的租户检查（"tenant required for conversation storage"）挡住。
#[tokio::test]
async fn domain_service_requires_a_context_tenant_to_create_a_conversation() {
    let presence = RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").expect("redis url")),
        test_config(),
    );
    let service = DefaultConversationDomainService::new(
        Arc::new(repository()),
        Arc::new(presence),
        None,
        ConversationDomainConfig::default(),
    );
    let requested = HashMap::from([("conversation_id".to_string(), CID.to_string())]);
    let cases = [
        (ConversationType::System, HashMap::new()),
        (ConversationType::Group, HashMap::new()),
        (ConversationType::Single, requested),
    ];
    for ctx in tenantless_contexts() {
        for (conversation_type, attributes) in cases.clone() {
            let error = service
                .create_conversation(
                    &ctx,
                    conversation_type,
                    "im".to_string(),
                    vec![participant()],
                    attributes,
                    ConversationVisibility::Private,
                    String::new(),
                )
                .await
                .expect_err("no tenant, no conversation");
            assert_eq!(
                error.code(),
                Some(ErrorCode::InvalidParameter),
                "{conversation_type:?}: {error}"
            );
            assert!(
                error
                    .reason()
                    .contains("tenant required to create a conversation"),
                "{conversation_type:?}: {}",
                error.reason()
            );
        }
    }
}
