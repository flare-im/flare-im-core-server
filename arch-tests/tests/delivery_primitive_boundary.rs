use std::fs;
use std::path::PathBuf;

#[test]
fn event_delivery_primitive_uses_typed_watermark_fields() {
    let root = workspace_root();
    let repo_root = root.parent().expect("flare-im repo root");

    let event_proto = fs::read_to_string(repo_root.join("flare-proto/proto/event.proto"))
        .expect("read event proto");
    let access_gateway_proto =
        fs::read_to_string(repo_root.join("flare-grpc-proto/proto/access_gateway.proto"))
            .expect("read access gateway proto");

    for required in [
        "EventEnvelopeDeliveryMode delivery_mode = 6",
        "string conversation_id = 7",
        "uint64 min_conversation_seq = 8",
        "bool inline_events_truncated = 9",
        "EVENT_ENVELOPE_DELIVERY_MODE_PING = 2",
        "EVENT_ENVELOPE_DELIVERY_MODE_PING_WITH_INLINE = 3",
    ] {
        assert!(
            event_proto.contains(required),
            "EventEnvelope must retain typed delivery primitive field `{required}`"
        );
    }

    for required in [
        "string conversation_id = 4",
        "uint64 max_conversation_seq = 5",
        "flare.common.v1.EventEnvelopeDeliveryMode delivery_mode = 6",
        "bool inline_events_truncated = 7",
    ] {
        assert!(
            access_gateway_proto.contains(required),
            "PushEventRequest must retain typed ping field `{required}`"
        );
    }
}

#[test]
fn large_conversation_fanout_uses_ping_without_inline_message_payload() {
    let root = workspace_root();

    let fanout = fs::read_to_string(
        root.join("flare-orchestrator/src/domain/service/message_fanout_service.rs"),
    )
    .expect("read message fanout service");
    assert!(
        fanout.contains("large_conversation")
            && fanout.contains("push_only_message_ping")
            && fanout.contains("inline_message_push_enabled")
            && fanout.contains("Vec::new()"),
        "large conversation fanout must use the typed envelope flag and keep the notify+pull ping branch without materialized push recipients"
    );

    let push_repository = fs::read_to_string(
        root.join("crates/flare-im-message-pipeline/src/messaging/push_repository.rs"),
    )
    .expect("read push repository");
    assert!(
        push_repository.contains("fn message_ping_event")
            && push_repository.contains("Self::message_event(message, false)")
            && push_repository.contains("HEADER_DELIVERY_MODE")
            && push_repository.contains("DELIVERY_MODE_PING"),
        "message ping events must be typed pings and must not carry inline message payload"
    );
    assert!(
        push_repository.contains("fn message_inline_event")
            && push_repository.contains("Self::message_event(message, true)")
            && push_repository.contains("Payload::Message")
            && push_repository.contains("DELIVERY_MODE_PING_WITH_INLINE"),
        "small conversation message fanout must use the unified inline event primitive"
    );

    let event_consumer = fs::read_to_string(
        root.join("flare-push/server/src/interface/messaging/event_consumer.rs"),
    )
    .expect("read push event consumer");
    assert!(
        event_consumer.contains("EventEnvelopeDeliveryMode::Ping")
            && event_consumer.contains("events: vec![]"),
        "push event consumer must translate ping MqEnvelope records into pure PushEventRequest pings"
    );

    let push_router = fs::read_to_string(
        root.join("flare-push/server/src/application/handlers/push_router_handler.rs"),
    )
    .expect("read push router handler");
    assert!(
        push_router.contains("ConversationOnlineIndexReader")
            && push_router.contains("handle_conversation_ping_without_recipients")
            && push_router.contains("publish_event_to_online_index_batch")
            && push_router.contains("conversation_online_index"),
        "push server must resolve recipient-less pings through the per-conversation online index"
    );
    assert!(
        !push_router.contains("ConversationParticipantResolver"),
        "push server must not restore full participant-page scans for recipient-less pings"
    );
}

#[test]
fn large_conversation_unread_updates_are_threshold_guarded() {
    let root = workspace_root();

    let config = fs::read_to_string(root.join("flare-conversation/src/config.rs"))
        .expect("read conversation config");
    assert!(
        config.contains("large_conversation_precise_unread_threshold")
            && config.contains("CONVERSATION_LARGE_CONVERSATION_PRECISE_UNREAD_THRESHOLD"),
        "conversation unread approximation threshold must stay configurable"
    );

    let postgres_repository = fs::read_to_string(
        root.join("flare-conversation/src/infrastructure/persistence/postgres_repository.rs"),
    )
    .expect("read conversation postgres repository");
    assert!(
        postgres_repository.contains("member_stats")
            && postgres_repository.contains("large_conversation_precise_unread_threshold")
            && postgres_repository.contains("member_stats.member_count <= $6"),
        "message-event unread write diffusion must remain guarded by member-count threshold"
    );
}

#[test]
fn push_server_can_read_online_status_from_redis_without_grpc_roundtrip() {
    let root = workspace_root();

    let online_status = fs::read_to_string(
        root.join("flare-push/server/src/infrastructure/online/online_status_service.rs"),
    )
    .expect("read push server online status service");
    assert!(
        online_status.contains("OnlineStatusBackend")
            && online_status.contains("Redis(ConnectionManager)")
            && online_status.contains("session:{user_id}")
            && online_status.contains("grpc_online_statuses"),
        "push server online filtering must retain redis direct-read backend and grpc fallback"
    );
}

#[test]
fn dlq_replay_cli_stays_in_workspace() {
    let root = workspace_root();

    let workspace = fs::read_to_string(root.join("Cargo.toml")).expect("read workspace Cargo.toml");
    assert!(
        workspace.contains("\"tools/flare-dlq-replay\""),
        "DLQ replay CLI must stay compiled as a workspace member"
    );

    let cli_manifest = fs::read_to_string(root.join("tools/flare-dlq-replay/Cargo.toml"))
        .expect("read DLQ replay manifest");
    assert!(
        cli_manifest.contains("name = \"flare-dlq-replay\"")
            && cli_manifest.contains("features = [\"kafka\", \"nats\", \"proto\"]"),
        "DLQ replay CLI must retain Kafka/NATS producer support"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
