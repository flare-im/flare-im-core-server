use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn message_send_path_is_owned_by_message_ingest_not_orchestrator() {
    let root = workspace_root();

    let route_handler = read(
        &root,
        "flare-signaling/route/src/application/handlers/message_routing_handler.rs",
    );
    assert!(
        route_handler.contains("Message Ingest")
            && route_handler.contains("MessageSendService.SendMessage"),
        "Route message handler must document Message Ingest as the send-command destination"
    );
    assert!(
        !route_handler.contains("Orchestrator SendMessage"),
        "Route message handler must not describe message sends as Orchestrator SendMessage traffic"
    );

    let forwarder = read(
        &root,
        "flare-signaling/route/src/infrastructure/forwarder.rs",
    );
    assert!(
        forwarder.contains("MESSAGE_INGEST") && forwarder.contains("forward_message"),
        "Route forwarder must resolve svid.im message sends to the Message Ingest service"
    );

    let ingest_lib = read(&root, "flare-message-ingest/src/lib.rs");
    assert!(
        ingest_lib.contains("Message Ingest")
            && ingest_lib.contains("MessageSendService.SendMessage")
            && ingest_lib.contains("flare.im.message.main"),
        "Message Ingest crate docs must own the message-send path into the main message stream"
    );
    assert!(
        ingest_lib.contains("事件/操作上行不经本服务"),
        "Message Ingest crate docs must explicitly exclude operation-event traffic"
    );

    let orchestrator_lib = read(&root, "flare-orchestrator/src/lib.rs");
    assert!(
        orchestrator_lib.contains("MessageEventService.ExecuteEvent")
            && orchestrator_lib.contains("MessageActionService")
            && orchestrator_lib.contains("消费 `flare.im.message.main` 做 storage/push fanout"),
        "Orchestrator crate docs must own operation events and main-stream fanout"
    );
    assert!(
        orchestrator_lib.contains("消息发送上行不经本服务"),
        "Orchestrator crate docs must explicitly exclude message-send ingress"
    );
}

#[test]
fn conversation_ensure_lives_in_message_ingest_boundary() {
    let root = workspace_root();

    let ingest_ensure = read(
        &root,
        "flare-message-ingest/src/domain/service/conversation_ensure_service.rs",
    );
    assert!(
        ingest_ensure.contains("MessageIngestHandler")
            && ingest_ensure.contains("ensure_conversation_sync")
            && ingest_ensure.contains("ensure_conversation_async"),
        "Message Ingest must own sync/async conversation ensure for sends"
    );
    assert!(
        !ingest_ensure.contains("build_conversation_ensure_request_from_event")
            && !ingest_ensure.contains("被消息和事件处理共同使用"),
        "Message Ingest conversation ensure must not pretend to own event ingress"
    );

    let orchestrator_repository_mod =
        read(&root, "flare-orchestrator/src/domain/repository/mod.rs");
    assert!(
        !orchestrator_repository_mod.contains("conversation_repository")
            && !orchestrator_repository_mod.contains("ConversationRepository"),
        "Orchestrator must not export a stale conversation-ensure repository port"
    );
    assert!(
        !root
            .join("flare-orchestrator/src/domain/repository/conversation_repository.rs")
            .exists(),
        "Orchestrator must not keep a stale conversation-ensure repository module"
    );
}

#[test]
fn session_creation_docs_have_single_authoritative_home() {
    let root = workspace_root();

    let ingest_doc = read(
        &root,
        "flare-message-ingest/docs/SESSION_CREATION_DESIGN.md",
    );
    assert!(
        ingest_doc
            .contains("Client -> Gateway -> Route -> Message Ingest -> flare.im.message.main")
            && ingest_doc.contains("Orchestrator may consume `flare.im.message.main`")
            && ingest_doc.contains("must not own message-send validation"),
        "Message Ingest session creation doc must describe the current send-path boundary"
    );
    assert!(
        !ingest_doc.contains("orchestrate_message_storage")
            && !ingest_doc.contains("Orchestrator 在编排存储流程"),
        "Message Ingest session creation doc must not carry stale Orchestrator-era wording"
    );
    assert!(
        !root
            .join("flare-orchestrator/docs/SESSION_CREATION_DESIGN.md")
            .exists(),
        "Session creation design must not be duplicated under Orchestrator"
    );
}

#[test]
fn workspace_root_does_not_contain_uncompiled_placeholder_tests() {
    let root = workspace_root();
    assert!(
        !root.join("tests").exists(),
        "flare-im-core is a virtual workspace; root tests/ is not compiled by Cargo and must not hold placeholder coverage"
    );
}

fn read(root: &Path, relative: &str) -> String {
    fs::read_to_string(root.join(relative)).unwrap_or_else(|err| {
        panic!("read {relative}: {err}");
    })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
