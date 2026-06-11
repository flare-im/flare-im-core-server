use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("service-kit crate should live under crates/flare-im-service-kit")
        .to_path_buf()
}

fn script(path: &str) -> String {
    fs::read_to_string(repo_root().join(path)).expect("script should be readable")
}

fn between<'a>(content: &'a str, start: &str, end: &str) -> &'a str {
    content
        .split_once(start)
        .and_then(|(_, tail)| tail.split_once(end))
        .map(|(block, _)| block)
        .expect("script block should exist")
}

#[test]
fn startup_scripts_guard_message_ingest_as_required_send_entrypoint() {
    let start_server = script("scripts/start_server.sh");
    let check_services = script("scripts/check_services.sh");
    let required_bins = between(&start_server, "REQUIRED_CORE_BINARIES=(", ")");
    let core_services = between(&start_server, "CORE_SERVICES=(", ")");
    let service_checks = between(&check_services, "SERVICES=(", ")");

    assert!(
        required_bins.contains("flare-message-ingest"),
        "start_server.sh must require the flare-message-ingest binary before allowing skipped builds"
    );
    assert!(
        core_services.contains("\"message-ingest\""),
        "start_server.sh must start message-ingest as a core service"
    );
    assert!(
        service_checks.contains("\"message-ingest:50182\""),
        "check_services.sh must verify the message-ingest gRPC listener on 50182"
    );
}

#[test]
fn smoke_message_flow_script_exercises_send_ack_and_durable_storage() {
    let smoke = script("scripts/smoke_message_flow.sh");

    for required in [
        "flare.message.v1.MessageSendService/SendMessage",
        "SEND_ACK_DURABILITY_BROKER_ACCEPTED",
        "message_write_ledger",
        "ack_published",
        "messages",
        "SMOKE_MESSAGE_INGEST_ENDPOINT",
        "SMOKE_STORAGE_READER_ENDPOINT",
        "SMOKE_POSTGRES_URL",
        "flare.storage.v1.StorageReaderService/QueryMessagesBySeq",
        "flare_message_flow_smoke_report",
        "storage_reader_messages_count",
    ] {
        assert!(
            smoke.contains(required),
            "smoke_message_flow.sh must include {required:?} in the executable contract"
        );
    }

    assert!(
        smoke.contains("* 1000"),
        "smoke_message_flow.sh must send createdAt in milliseconds, not Unix seconds"
    );
}
