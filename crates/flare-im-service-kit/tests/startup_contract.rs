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
fn smoke_message_flow_script_sends_through_edge_and_checks_durable_storage() {
    let smoke = script("scripts/smoke_message_flow.sh");

    for required in [
        "/api/v1/auth/register",
        "/api/v1/auth/login",
        "/api/v1/relations/friend-requests",
        "/api/v1/messages/send",
        "/api/v1/conversations",
        "tokens.im_connect_token",
        "SMOKE_BASE_URL",
        "SMOKE_CA_FILE",
        "SMOKE_SERVER_NAME",
        "SMOKE_PG_CONTAINER",
        "BEGIN TRANSACTION READ ONLY",
        "default_transaction_read_only=on",
        "message_write_ledger",
        "archive_persisted",
        "ack_published",
        "flare_message_flow_smoke_report",
    ] {
        assert!(
            smoke.contains(required),
            "smoke_message_flow.sh must include {required:?} in the executable contract"
        );
    }

    // Message Ingest only accepts identities signed inside the Cell; the smoke
    // must never fall back to a direct gRPC call with unsigned identity headers.
    let lowered = smoke.to_ascii_lowercase();
    for forbidden in ["x-tenant-id:", "x-user-id:", "x-flare-ctx:"] {
        assert!(
            !lowered.contains(forbidden),
            "smoke_message_flow.sh must not send the {forbidden:?} header"
        );
    }
    for forbidden in ["grpcurl ", "\"$grpcurl", "SMOKE_TENANT_ID:-0"] {
        assert!(
            !smoke.contains(forbidden),
            "smoke_message_flow.sh must not contain {forbidden:?}"
        );
    }
}
