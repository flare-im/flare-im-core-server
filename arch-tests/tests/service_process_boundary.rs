use std::fs;
use std::path::PathBuf;

#[test]
fn microservice_workspace_does_not_reintroduce_aggregate_runner() {
    let root = workspace_root();
    let workspace = fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest");

    assert!(
        !workspace.contains("\"flare-im-all\""),
        "core workspace should stay microservice-first; use scripts/start_server.sh instead of an aggregate runner crate"
    );
    assert!(
        !root.join("flare-im-all").exists(),
        "flare-im-all should not live in flare-im-core because it couples all services back into one crate"
    );
}

#[test]
fn service_binaries_keep_own_process_lifecycle() {
    let root = workspace_root();

    for path in [
        "flare-api-gateway/src/service.rs",
        "flare-admin-gateway/src/service.rs",
        "flare-signaling/gateway/src/service/bootstrap.rs",
        "flare-signaling/online/src/service/bootstrap.rs",
        "flare-signaling/route/src/service/bootstrap.rs",
        "flare-storage/reader/src/service/mod.rs",
        "flare-storage/writer/src/service/mod.rs",
        "flare-conversation/src/service/mod.rs",
        "flare-sync-orchestrator/src/service/mod.rs",
        "flare-media/src/service/bootstrap.rs",
        "flare-push/proxy/src/service/bootstrap.rs",
        "flare-push/server/src/service/bootstrap.rs",
        "flare-push/worker/src/service/bootstrap.rs",
        "flare-message-ingest/src/bootstrap.rs",
        "flare-orchestrator/src/bootstrap.rs",
        "flare-capability/src/composition/bootstrap.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).expect("read service bootstrap");
        assert!(
            !source.contains("run_with_shutdown_signals"),
            "{path} should not expose aggregate-runner lifecycle hooks"
        );
        assert!(
            !source.contains("run_with_registration_and_signals"),
            "{path} should not depend on aggregate-runner registration lifecycle"
        );
        assert!(
            !source.contains("RuntimeShutdownSignals"),
            "{path} should keep process lifecycle local to its service binary"
        );
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("arch-tests lives under workspace root")
        .to_path_buf()
}
