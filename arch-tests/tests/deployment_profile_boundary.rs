use std::fs;
use std::path::PathBuf;

#[test]
fn aggregate_profile_crate_is_a_workspace_member() {
    let root = workspace_root();
    let workspace = fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest");

    assert!(
        workspace.contains("\"flare-im-all\""),
        "flare-im-all must stay in the workspace so profile drift is compiled and tested"
    );
    assert!(
        root.join("flare-im-all/src/lib.rs").is_file(),
        "flare-im-all must expose the profile contract as a library"
    );
    assert!(
        root.join("flare-im-all/src/main.rs").is_file(),
        "flare-im-all must expose an operator-facing binary"
    );
}

#[test]
fn aggregate_profile_models_dev_standard_and_full_shapes() {
    let source =
        fs::read_to_string(workspace_root().join("flare-im-all/src/lib.rs")).expect("read profile");

    for required in [
        "pub enum DeploymentProfile",
        "Dev",
        "Standard",
        "Full",
        "pub enum StandardGroup",
        "Edge",
        "Core",
        "Data",
        "EmbeddedSingleProcess",
        "EmbeddedServiceGroup",
        "IndependentServiceProcess",
        "pub const ALL_RUNTIME_SERVICES: [ServiceSpec; 16]",
    ] {
        assert!(
            source.contains(required),
            "flare-im-all profile contract must contain {required}"
        );
    }
}

#[test]
fn aggregate_profile_covers_all_runtime_services_once() {
    let source =
        fs::read_to_string(workspace_root().join("flare-im-all/src/lib.rs")).expect("read profile");

    for service in [
        "API_GATEWAY",
        "ADMIN_GATEWAY",
        "ACCESS_GATEWAY",
        "SIGNALING_ROUTE",
        "MESSAGE_INGEST",
        "ORCHESTRATOR",
        "CONVERSATION",
        "SYNC_ORCHESTRATOR",
        "PUSH_PROXY",
        "PUSH_SERVER",
        "PUSH_WORKER",
        "CAPABILITY",
        "MEDIA",
        "STORAGE_WRITER",
        "STORAGE_READER",
        "SIGNALING_ONLINE",
    ] {
        let count = source.matches(service).count();
        assert!(
            count >= 2,
            "{service} must be imported and included in ALL_RUNTIME_SERVICES"
        );
    }
}

#[test]
fn aggregate_profile_has_embedded_runner_contract() {
    let root = workspace_root();
    let lib = fs::read_to_string(root.join("flare-im-all/src/lib.rs")).expect("read profile lib");
    let embedded =
        fs::read_to_string(root.join("flare-im-all/src/embedded.rs")).expect("read embedded");
    let main = fs::read_to_string(root.join("flare-im-all/src/main.rs")).expect("read main");

    for required in [
        "pub mod embedded;",
        "pub const ALL_RUNTIME_SERVICES: [ServiceSpec; 16]",
    ] {
        assert!(
            lib.contains(required),
            "flare-im-all lib must expose embedded profile contract: {required}"
        );
    }

    for required in [
        "pub const EMBEDDED_SERVICE_RUNNERS: [EmbeddedServiceRunner; 16]",
        "LocalSet",
        "ChannelSignal",
        "run_embedded_dev",
        "run_embedded_standard_group",
    ] {
        assert!(
            embedded.contains(required),
            "flare-im-all embedded runner must contain {required}"
        );
    }

    for required in [
        "flare-im-all run dev",
        "flare-im-all run standard <edge|core|data>",
    ] {
        assert!(
            main.contains(required),
            "flare-im-all CLI help must advertise {required}"
        );
    }
}

#[test]
fn runtime_services_expose_embedded_shutdown_entrypoints() {
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
    ] {
        let source = fs::read_to_string(root.join(path)).expect("read service bootstrap");
        assert!(
            source.contains("run_with_shutdown_signals"),
            "{path} must expose run_with_shutdown_signals for flare-im-all"
        );
    }

    let capability = fs::read_to_string(root.join("flare-capability/src/composition/bootstrap.rs"))
        .expect("read capability bootstrap");
    assert!(
        capability.contains("run_from_env_with_shutdown_signals"),
        "capability must expose env-based embedded shutdown entrypoint"
    );
}

#[test]
fn embedded_ready_services_do_not_hardwire_process_registration_signals() {
    let root = workspace_root();

    for path in [
        "flare-api-gateway/src/service.rs",
        "flare-admin-gateway/src/service.rs",
        "flare-signaling/gateway/src/service/startup.rs",
        "flare-signaling/online/src/service/bootstrap.rs",
        "flare-signaling/route/src/service/bootstrap.rs",
        "flare-storage/reader/src/service/mod.rs",
        "flare-conversation/src/service/mod.rs",
        "flare-sync-orchestrator/src/service/mod.rs",
        "flare-media/src/service/bootstrap.rs",
        "flare-push/proxy/src/service/bootstrap.rs",
        "flare-message-ingest/src/bootstrap.rs",
        "flare-orchestrator/src/bootstrap.rs",
        "flare-capability/src/composition/bootstrap.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).expect("read registered bootstrap");
        assert!(
            source.contains("run_with_registration_and_signals"),
            "{path} must pass embedded shutdown signals into registered runtime"
        );
        assert!(
            !source.contains(".run_with_registration("),
            "{path} must not hardwire process-level registration shutdown signals"
        );
    }

    for path in [
        "flare-push/server/src/service/bootstrap.rs",
        "flare-push/worker/src/service/bootstrap.rs",
        "flare-storage/writer/src/service/mod.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).expect("read background bootstrap");
        assert!(
            source.contains("run_with_signals"),
            "{path} must pass embedded shutdown signals into background runtime"
        );
        assert!(
            !source.contains(".run()"),
            "{path} must not hardwire process-level background shutdown signals"
        );
    }
}

#[test]
fn gateway_and_capability_bootstraps_are_library_entrypoints() {
    let root = workspace_root();

    for (lib_path, required) in [
        (
            "flare-api-gateway/src/lib.rs",
            "pub use service::ApplicationBootstrap;",
        ),
        (
            "flare-admin-gateway/src/lib.rs",
            "pub use service::ApplicationBootstrap;",
        ),
    ] {
        let source = fs::read_to_string(root.join(lib_path)).expect("read gateway lib");
        assert!(
            source.contains(required),
            "{lib_path} must expose ApplicationBootstrap for aggregate profile wiring"
        );
    }

    let capability = fs::read_to_string(root.join("flare-capability/src/composition/bootstrap.rs"))
        .expect("read capability bootstrap");
    assert!(
        capability.contains("pub async fn run_from_env()"),
        "capability must expose an env-based bootstrap for aggregate profile wiring"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("arch-tests lives under workspace root")
        .to_path_buf()
}
