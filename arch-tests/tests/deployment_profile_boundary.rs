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
        "flare-im-all must expose a thin operator-facing binary"
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
