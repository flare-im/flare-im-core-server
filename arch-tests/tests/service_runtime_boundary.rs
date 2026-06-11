use std::fs;
use std::path::PathBuf;

#[test]
fn sync_orchestrator_uses_shared_service_runtime_scaffold() {
    assert!(
        workspace_root()
            .join("crates/flare-im-service-kit/src/runtime.rs")
            .is_file(),
        "service-kit must own the shared runtime assembly scaffold"
    );

    assert_bootstrap_uses_runtime_scaffold(
        "flare-sync-orchestrator/src/service/mod.rs",
        &[
            "load_app_config_from_env",
            "sync_orchestrator_service",
            "build_service_runtime_plan",
            ".service_runtime()",
        ],
        &[
            "std::env::var(\"SYNC_ORCHESTRATOR_HOST\")",
            "std::env::var(\"SYNC_ORCHESTRATOR_PORT\")",
            "ServiceRuntime::new",
            "attach_runtime_health_checks",
        ],
    );
}

#[test]
fn message_pipeline_services_use_shared_service_runtime_scaffold() {
    for (path, service_config_method) in [
        (
            "flare-message-ingest/src/bootstrap.rs",
            "message_ingest_service",
        ),
        (
            "flare-orchestrator/src/bootstrap.rs",
            "orchestrator_service",
        ),
    ] {
        assert_bootstrap_uses_runtime_scaffold(
            path,
            &[
                "load_app_config_from_env",
                service_config_method,
                "build_service_runtime_plan",
                ".service_runtime()",
            ],
            &[
                "load_config(Some(\"config\"))",
                "ServiceHelper::parse_server_addr",
                "ServiceRuntime::new",
                "attach_runtime_health_checks",
            ],
        );
    }
}

#[test]
fn push_background_services_use_shared_runtime_scaffold() {
    for (path, service_config_method) in [
        (
            "flare-push/server/src/service/bootstrap.rs",
            "push_server_service",
        ),
        (
            "flare-push/worker/src/service/bootstrap.rs",
            "push_worker_service",
        ),
        (
            "flare-storage/writer/src/service/mod.rs",
            "storage_writer_service",
        ),
    ] {
        assert_bootstrap_uses_runtime_scaffold(
            path,
            &[
                "load_app_config_from_env",
                service_config_method,
                "build_background_service_runtime",
                "background_service_runtime",
            ],
            &[
                "load_config(Some(\"./config\"))",
                "ServiceRuntime::mq_consumer",
                "with_health_failure_action",
                "attach_runtime_health_checks",
            ],
        );
    }
}

#[test]
fn grpc_services_use_shared_service_runtime_plan() {
    for (path, service_config_method) in [
        (
            "flare-signaling/online/src/service/bootstrap.rs",
            "signaling_online_service",
        ),
        (
            "flare-signaling/route/src/service/bootstrap.rs",
            "signaling_route_service",
        ),
        ("flare-media/src/service/bootstrap.rs", "media_service"),
        (
            "flare-push/proxy/src/service/bootstrap.rs",
            "push_proxy_service",
        ),
        (
            "flare-storage/reader/src/service/mod.rs",
            "storage_reader_service",
        ),
        (
            "flare-conversation/src/service/mod.rs",
            "conversation_service",
        ),
        (
            "flare-capability/src/composition/bootstrap.rs",
            "capability_service",
        ),
    ] {
        assert_bootstrap_uses_runtime_scaffold(
            path,
            &[
                "load_app_config_from_env",
                service_config_method,
                "build_service_runtime_plan",
                ".service_runtime()",
            ],
            &[
                "load_config(Some(",
                "ServiceHelper::parse_server_addr",
                "ServiceRuntime::new",
                "with_health_failure_action",
                "attach_runtime_health_checks",
                "std::env::var(\"PUSH_PROXY_LISTEN\")",
            ],
        );
    }
}

#[test]
fn access_gateway_uses_shared_runtime_scaffold_for_registration_shell() {
    assert_bootstrap_uses_runtime_scaffold(
        "flare-signaling/gateway/src/service/bootstrap.rs",
        &["resolve_config_path", "load_app_config_from_env"],
        &["load_config(Some("],
    );

    assert_bootstrap_uses_runtime_scaffold(
        "flare-signaling/gateway/src/service/startup.rs",
        &[
            "ImServiceRuntimePlan",
            ".service_runtime()",
            "ACCESS_GATEWAY",
        ],
        &[
            "ServiceRuntime::new",
            "with_health_failure_action",
            "attach_runtime_health_checks",
            "\"access-gateway\"",
        ],
    );
}

fn assert_bootstrap_uses_runtime_scaffold(
    service_path: &str,
    required: &[&str],
    forbidden: &[&str],
) {
    let root = workspace_root();
    let service_bootstrap =
        fs::read_to_string(root.join(service_path)).expect("read service bootstrap");
    for required in required {
        assert!(
            service_bootstrap.contains(required),
            "{service_path} must use shared runtime scaffold: {required}"
        );
    }

    for forbidden in forbidden {
        assert!(
            !service_bootstrap.contains(forbidden),
            "{service_path} must not hand-roll runtime assembly: {forbidden}"
        );
    }
}

#[test]
fn sync_orchestrator_has_typed_service_config() {
    let root = workspace_root();
    let config_model =
        fs::read_to_string(root.join("crates/flare-im-service-kit/src/config/mod.rs"))
            .expect("read service-kit config model");
    let config_file = root.join("config/services/sync-orchestrator.toml");

    assert!(
        config_model.contains("SyncOrchestratorServiceConfig"),
        "service-kit config model must expose a typed sync orchestrator service config"
    );
    assert!(
        config_model.contains("sync_orchestrator_service"),
        "service-kit config model must expose sync_orchestrator_service()"
    );
    assert!(
        config_model.contains("sync_orchestrator"),
        "services config must include [services.sync_orchestrator]"
    );
    assert!(
        config_file.is_file(),
        "sync orchestrator should be configured through config/services/sync-orchestrator.toml"
    );
}

#[test]
fn push_proxy_has_typed_service_config() {
    let root = workspace_root();
    let config_model =
        fs::read_to_string(root.join("crates/flare-im-service-kit/src/config/mod.rs"))
            .expect("read service-kit config model");
    let config_file = root.join("config/services/push_proxy.toml");

    assert!(
        config_model.contains("PushProxyServiceConfig"),
        "service-kit config model must expose a typed push proxy service config"
    );
    assert!(
        config_model.contains("push_proxy_service"),
        "service-kit config model must expose push_proxy_service()"
    );
    assert!(
        config_model.contains("push_proxy"),
        "services config must include [services.push_proxy]"
    );
    assert!(
        config_file.is_file(),
        "push proxy should be configured through config/services/push_proxy.toml"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
