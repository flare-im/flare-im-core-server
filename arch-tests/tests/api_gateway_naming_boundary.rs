use std::fs;
use std::path::PathBuf;

#[test]
fn api_gateway_runtime_identity_is_consistent() {
    let root = workspace_root();

    let service_names =
        fs::read_to_string(root.join("crates/flare-im-contracts/src/service_names.rs"))
            .expect("read service names");
    assert!(service_names.contains("pub const API_GATEWAY: &str = \"flare-api-gateway\""));

    let service_rs =
        fs::read_to_string(root.join("flare-api-gateway/src/service.rs")).expect("read service");
    for required in [
        "GatewayEnvScope::Api",
        "service_names::API_GATEWAY",
        "config/services/api_gateway.toml",
        "FLARE_API_GATEWAY_TOKEN_SECRET",
        "services.api_gateway.token_secret",
    ] {
        assert!(
            service_rs.contains(required),
            "api-gateway service bootstrap must use runtime identity `{required}`"
        );
    }

    let main_rs =
        fs::read_to_string(root.join("flare-api-gateway/src/main.rs")).expect("read main");
    assert!(
        main_rs.contains("flare_api_gateway::ApplicationBootstrap::run().await"),
        "api-gateway main must stay a thin process wrapper around ApplicationBootstrap"
    );

    assert!(
        root.join("config/services/api_gateway.toml").is_file(),
        "api-gateway config file must be named api_gateway.toml"
    );

    let start_script =
        fs::read_to_string(root.join("scripts/start_server.sh")).expect("read start script");
    for required in [
        "\"api-gateway\"",
        "FLARE_API_GATEWAY_TOKEN_SECRET",
        "FLARE_API_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK",
    ] {
        assert!(
            start_script.contains(required),
            "start script must launch api-gateway with `{required}`"
        );
    }

    let old_const = ["CORE", "GATEWAY"].join("_");
    let old_service_name = ["flare-core", "gateway"].join("-");
    let old_service_key = ["services.core", "gateway"].join("_");
    let old_config_file = ["core", "gateway.toml"].join("_");
    let old_env_prefix = ["FLARE_CORE", "GATEWAY"].join("_");
    let forbidden = [
        old_const.as_str(),
        old_service_name.as_str(),
        old_service_key.as_str(),
        old_config_file.as_str(),
        old_env_prefix.as_str(),
    ];

    for (label, content) in [
        ("service_names.rs", service_names.as_str()),
        ("flare-api-gateway/src/service.rs", service_rs.as_str()),
        ("flare-api-gateway/src/main.rs", main_rs.as_str()),
        ("scripts/start_server.sh", start_script.as_str()),
    ] {
        for pattern in forbidden {
            assert!(
                !content.contains(pattern),
                "{label} must not retain old api-gateway identity `{pattern}`"
            );
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
