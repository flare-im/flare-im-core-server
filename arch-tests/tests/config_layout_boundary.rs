use std::fs;
use std::path::PathBuf;

#[test]
fn service_config_files_are_kebab_case_and_indexed() {
    let root = workspace_root();
    let services_dir = root.join("config/services");
    let readme = fs::read_to_string(root.join("config/README.md")).expect("read config readme");

    let expected = [
        ("access-gateway.toml", "[services.access_gateway]"),
        ("admin-gateway.toml", "[services.admin_gateway]"),
        ("api-gateway.toml", "[services.api_gateway]"),
        ("capability.toml", "[services.capability]"),
        ("conversation.toml", "[services.conversation]"),
        ("media.toml", "[services.media]"),
        ("message-ingest.toml", "[services.message_ingest]"),
        (
            "message-orchestrator.toml",
            "[services.message_orchestrator]",
        ),
        ("push-proxy.toml", "[services.push_proxy]"),
        ("push-server.toml", "[services.push_server]"),
        ("push-worker.toml", "[services.push_worker]"),
        ("signaling-online.toml", "[services.signaling_online]"),
        ("signaling-route.toml", "[services.signaling_route]"),
        ("storage-reader.toml", "[services.storage_reader]"),
        ("storage-writer.toml", "[services.storage_writer]"),
        ("sync-orchestrator.toml", "[services.sync_orchestrator]"),
    ];

    let toml_files = fs::read_dir(&services_dir)
        .expect("read service config dir")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            (path.extension().is_some_and(|ext| ext == "toml")).then_some(path)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        toml_files.len(),
        expected.len(),
        "service config dir should contain exactly one TOML file per service"
    );

    for (file_name, table_header) in expected {
        let path = services_dir.join(file_name);
        assert!(path.is_file(), "missing service config file {file_name}");
        assert!(
            !file_name.contains('_'),
            "service config file names must use kebab-case: {file_name}"
        );
        let content = fs::read_to_string(&path).expect("read service config");
        assert!(
            content.contains(table_header),
            "{file_name} must own table {table_header}"
        );
        assert!(
            readme.contains(file_name) && readme.contains(table_header),
            "config/README.md must index {file_name} and {table_header}"
        );
    }
}

#[test]
fn config_root_documents_all_merge_layers() {
    let root = workspace_root();
    let readme = fs::read_to_string(root.join("config/README.md")).expect("read config readme");

    for required in [
        "base.toml",
        "shared/*.toml",
        "services/*.toml",
        "overrides/*.toml",
        "environments/{FLARE_ENV}.toml",
        "FLARE_CONFIG_PATH",
    ] {
        assert!(
            readme.contains(required),
            "config/README.md must document {required}"
        );
    }

    for dir in ["shared", "services", "overrides", "environments"] {
        assert!(
            !root.join("config").join(dir).join("README.md").exists(),
            "config/{dir}/README.md must not exist; keep config docs centralized in config/README.md"
        );
    }
}

#[test]
fn startup_script_uses_canonical_service_config_file_names() {
    let root = workspace_root();
    let script =
        fs::read_to_string(root.join("scripts/start_server.sh")).expect("read startup script");

    for forbidden in [
        "access_gateway.toml",
        "admin_gateway.toml",
        "api_gateway.toml",
        "message_ingest.toml",
        "message_orchestrator.toml",
        "push_proxy.toml",
        "push_server.toml",
        "push_worker.toml",
    ] {
        assert!(
            !script.contains(forbidden),
            "startup script must use canonical kebab-case config file names, found {forbidden}"
        );
    }
}

#[test]
fn directory_loader_order_matches_documented_configuration_layers() {
    let root = workspace_root();
    let runtime = fs::read_to_string(root.join("crates/flare-im-service-kit/src/config/mod.rs"))
        .expect("read config loader");
    let readme = fs::read_to_string(root.join("config/README.md")).expect("read config readme");

    for layer in ["shared", "services", "overrides"] {
        assert!(
            runtime.contains(&format!(
                "merge_directory(&mut merged, &path.join(\"{layer}\"))"
            )),
            "config loader must merge {layer}"
        );
        assert!(
            readme.contains(&format!("{layer}/*.toml")),
            "config README must document {layer} merge layer"
        );
    }
}

#[test]
fn committed_service_configs_do_not_advertise_dead_toml_keys() {
    let root = workspace_root();
    let config_dir = root.join("config");
    let forbidden = [
        (
            "conversation_store =",
            "use token_store/session_store typed fields or remove the old key",
        ),
        (
            "conversation_store_ttl_seconds",
            "use session_store_ttl_seconds only after it is consumed by the service",
        ),
        (
            "upload_conversation_store",
            "media config must use upload_session_store",
        ),
        (
            "[services.api_gateway.grpc]",
            "gateway downstream routes are env-scoped until typed config is wired",
        ),
        (
            "[services.admin_gateway.grpc]",
            "gateway downstream routes are env-scoped until typed config is wired",
        ),
        (
            "max_poll_records",
            "push-server batch tuning is not read from FlareAppConfig",
        ),
        (
            "gateway_router_connection_pool_size",
            "push-server gateway router tuning is not read from FlareAppConfig",
        ),
        (
            "ack_timeout_seconds",
            "push-server ACK timeout tuning is not read from FlareAppConfig",
        ),
    ];

    for path in toml_files_under(&config_dir) {
        let content = fs::read_to_string(&path).expect("read config toml");
        for (needle, reason) in forbidden {
            assert!(
                !content.contains(needle),
                "{} contains dead config key `{needle}`: {reason}",
                path.display()
            );
        }
    }
}

#[test]
fn config_readme_documents_environment_scope_and_placeholder_expansion() {
    let root = workspace_root();
    let readme = fs::read_to_string(root.join("config/README.md")).expect("read config readme");

    for required in [
        "only merges root `[mq]` and `[object_storage.*]`",
        "overrides/*.toml",
        "${ENV_VAR}",
        "Missing variables fail config loading",
    ] {
        assert!(
            readme.contains(required),
            "config/README.md must document environment scope detail: {required}"
        );
    }
}

fn toml_files_under(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_toml_files(dir, &mut out);
    out.sort();
    out
}

fn collect_toml_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read config dir") {
        let entry = entry.expect("read config dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_toml_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "toml") {
            out.push(path);
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

/// `base.toml` 里的环境占位符必须**都带内联默认值**。
///
/// 中间件地址此前是硬编码的 `localhost:25432` 那一套，因此容器里跑不起来——
/// 服务会去连自己容器的 localhost。改成占位符之后，本机开发靠的就是这些默认值：
/// 少写一个 `:-`，配置加载会直接报 "environment variable X is not set"，
/// 本机与测试全线起不来。
///
/// 这条断言很便宜，但挡的是「容器能跑了、本机反而跑不了」这种一改就翻车的方向。
#[test]
fn base_config_placeholders_all_carry_defaults() {
    let root = workspace_root();
    let base = fs::read_to_string(root.join("config/base.toml")).expect("read base.toml");

    let mut offenders = Vec::new();
    let mut cursor = 0usize;
    while let Some(rel) = base[cursor..].find("${") {
        let start = cursor + rel;
        let end = base[start..]
            .find('}')
            .map(|e| start + e)
            .expect("unclosed placeholder in base.toml");
        let body = &base[start + 2..end];
        if !body.contains(":-") {
            offenders.push(body.to_string());
        }
        cursor = end + 1;
    }

    assert!(
        offenders.is_empty(),
        "base.toml 的这些占位符没有内联默认值，本机开发会因为「环境变量未设置」直接起不来：{offenders:?}\n\
         写法应为 ${{VAR:-本机默认值}}。"
    );
}

/// 占位符的默认值必须仍然是**本机 dev 的那一套地址**。
///
/// 参数化的前提是「不改变本机行为」。若有人把默认值顺手改成容器里的服务名
/// （postgres / redis / nats），本机开发和一整套依赖 dev 栈的测试会集体连不上，
/// 而且报错是连接超时，不会有人一眼想到是配置默认值被动过。
#[test]
fn base_config_defaults_still_point_at_local_dev() {
    let root = workspace_root();
    let base = fs::read_to_string(root.join("config/base.toml")).expect("read base.toml");

    for expected in [
        "${FLARE_POSTGRES_URL:-postgres://flare:flare123@localhost:25432/flare2}",
        "${FLARE_REDIS_URL_BASE:-redis://localhost:26379}",
        "${FLARE_NATS_URL:-nats://127.0.0.1:24222}",
        "${FLARE_KAFKA_BROKERS:-127.0.0.1:29092}",
        "${FLARE_S3_ENDPOINT:-http://127.0.0.1:29000}",
        "${FLARE_CONSUL_ENDPOINT:-http://localhost:28500}",
    ] {
        assert!(
            base.contains(expected),
            "base.toml 少了这条本机默认值（被改动过？）：{expected}"
        );
    }
}
