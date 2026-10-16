//! Core 服务进程的日志与 trace 只经 `flare_im_service_kit::tracing::init_tracing_from_config`
//! 初始化：`[logging]`、`RUST_LOG`、`FLARE_LOG_FORMAT`、`OTEL_EXPORTER_OTLP_ENDPOINT` 在每个服务上
//! 含义一致。漏接的服务没有全局 subscriber，日志全部丢弃、也不导出 trace。
use std::fs;
use std::path::PathBuf;

/// (服务 crate 目录, 启动路径上的源文件)：第一项是 `[[bin]]` 入口，其后是入口直接调用的 bootstrap。
const SERVICE_STARTUP_PATHS: &[(&str, &[&str])] = &[
    ("flare-api-gateway", &["src/main.rs", "src/service.rs"]),
    ("flare-admin-gateway", &["src/main.rs", "src/service.rs"]),
    ("flare-signaling/gateway", &["cmd/main.rs"]),
    ("flare-signaling/online", &["cmd/main.rs"]),
    ("flare-signaling/route", &["cmd/main.rs"]),
    ("flare-push/proxy", &["cmd/main.rs"]),
    ("flare-push/server", &["cmd/main.rs"]),
    ("flare-push/worker", &["cmd/main.rs"]),
    ("flare-storage/writer", &["cmd/main.rs"]),
    ("flare-storage/reader", &["cmd/main.rs"]),
    ("flare-conversation", &["cmd/main.rs"]),
    (
        "flare-sync-orchestrator",
        &["cmd/main.rs", "src/service/mod.rs"],
    ),
    ("flare-message-ingest", &["cmd/main.rs"]),
    ("flare-orchestrator", &["cmd/main.rs"]),
    (
        "flare-capability",
        &["cmd/main.rs", "src/composition/bootstrap.rs"],
    ),
    ("flare-media", &["cmd/main.rs"]),
];

const INIT_CALL: &str = "init_tracing_from_config(";

#[test]
fn every_service_binary_initializes_tracing_through_service_kit() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for (crate_dir, files) in SERVICE_STARTUP_PATHS {
        let manifest =
            fs::read_to_string(root.join(crate_dir).join("Cargo.toml")).expect("read manifest");
        let entrypoint = files[0];
        if !manifest.contains(&format!("path = \"{entrypoint}\"")) {
            violations.push(format!(
                "{crate_dir}: {entrypoint} is not the [[bin]] entrypoint declared in Cargo.toml"
            ));
        }

        let mut init_calls = 0;
        for file in *files {
            let path = format!("{crate_dir}/{file}");
            let source = fs::read_to_string(root.join(&path)).expect("read startup source");
            let normalized = source.split_whitespace().collect::<Vec<_>>().join(" ");
            init_calls += normalized.matches(INIT_CALL).count();

            for forbidden in ["tracing_subscriber", "set_global_default"] {
                if source.contains(forbidden) {
                    violations.push(format!(
                        "{path} sets up its own subscriber via `{forbidden}`; call flare_im_service_kit::tracing::init_tracing_from_config instead"
                    ));
                }
            }
            if drops_tracing_guard(&normalized) {
                violations.push(format!(
                    "{path} binds the tracing guard to `_`; hold it (e.g. `let _tracing = ...`) until the service stops so the last spans are exported"
                ));
            }
        }

        if init_calls != 1 {
            violations.push(format!(
                "{crate_dir}: startup path {files:?} must call init_tracing_from_config exactly once, found {init_calls}"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "Core service binaries must initialize tracing through service-kit:\n{}",
        violations.join("\n")
    );
}

#[test]
fn tracing_startup_table_covers_every_service_binary() {
    let root = workspace_root();
    let workspace = fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest");
    let mut missing = Vec::new();

    for member in workspace_members(&workspace) {
        // 运维工具是一次性 CLI，自带终端输出，不是常驻服务进程。
        if member.starts_with("tools/") {
            continue;
        }
        let manifest =
            fs::read_to_string(root.join(&member).join("Cargo.toml")).expect("read manifest");
        let has_binary =
            manifest.contains("[[bin]]") || root.join(&member).join("src/main.rs").is_file();
        if has_binary
            && !SERVICE_STARTUP_PATHS
                .iter()
                .any(|(crate_dir, _)| *crate_dir == member)
        {
            missing.push(member);
        }
    }

    assert!(
        missing.is_empty(),
        "service binaries missing from SERVICE_STARTUP_PATHS (add their entrypoint and bootstrap): {missing:?}"
    );
}

/// `let _ = ...init_tracing_from_config(...)` 会立刻释放守卫：OTLP exporter 随即关闭，此后的 span 不再导出。
fn drops_tracing_guard(normalized: &str) -> bool {
    normalized.match_indices(INIT_CALL).any(|(index, _)| {
        let statement_start = normalized[..index]
            .rfind([';', '{', '}'])
            .map_or(0, |position| position + 1);
        let statement = &normalized[statement_start..index];
        statement
            .rfind("let ")
            .is_some_and(|binding| statement[binding..].starts_with("let _ ="))
    })
}

fn workspace_members(workspace: &str) -> Vec<String> {
    let start = workspace
        .find("members = [")
        .expect("workspace manifest declares members");
    let block = &workspace[start + "members = [".len()..];
    let block = &block[..block.find(']').expect("members list is closed")];
    block
        .lines()
        .map(|line| line.split('#').next().unwrap_or_default().trim())
        .map(|entry| entry.trim_end_matches(',').trim_matches('"'))
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
