//! `flare-api-gateway/.env.example` 只列 API Gateway 真正读取、且登记在 env_registry 的变量。
//!
//! 示例文件曾整份停在旧前缀 `FLARE_CORE_GATEWAY_*` 上（网关早已只读 `FLARE_API_GATEWAY_*`），
//! 还列着网关从不读取的变量；照着它配置的部署全部静默不生效。
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

const API_PREFIX: &str = "FLARE_API_GATEWAY_";

#[test]
fn env_example_lists_only_registered_api_gateway_variables() {
    let root = workspace_root();
    let example =
        fs::read_to_string(root.join("flare-api-gateway/.env.example")).expect("read .env.example");
    let keys = example_keys(&example);
    assert!(!keys.is_empty(), ".env.example lists no variables");

    let registry = fs::read_to_string(root.join("crates/flare-im-service-kit/src/env_registry.rs"))
        .expect("read env registry");
    let owners = registered_owners(&registry);

    let read = api_gateway_reads(&root);
    let obsolete = ["FLARE", "CORE", "GATEWAY"].join("_");
    let mut violations = Vec::new();
    for key in &keys {
        if key.starts_with(&obsolete) {
            violations.push(format!("{key}: obsolete prefix {obsolete}_"));
            continue;
        }
        match owners.get(key).map(String::as_str) {
            Some("ApiGateway" | "Shared" | "Runtime") => {}
            Some(other) => violations.push(format!("{key}: registered for {other}")),
            None => violations.push(format!("{key}: not in env_registry")),
        }
        if !read.contains(key) {
            violations.push(format!("{key}: API Gateway does not read it"));
        }
    }
    assert!(
        violations.is_empty(),
        "flare-api-gateway/.env.example:\n{}",
        violations.join("\n")
    );
}

/// `KEY=value` 与注释掉的 `# KEY=value` 行里的变量名。
fn example_keys(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim_start_matches('#').trim();
            let (key, _) = line.split_once('=')?;
            is_env_key(key).then(|| key.to_string())
        })
        .collect()
}

/// env_registry 里每个变量的属主：`env(\n "KEY",\n EnvOwner::X,` 或单行写法。
fn registered_owners(source: &str) -> BTreeMap<String, String> {
    let mut owners = BTreeMap::new();
    let mut rest = source;
    while let Some(start) = rest.find("env(") {
        rest = &rest[start + 4..];
        let Some(open) = rest.find('"') else { break };
        let after = &rest[open + 1..];
        let Some(close) = after.find('"') else { break };
        let key = &after[..close];
        let tail = &after[close + 1..];
        if let Some(owner_at) = tail.find("EnvOwner::") {
            let owner: String = tail[owner_at + "EnvOwner::".len()..]
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric())
                .collect();
            if is_env_key(key) {
                owners.insert(key.to_string(), owner);
            }
        }
        rest = tail;
    }
    owners
}

/// API Gateway 读取的变量：`GatewaySettings` 在 API 作用域拼出的 `FLARE_API_GATEWAY_<NAME>`
/// （config.rs 里 `scope,` 后紧跟 `"NAME"` 的写法，可跨行），以及网关源码里直接写出的变量名字面量。
fn api_gateway_reads(root: &std::path::Path) -> BTreeSet<String> {
    let mut read = BTreeSet::new();
    let config = fs::read_to_string(root.join("crates/flare-im-service-kit/src/gateway/config.rs"))
        .expect("read gateway config");
    let mut rest = config.as_str();
    while let Some(at) = rest.find("scope,") {
        let after = rest[at + "scope,".len()..].trim_start();
        rest = after;
        let Some(name) = after.strip_prefix('"') else {
            continue;
        };
        let Some(end) = name.find('"') else { break };
        read.insert(format!("{API_PREFIX}{}", &name[..end]));
        rest = &name[end + 1..];
    }
    for file in [
        "flare-api-gateway/src/service.rs",
        "flare-api-gateway/src/interface/http/push_token_handler.rs",
    ] {
        let source = fs::read_to_string(root.join(file)).expect("read gateway source");
        let mut rest = source.as_str();
        while let Some(start) = rest.find('"') {
            let after = &rest[start + 1..];
            let Some(end) = after.find('"') else { break };
            let candidate = &after[..end];
            if candidate.starts_with("FLARE_") && is_env_key(candidate) {
                read.insert(candidate.to_string());
            }
            rest = &after[end + 1..];
        }
    }
    read
}

fn is_env_key(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
