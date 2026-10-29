//! 缺租户时不得兜底成租户 `"0"`（W11-T）。
//!
//! 租户 `"0"` 是单租户私有化部署里的正常租户，只能被显式声明。缺租户却按 `"0"` 处理会把消息、
//! 事件与会话写进（或读出）租户 0 的数据：单聊会话 ID 不含租户，同一 user_id 对在两个租户里得到
//! 同一个会话 ID；生产方盖上 `"0"` 还让消费者的死信分支永远走不到（W10-A 复审）。
//!
//! 这里扫描 Core 工作区的生产代码（去掉内联测试模块、`tests/`、`benches/`、顶层示例客户端），
//! 找出一条语句里同时出现「租户」与「`"0"` 兜底」的写法：`unwrap_or("0")`、
//! `unwrap_or_else(|| "0".to_string())`、`with_tenant_id("0")`、`DEFAULT_TENANT_ID` 兜底、
//! serde 的租户默认值，以及 `normalize_tenant_id(... unwrap_or_default())` 这种借归一化把空串
//! 变成 `"0"` 的写法。`ALLOWED` 是尚未处理、各有理由的存量（按文件精确计数）：新增一处、或修掉
//! 一处而不更新清单都会失败，清单只能缩小。缺租户应当报错或进死信，见
//! `flare_im_contracts::utils::normalize_optional_tenant_id`。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// 存量：(文件, 处数, 理由)。新代码不得加入；修掉后把对应行删掉或调小。
const ALLOWED: &[(&str, usize, &str)] = &[
    (
        "crates/flare-im-contracts/src/utils/mod.rs",
        1,
        "定义显式租户 0 的常量（normalize_tenant_id 的别名目标），不是兜底本身",
    ),
    (
        "crates/flare-im-service-kit/src/gateway_auth.rs",
        1,
        "宽松策略下未绑定租户的连接按部署默认租户 0 查准入（单租户私有化的旧令牌不带租户），严格策略已拒绝；改为一律拒绝属部署级默认租户的取舍（第 36 条），待用户决定",
    ),
    (
        "flare-push/server/src/config/settings.rs",
        1,
        "推送服务的部署级默认租户配置项（PUSH_SERVER_DEFAULT_TENANT_ID 未设时为 0，单租户私有化默认），去留属第 36 条，待用户决定",
    ),
    (
        "flare-signaling/gateway/src/application/handlers/auth_handler.rs",
        1,
        "令牌不带租户时的部署级默认租户（ACCESS_GATEWAY_DEFAULT_TENANT_ID 未设时为 0，单租户私有化默认）；改为拒绝会让不带租户的令牌都登录失败，属第 36 条，待用户决定",
    ),
    (
        "flare-signaling/gateway/src/config/settings.rs",
        1,
        "网关的部署级默认租户配置项（未设时为 0，单租户私有化默认），属第 36 条，待用户决定",
    ),
    (
        "flare-signaling/gateway/src/constants.rs",
        1,
        "网关部署级默认租户常量（上两项的取值），属第 36 条，待用户决定",
    ),
    (
        "flare-signaling/route/src/service/wire.rs",
        1,
        "路由服务的部署级默认租户配置项（未设时为 0，单租户私有化默认），属第 36 条，待用户决定",
    ),
    (
        "tools/flare-dlq-replay/src/main.rs",
        1,
        "运维重放工具的环境变量默认值，由操作者显式指定",
    ),
];

/// W11-T 改成缺租户即报错或进死信的文件：必须保持零兜底，也不得进入 `ALLOWED`。
/// （W11-T 也改过的 `crates/flare-im-ack/src/ack/archiver.rs` 从不在模块树里，已删除。）
const FIXED_BY_W11_T: &[&str] = &[
    "crates/flare-im-message-pipeline/src/hook/builder.rs",
    "crates/flare-im-message-pipeline/src/hook/execution_service.rs",
    "crates/flare-im-message-pipeline/src/messaging/push_repository.rs",
    "flare-conversation/src/infrastructure/persistence/postgres_repository.rs",
    "flare-message-ingest/src/application/handlers/message_ingest_handler.rs",
    "flare-message-ingest/src/application/handlers/wal_replay_handler.rs",
    "flare-orchestrator/src/application/handlers/event_handler.rs",
    "flare-orchestrator/src/domain/repository/user_sync_compensation_repository.rs",
    "flare-orchestrator/src/infrastructure/persistence/redis_user_sync_index.rs",
    "flare-orchestrator/src/interface/mq/storage_consumer.rs",
    "flare-storage/writer/src/convert.rs",
    "flare-storage/writer/src/domain/repository/message_idempotency_repository.rs",
    "flare-storage/writer/src/domain/service/message_persistence.rs",
    "flare-storage/writer/src/infrastructure/persistence/repository/redis_cache.rs",
    "flare-storage/writer/src/interface/messaging/message_consumer.rs",
    "flare-storage/writer/src/interface/messaging/operation_consumer.rs",
    "flare-sync-orchestrator/src/application/handlers/sync_orchestration_handler.rs",
    "flare-sync-orchestrator/src/infrastructure/rpc/sync_adapters.rs",
];

/// W12-T 改成缺租户即报错的文件：必须保持零兜底，也不得进入 `ALLOWED`。
const FIXED_BY_W12_T: &[&str] = &[
    "crates/flare-im-hooks/src/hooks/selector.rs",
    "crates/flare-im-message-pipeline/src/extension/orchestrator.rs",
    "crates/flare-im-message-pipeline/src/extension/routing.rs",
    "flare-conversation/src/domain/service/conversation_domain_service.rs",
    "flare-media/src/interface/grpc/media_handler.rs",
    "flare-orchestrator/src/application/commands/message_action_commands.rs",
    "flare-signaling/gateway/src/domain/service/push_domain_service.rs",
    "flare-signaling/gateway/src/domain/service/sync_service.rs",
    "flare-storage/reader/src/infrastructure/persistence/optimized_postgres_store.rs",
];

/// W13-D 改成缺租户即报错的 Capability 调度文件（调度请求体、调度日志字段）：
/// 必须保持零兜底，也不得进入 `ALLOWED`。
const FIXED_BY_W13_D: &[&str] = &[
    "flare-capability/src/application/handler/capability_dispatch.rs",
    "flare-capability/src/application/handler/remote_dispatch.rs",
    "flare-capability/src/interface/grpc/capability/service.rs",
];

/// 兜底写法（已去掉空白）。语句里同时提到租户才算。
const FALLBACKS: &[&str] = &[
    r#"unwrap_or("0")"#,
    r#"||"0".to_string()"#,
    r#"||"0".to_owned()"#,
    r#"||"0".into()"#,
    r#"||String::from("0")"#,
    r#"|_|"0".to_string()"#,
    r#"|_|"0".into()"#,
    r#"with_tenant_id("0")"#,
    r#"Some("0")"#,
    r#"tenant_id:"0".to_string()"#,
    r#"DEFAULT_TENANT_ID:&str="0""#,
    "unwrap_or(DEFAULT_TENANT_ID)",
    "||DEFAULT_TENANT_ID.to_string()",
    "||DEFAULT_TENANT_ID.to_owned()",
    "||DEFAULT_TENANT_ID.into()",
    r#"default="default_tenant_id""#,
];

/// 借归一化把空租户变成 "0"：语句里有 `normalize_tenant_id(` 时才算。
const IMPLICIT_FALLBACKS: &[&str] = &["unwrap_or_default()", r#"unwrap_or("")"#];

/// 顶层目录里不是服务端生产代码的部分。
const SKIPPED_TOP_LEVEL: &[&str] = &["arch-tests", "examples"];

#[test]
fn production_code_never_defaults_a_missing_tenant_to_zero() {
    let root = workspace_root();
    let found = tenant_fallbacks(&root);
    let mut violations = Vec::new();

    for (file, sites) in &found {
        match ALLOWED.iter().find(|(allowed, _, _)| allowed == file) {
            None => violations.push(format!(
                "{file}: {} new tenant \"0\" fallback(s); reject or dead-letter instead:\n{}",
                sites.len(),
                sites.join("\n")
            )),
            Some((_, allowed, _)) if *allowed != sites.len() => violations.push(format!(
                "{file}: {} tenant \"0\" fallback(s), the allowlist says {allowed}; \
                 a new one must be removed, a fixed one must shrink ALLOWED:\n{}",
                sites.len(),
                sites.join("\n")
            )),
            Some(_) => {}
        }
    }
    for (file, allowed, _) in ALLOWED {
        if !found.contains_key(*file) {
            violations.push(format!(
                "{file}: allowlisted for {allowed} fallback(s) but none remain; remove it from ALLOWED"
            ));
        }
    }

    assert!(violations.is_empty(), "{}", violations.join("\n\n"));
}

#[test]
fn files_fixed_by_w11_t_stay_free_of_tenant_zero_fallbacks() {
    let root = workspace_root();
    let found = tenant_fallbacks(&root);
    for file in FIXED_BY_W11_T {
        assert!(
            root.join(file).is_file(),
            "{file} moved; update this guard together with the code"
        );
        assert!(
            !ALLOWED.iter().any(|(allowed, _, _)| allowed == file),
            "{file} must not be allowlisted"
        );
        assert!(
            !found.contains_key(*file),
            "{file} defaults a missing tenant to \"0\" again:\n{}",
            found[*file].join("\n")
        );
    }
}

#[test]
fn files_fixed_by_w12_t_stay_free_of_tenant_zero_fallbacks() {
    let root = workspace_root();
    let found = tenant_fallbacks(&root);
    for file in FIXED_BY_W12_T {
        assert!(
            root.join(file).is_file(),
            "{file} moved; update this guard together with the code"
        );
        assert!(
            !ALLOWED.iter().any(|(allowed, _, _)| allowed == file),
            "{file} must not be allowlisted"
        );
        assert!(
            !found.contains_key(*file),
            "{file} defaults a missing tenant to \"0\" again:\n{}",
            found[*file].join("\n")
        );
    }
}

#[test]
fn files_fixed_by_w13_d_stay_free_of_tenant_zero_fallbacks() {
    let root = workspace_root();
    let found = tenant_fallbacks(&root);
    for file in FIXED_BY_W13_D {
        assert!(
            root.join(file).is_file(),
            "{file} moved; update this guard together with the code"
        );
        assert!(
            !ALLOWED.iter().any(|(allowed, _, _)| allowed == file),
            "{file} must not be allowlisted"
        );
        assert!(
            !found.contains_key(*file),
            "{file} defaults a missing tenant to \"0\" again:\n{}",
            found[*file].join("\n")
        );
    }
}

/// 扫描器自检：每种写法都能认出来，不提租户的 "0" 不算，测试模块被跳过。
#[test]
fn the_scanner_recognises_each_fallback_shape() {
    let flagged = [
        r#"let tenant_id = ctx.tenant_id().unwrap_or("0");"#,
        "let tenant_id = ctx\n    .tenant_id()\n    .map(normalize_tenant_id)\n    .unwrap_or_else(|| \"0\".to_string());",
        r#"ctx = ctx.with_tenant_id("0");"#,
        r#"let tenant = req.tenant_id.clone().unwrap_or_else(|| "0".into());"#,
        r#"let t = TenantRef { tenant_id: "0".to_string() };"#,
        r#"const DEFAULT_TENANT_ID: &str = "0";"#,
        "let tenant = ctx.tenant_id().unwrap_or(DEFAULT_TENANT_ID);",
        "#[serde(default = \"default_tenant_id\")]\npub tenant_id: String,",
        "normalize_tenant_id(ctx.tenant_id().unwrap_or_default())",
        r#"let tenant = env::var("X_TENANT_ID").unwrap_or_else(|_| "0".to_string());"#,
        r#"scope(tenant_id.or(Some("0")))"#,
    ];
    for source in flagged {
        assert_eq!(fallback_sites(source).len(), 1, "not recognised:\n{source}");
    }

    let ignored = [
        r#"let sender_id = sender_id.unwrap_or("0");"#,
        r#"let enabled = matches!(value, "0" | "false");"#,
        "let tenant = normalize_optional_tenant_id(ctx.tenant_id());\nlet uid = uid.unwrap_or_default();",
        "#[cfg(test)]\nmod tests {\n    fn f() { let t = ctx.tenant_id().unwrap_or(\"0\"); }\n}",
        "// let tenant_id = ctx.tenant_id().unwrap_or(\"0\");",
    ];
    for source in ignored {
        assert!(
            fallback_sites(source).is_empty(),
            "false positive:\n{source}"
        );
    }
}

fn tenant_fallbacks(root: &Path) -> BTreeMap<String, Vec<String>> {
    let mut found = BTreeMap::new();
    for file in rust_sources(root) {
        let relative = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        let content = fs::read_to_string(&file).expect("read rust source");
        let sites = fallback_sites(&content);
        if !sites.is_empty() {
            found.insert(relative, sites);
        }
    }
    found
}

/// 返回生产代码中每处租户兜底所在的行（`行号: 原文`）。
fn fallback_sites(content: &str) -> Vec<String> {
    let lines = production_lines(content);
    // 去掉空白后拼成一串，记下每个字符所在的行，便于按语句判断。
    let mut text = String::new();
    let mut line_of = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let code = strip_line_comment(line);
        for ch in code.chars().filter(|ch| !ch.is_whitespace()) {
            text.push(ch);
            line_of.push(index);
        }
        // 行尾当成语句内的分隔，不影响按 `;`、`{`、`}` 切语句。
    }

    let mut hit_lines = Vec::new();
    let mut record = |start: usize, needs: &str| {
        let statement_start = text[..start]
            .rfind([';', '{', '}'])
            .map(|position| position + 1)
            .unwrap_or(0);
        let statement_end = text[start..]
            .find([';', '{', '}'])
            .map(|position| start + position)
            .unwrap_or(text.len());
        let statement = &text[statement_start..statement_end];
        if statement.to_ascii_lowercase().contains("tenant") && statement.contains(needs) {
            let line = line_of[start];
            if !hit_lines.contains(&line) {
                hit_lines.push(line);
            }
        }
    };
    for pattern in FALLBACKS {
        for (start, _) in text.match_indices(pattern) {
            record(start, "");
        }
    }
    for pattern in IMPLICIT_FALLBACKS {
        for (start, _) in text.match_indices(pattern) {
            record(start, "normalize_tenant_id(");
        }
    }

    hit_lines.sort_unstable();
    hit_lines
        .into_iter()
        .map(|line| format!("  {}: {}", line + 1, lines[line].trim()))
        .collect()
}

fn strip_line_comment(line: &str) -> &str {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") {
        return "";
    }
    match line.find(" //") {
        Some(position) if !line[..position].contains('"') => &line[..position],
        _ => line,
    }
}

/// 去掉 `#[cfg(test)]` 模块后的源码行；被去掉的行留空，行号保持不变。
fn production_lines(content: &str) -> Vec<&str> {
    let mut output = Vec::new();
    let mut pending_test_cfg = false;
    let mut skipping_test_module = false;
    let mut brace_depth = 0isize;

    for line in content.lines() {
        let trimmed = line.trim_start();

        if skipping_test_module {
            brace_depth += brace_delta(line);
            if brace_depth <= 0 {
                skipping_test_module = false;
            }
            output.push("");
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_test_cfg = true;
            output.push("");
            continue;
        }

        if pending_test_cfg && (trimmed.starts_with("mod ") || trimmed.starts_with("pub mod ")) {
            skipping_test_module = true;
            brace_depth = brace_delta(line);
            if brace_depth <= 0 {
                brace_depth = 1;
            }
            pending_test_cfg = false;
            output.push("");
            continue;
        }

        pending_test_cfg = false;
        output.push(line);
    }

    output
}

fn brace_delta(line: &str) -> isize {
    line.chars().filter(|ch| *ch == '{').count() as isize
        - line.chars().filter(|ch| *ch == '}').count() as isize
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).expect("read workspace root") {
        let entry = entry.expect("read workspace entry");
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() && !name.starts_with('.') && !SKIPPED_TOP_LEVEL.contains(&name.as_str()) {
            collect_rust_sources(&path, &mut files);
        }
    }
    files.sort();
    files
}

fn collect_rust_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if path.is_dir() {
            if name.starts_with('.')
                || matches!(
                    name.as_ref(),
                    "target" | "tests" | "benches" | "examples" | "node_modules"
                )
            {
                continue;
            }
            collect_rust_sources(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && !name.ends_with("_tests.rs")
            && name != "tests.rs"
        {
            files.push(path);
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
