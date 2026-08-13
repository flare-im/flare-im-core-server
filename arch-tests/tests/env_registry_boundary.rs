use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn production_env_vars_are_registered() {
    let root = workspace_root();
    let registry = fs::read_to_string(root.join("crates/flare-im-service-kit/src/env_registry.rs"))
        .expect("read env registry");
    let registered = registered_keys(&registry);

    let mut violations = Vec::new();
    for file in rust_sources(&root) {
        let relative = file.strip_prefix(&root).unwrap_or(&file);
        if should_skip(relative) {
            continue;
        }

        let content = fs::read_to_string(&file).expect("read rust source");
        let production_content = strip_inline_test_modules(&content);
        for key in env_var_literals(&production_content) {
            if !registered.contains(&key) {
                violations.push(format!(
                    "{} reads undocumented env var `{}`",
                    relative.display(),
                    key
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production env vars must be listed in flare-im-service-kit::env_registry:\n{}",
        violations.join("\n")
    );
}

fn registered_keys(source: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let mut rest = source;
    while let Some(start) = rest.find('"') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('"') else {
            break;
        };
        let candidate = &after[..end];
        if is_env_key(candidate) {
            keys.insert(candidate.to_string());
        }
        rest = &after[end + 1..];
    }
    keys
}

fn env_var_literals(source: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for marker in ["env::var(\"", "std::env::var(\""] {
        let mut rest = source;
        while let Some(start) = rest.find(marker) {
            let after = &rest[start + marker.len()..];
            let Some(end) = after.find('"') else {
                break;
            };
            keys.insert(after[..end].to_string());
            rest = &after[end + 1..];
        }
    }
    keys
}

fn is_env_key(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
}

fn should_skip(relative: &Path) -> bool {
    let first = relative
        .components()
        .next()
        .and_then(|component| component.as_os_str().to_str());
    matches!(first, Some("arch-tests" | "examples" | "target"))
}

fn strip_inline_test_modules(content: &str) -> String {
    let mut output = String::new();
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
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_test_cfg = true;
            continue;
        }

        // 只认 `mod tests` 会漏掉任何自定义命名的测试模块（本仓常按内容命名，
        // 例如 `mod quic_port_probe_tests`），于是测试代码被当成生产代码报违规。
        // 判据应是「带 #[cfg(test)] 的任意 mod」——模块叫什么与它是不是测试无关。
        if pending_test_cfg && (trimmed.starts_with("mod ") || trimmed.starts_with("pub mod ")) {
            skipping_test_module = true;
            brace_depth = brace_delta(line);
            if brace_depth <= 0 {
                brace_depth = 1;
            }
            pending_test_cfg = false;
            continue;
        }

        pending_test_cfg = false;
        output.push_str(line);
        output.push('\n');
    }

    output
}

fn brace_delta(line: &str) -> isize {
    line.chars().filter(|ch| *ch == '{').count() as isize
        - line.chars().filter(|ch| *ch == '}').count() as isize
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_sources(root, &mut files);
    files
}

fn collect_rust_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if path.is_dir() {
            if matches!(name.as_ref(), "target" | ".git") {
                continue;
            }
            collect_rust_sources(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
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
