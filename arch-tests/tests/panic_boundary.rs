use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn critical_runtime_paths_do_not_use_unwrap_or_expect_in_production_code() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for source_root in [
        "flare-signaling/gateway/src",
        "flare-storage/writer/src",
        "flare-message-ingest/src",
    ] {
        let files = rust_sources(&root.join(source_root));
        let test_only = test_only_path_modules(&files);
        for file in files {
            if test_only.contains(&file) {
                continue;
            }
            let relative = file.strip_prefix(&root).unwrap_or(&file);
            let content = fs::read_to_string(&file).expect("read rust source");
            let production_content = strip_inline_test_modules(&content);

            for forbidden in [".unwrap()", ".expect("] {
                if production_content.contains(forbidden) {
                    violations.push(format!(
                        "{} contains panic-prone `{forbidden}` in production code",
                        relative.display()
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "critical long-running service paths must return or degrade instead of panicking:\n{}",
        violations.join("\n")
    );
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

        // `#[cfg(test)]` 之后的属性行（如 `#[path = "..."]`）仍属于同一条 mod 声明，
        // 不能把 pending 状态清掉——否则三行形式的声明会漏剥离。
        if pending_test_cfg && trimmed.starts_with("#[") {
            continue;
        }

        // 只认 `mod tests` 会漏掉任何自定义命名的测试模块（例如
        // `mod quic_port_probe_tests`），把测试里的 expect 误判成生产代码违规。
        // 判据改成「带 #[cfg(test)] 的任意 mod」——这才是「这是测试代码」的真判据。
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

/// Files that a `#[cfg(test)] mod name;` declaration pulls in. They sit
/// under `src/` next to production code, but they only ever compile as test code.
/// 两种声明形式都要识别：
/// - 带 `#[path]` 的：路径在属性里，直接解析；
/// - 不带 `#[path]` 的：按 Rust 模块解析规则找 `dir/name.rs` 或 `dir/name/mod.rs`。
///   只认 `#[path]` 会漏掉 `#[cfg(test)] mod tests;` 这种最常见的写法，
///   整个测试文件（如 `static_read_endpoints/tests.rs`）会被当成生产代码误判。
fn test_only_path_modules(files: &[PathBuf]) -> Vec<PathBuf> {
    let mut test_only = Vec::new();
    for file in files {
        let Ok(content) = fs::read_to_string(file) else {
            continue;
        };
        let Some(dir) = file.parent() else {
            continue;
        };
        let mut cfg_test = false;
        let mut path_attr: Option<&str> = None;
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("#[cfg(test)]") {
                cfg_test = true;
                continue;
            }
            if let Some(path) = trimmed
                .strip_prefix("#[path = \"")
                .and_then(|rest| rest.strip_suffix("\"]"))
            {
                path_attr = Some(path);
                continue;
            }
            let declares_module = (trimmed.starts_with("mod ") || trimmed.starts_with("pub mod "))
                && trimmed.ends_with(';');
            if declares_module && cfg_test {
                if let Some(path) = path_attr {
                    test_only.push(dir.join(path));
                } else if let Some(name) = module_declaration_name(trimmed) {
                    for candidate in inline_test_module_paths(dir, file, name) {
                        if candidate.is_file() {
                            test_only.push(candidate);
                        }
                    }
                }
            }
            cfg_test = false;
            path_attr = None;
        }
    }
    test_only
}

/// `mod tests;` / `pub mod tests;` -> `tests`
fn module_declaration_name(line: &str) -> Option<&str> {
    line.trim_start_matches("pub ")
        .strip_prefix("mod ")?
        .strip_suffix(';')?
        .trim()
        .split("::")
        .next()
        .filter(|name| !name.is_empty())
}

/// Rust 2018+ 模块解析规则下，`mod name;` 可能指向的两个文件：
/// - `mod.rs` / `lib.rs` / `main.rs` 中声明：子模块在同级 `name.rs` 或 `name/mod.rs`；
/// - `other_file.rs` 中声明：子模块在 `other_file/name.rs` 或 `other_file/name/mod.rs`。
fn inline_test_module_paths(dir: &Path, declaring_file: &Path, name: &str) -> Vec<PathBuf> {
    let file_name = declaring_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let base_dir = match file_name {
        "mod.rs" | "lib.rs" | "main.rs" => dir.to_path_buf(),
        stem => dir.join(stem.trim_end_matches(".rs")),
    };
    vec![
        base_dir.join(format!("{name}.rs")),
        base_dir.join(name).join("mod.rs"),
    ]
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
            if matches!(name.as_ref(), "target" | ".git" | "tests" | "benches") {
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
