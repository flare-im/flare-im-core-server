use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn production_env_vars_are_registered() {
    let root = workspace_root();
    let registry = fs::read_to_string(root.join("crates/flare-im-service-kit/src/env_registry.rs"))
        .expect("read env registry");
    let registered = registered_keys(&registry);

    let sources = rust_sources(&root);
    let test_modules = test_module_files(&sources);
    let mut violations = Vec::new();
    for file in sources {
        let relative = file.strip_prefix(&root).unwrap_or(&file);
        if should_skip(relative)
            || is_cargo_integration_test(&root, &file)
            || is_test_module_file(&test_modules, &file)
        {
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

/// Cargo 集成测试（`<crate>/tests/` 下的文件）只编进测试目标，不是生产代码，与内联的
/// `#[cfg(test)]` 模块同样豁免。
fn is_cargo_integration_test(root: &Path, file: &Path) -> bool {
    file.ancestors()
        .skip(1)
        .take_while(|dir| dir.starts_with(root) && *dir != root)
        .any(|dir| {
            dir.file_name().is_some_and(|name| name == "tests")
                && dir
                    .parent()
                    .is_some_and(|crate_dir| crate_dir.join("Cargo.toml").is_file())
        })
}

/// 所有源文件里以 `#[cfg(test)]` 声明的文件模块（`#[cfg(test)] mod x;`，可带
/// `#[path = "..."]`）解析出的文件路径。它们与内联的测试模块一样是测试代码。
fn test_module_files(sources: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut modules = BTreeSet::new();
    for file in sources {
        let Ok(content) = fs::read_to_string(file) else {
            continue;
        };
        modules.extend(declared_test_modules(file, &content));
    }
    modules
}

/// 文件本身是测试模块，或位于测试模块的子模块目录下（`x.rs` 的 `x/`、`x/mod.rs` 所在目录）。
fn is_test_module_file(test_modules: &BTreeSet<PathBuf>, file: &Path) -> bool {
    test_modules.iter().any(|module| {
        if file == module {
            return true;
        }
        let module_dir = if module.file_name().is_some_and(|name| name == "mod.rs") {
            module.parent().map(Path::to_path_buf)
        } else {
            Some(module.with_extension(""))
        };
        module_dir.is_some_and(|dir| file.starts_with(dir))
    })
}

/// 解析一个源文件里 `#[cfg(test)]`（其后可隔若干属性行）声明的 `mod x;`，按 Rust 的模块文件
/// 规则给出文件路径：有 `#[path]` 时相对本文件所在目录；否则 `lib.rs`、`main.rs`、`mod.rs`
/// 里的子模块在同目录，其余文件 `a/b.rs` 的子模块在 `a/b/` 下，取 `x.rs` 或 `x/mod.rs` 中存在的那个。
fn declared_test_modules(file: &Path, content: &str) -> Vec<PathBuf> {
    let Some(dir) = file.parent() else {
        return Vec::new();
    };
    let is_mod_rs = file
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| matches!(name, "lib.rs" | "main.rs" | "mod.rs"));
    let module_dir = if is_mod_rs {
        dir.to_path_buf()
    } else {
        file.with_extension("")
    };

    let mut modules = Vec::new();
    let mut pending_test_cfg = false;
    let mut path_attr: Option<String> = None;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#[cfg(test)]") {
            pending_test_cfg = true;
            path_attr = None;
            continue;
        }
        if pending_test_cfg && trimmed.starts_with("#[") {
            if let Some(rest) = trimmed.strip_prefix("#[path") {
                path_attr = rest.split('"').nth(1).map(str::to_string);
            }
            continue;
        }
        if pending_test_cfg
            && let Some(name) = trimmed
                .strip_prefix("pub mod ")
                .or_else(|| trimmed.strip_prefix("mod "))
                .and_then(|rest| rest.strip_suffix(';'))
        {
            let name = name.trim();
            match path_attr.take() {
                Some(path) => modules.push(dir.join(path)),
                None => {
                    let flat = module_dir.join(format!("{name}.rs"));
                    let nested = module_dir.join(name).join("mod.rs");
                    modules.push(if nested.is_file() && !flat.is_file() {
                        nested
                    } else {
                        flat
                    });
                }
            }
        }
        pending_test_cfg = false;
        path_attr = None;
    }
    modules
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

#[test]
fn file_test_modules_and_integration_tests_are_not_production_code() {
    let root = workspace_root();

    let main = root.join("tools/example/src/main.rs");
    assert_eq!(
        declared_test_modules(&main, "#[cfg(test)]\nmod tests;\n"),
        vec![root.join("tools/example/src/tests.rs")]
    );
    let router = root.join("flare-api-gateway/src/interface/http/router.rs");
    assert_eq!(
        declared_test_modules(
            &router,
            "#[cfg(test)]\n#[path = \"router_media_tests.rs\"]\nmod media_tests;\n"
        ),
        vec![root.join("flare-api-gateway/src/interface/http/router_media_tests.rs")]
    );
    let handler = root.join("svc/src/handler.rs");
    assert_eq!(
        declared_test_modules(&handler, "#[cfg(test)]\nmod helpers;\n"),
        vec![root.join("svc/src/handler/helpers.rs")]
    );
    // 不带 `#[cfg(test)]` 的文件模块与内联的测试模块不算。
    assert!(declared_test_modules(&main, "mod config;\n#[cfg(test)]\nmod tests {\n}\n").is_empty());

    let modules = BTreeSet::from([root.join("svc/src/handler/helpers.rs")]);
    assert!(is_test_module_file(
        &modules,
        &root.join("svc/src/handler/helpers.rs")
    ));
    assert!(is_test_module_file(
        &modules,
        &root.join("svc/src/handler/helpers/fixtures.rs")
    ));
    assert!(!is_test_module_file(
        &modules,
        &root.join("svc/src/handler.rs")
    ));

    assert!(is_cargo_integration_test(
        &root,
        &root.join("tools/flare-supervise/tests/process_lifecycle.rs")
    ));
    assert!(!is_cargo_integration_test(
        &root,
        &root.join("tools/flare-supervise/src/main.rs")
    ));
    // 名为 tests 但旁边没有 Cargo.toml 的目录不是集成测试目录。
    assert!(!is_cargo_integration_test(
        &root,
        &root.join("flare-api-gateway/src/tests/fixtures.rs")
    ));
}
