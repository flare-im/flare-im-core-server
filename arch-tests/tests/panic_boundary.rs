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
        for file in rust_sources(&root.join(source_root)) {
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
