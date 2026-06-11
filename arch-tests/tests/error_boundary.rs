use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn im_core_production_errors_use_server_core_error_foundation() {
    // arch-tests 位于 workspace 根的一级子目录，向上一级扫描全部 crate
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf();
    let mut violations = Vec::new();

    for file in rust_sources(&root) {
        let relative = file.strip_prefix(&root).unwrap_or(&file);
        let content = fs::read_to_string(&file).expect("read rust source");

        for forbidden in [
            "anyhow::Result",
            "anyhow::Error",
            "use anyhow::Result",
            "use anyhow::{",
            "anyhow::anyhow",
            "anyhow!",
            "bail!",
            "thiserror::Error",
            "#[error(",
            "use flare_im_core::error",
            "flare_im_core::error::",
        ] {
            if content.contains(forbidden) {
                violations.push(format!(
                    "{} contains forbidden error pattern `{forbidden}`",
                    relative.display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "IM production code must use flare_server_core::error directly:\n{}",
        violations.join("\n")
    );
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
            if matches!(
                name.as_ref(),
                "target" | ".git" | "tests" | "benches" | "examples"
            ) {
                continue;
            }
            collect_rust_sources(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}
