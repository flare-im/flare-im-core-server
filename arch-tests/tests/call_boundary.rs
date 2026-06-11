use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn call_lifecycle_is_owned_by_flare_call_not_conversation() {
    let root = workspace_root();

    for required in [
        "flare-call/src/domain/call_session.rs",
        "flare-call/src/domain/event.rs",
        "flare-call/src/domain/repository.rs",
        "flare-call/src/application/call/start_call_handler.rs",
    ] {
        assert!(
            root.join(required).is_file(),
            "call lifecycle owner file is missing: {required}"
        );
    }

    let conversation_src = root.join("flare-conversation/src");
    let forbidden = [
        "CallSession",
        "CallSessionEvent",
        "CallSessionRepository",
        "CallSessionState",
        "domain::call",
        "pub mod call;",
    ];
    let mut violations = Vec::new();
    collect_rs_files(&conversation_src, &mut violations, &forbidden);

    assert!(
        violations.is_empty(),
        "flare-conversation must not own call lifecycle code:\n{}",
        violations.join("\n")
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn collect_rs_files(dir: &Path, violations: &mut Vec<String>, forbidden: &[&str]) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();

        if path.is_dir() {
            collect_rs_files(&path, violations, forbidden);
            continue;
        }

        if path.extension().is_some_and(|ext| ext == "rs") {
            let content = fs::read_to_string(&path).expect("read rust source");
            for pattern in forbidden {
                if content.contains(pattern) {
                    violations.push(format!(
                        "{} contains forbidden call lifecycle pattern `{pattern}`",
                        path.strip_prefix(dir).unwrap_or(path.as_path()).display()
                    ));
                }
            }
        }
    }
}
