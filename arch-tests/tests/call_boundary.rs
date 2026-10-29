//! Calls are a managed plugin (`flare.call`): Core keeps only business-neutral primitives
//! (realtime delivery to conversation members, the catalog-changed notice) and owns no call
//! lifecycle, signalling route or media control. This gate keeps call-specific code from
//! coming back into Core production sources.

use std::fs;
use std::path::{Path, PathBuf};

/// Identifiers that only call or media-control code would use.
const FORBIDDEN: &[&str] = &[
    "flare_call",
    "flare-call",
    "CallSession",
    "call_signal",
    "CallSignal",
    "RtcDispatchRoute",
    "RtcCapability",
    "RtcInvite",
    "rtc_intent",
    "SfuControl",
    "sfu_control",
    "flare-strom-sfu",
    "STROM_SFU",
    "\"rtc.",
];

#[test]
fn core_production_sources_hold_no_call_specific_code() {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);
    assert!(
        files.len() > 200,
        "scan found too few sources; the gate would pass vacuously"
    );

    let mut violations = Vec::new();
    for file in &files {
        let content = fs::read_to_string(file).expect("read rust source");
        for pattern in FORBIDDEN {
            if content.contains(pattern) {
                violations.push(format!(
                    "{} contains call-specific `{pattern}`",
                    file.strip_prefix(&root).unwrap_or(file).display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Core must stay call-agnostic; call logic belongs to the flare.call plugin:\n{}",
        violations.join("\n")
    );
}

#[test]
fn removed_call_crates_and_modules_stay_removed() {
    let root = workspace_root();
    for removed in [
        "crates/flare-call",
        "flare-signaling/gateway/src/call_signal",
        "flare-storage/writer/src/call",
        "crates/flare-im-capability-core/src/rtc.rs",
        "flare-capability/src/domain/capability/rtc.rs",
        "flare-capability/src/infrastructure/capability/internal",
    ] {
        assert!(!root.join(removed).exists(), "{removed} must not exist");
    }
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest");
    assert!(
        !manifest.contains("flare-call"),
        "the workspace must not list a call crate"
    );
}

/// The scanner catches an injected violation, so a broken walk cannot keep the gate green.
#[test]
fn the_scanner_catches_an_injected_violation() {
    let dir = std::env::temp_dir().join(format!("flare-call-gate-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("src")).expect("temp dir");
    fs::create_dir_all(dir.join("tests")).expect("temp dir");
    fs::write(
        dir.join("src/lib.rs"),
        "fn f(id: &str) -> bool { id.starts_with(\"rtc.\") }\n",
    )
    .expect("write source");
    fs::write(dir.join("tests/t.rs"), "const X: &str = \"rtc.call\";\n").expect("write test");
    let mut files = Vec::new();
    collect_rust_sources(&dir, &mut files);
    let _ = fs::remove_dir_all(&dir);
    assert_eq!(
        files.len(),
        1,
        "only production sources are scanned: {files:?}"
    );
    assert!(files[0].ends_with("src/lib.rs"));
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

/// Every `.rs` file under `dir` except `target`, `.git`, `tests`, `benches` and `examples`
/// directories (the walk `error_boundary` uses).
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
