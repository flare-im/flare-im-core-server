use std::fs;
use std::path::PathBuf;

#[test]
fn capability_contracts_are_owned_by_shared_core_crate() {
    let root = workspace_root();
    let contract_root = root.join("crates/flare-im-capability-core/src");

    for required in [
        "error.rs",
        "context.rs",
        "dispatch.rs",
        "extension_operation.rs",
        "grant.rs",
        "ports.rs",
        "recipient.rs",
        "rtc.rs",
    ] {
        assert!(
            contract_root.join(required).is_file(),
            "capability contract file is missing from shared core crate: {required}"
        );
    }

    for facade in [
        "flare-capability/src/domain/capability/error.rs",
        "flare-capability/src/domain/capability/context.rs",
        "flare-capability/src/domain/capability/dispatch.rs",
        "flare-capability/src/domain/capability/extension_operation.rs",
        "flare-capability/src/domain/capability/grant.rs",
        "flare-capability/src/domain/capability/ports.rs",
        "flare-capability/src/domain/capability/recipient.rs",
        "flare-capability/src/domain/capability/rtc.rs",
    ] {
        let content = fs::read_to_string(root.join(facade)).expect("read capability facade");
        assert!(
            content.contains("pub use flare_im_capability_core::"),
            "{facade} must remain a compatibility facade over flare-im-capability-core"
        );
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
