use std::fs;
use std::path::PathBuf;

#[test]
fn extracted_core_crates_have_runtime_owners() {
    let root = workspace_root();
    let runtime_manifests = [
        "flare-api-gateway/Cargo.toml",
        "flare-admin-gateway/Cargo.toml",
        "flare-signaling/gateway/Cargo.toml",
        "flare-signaling/online/Cargo.toml",
        "flare-signaling/route/Cargo.toml",
        "flare-push/proxy/Cargo.toml",
        "flare-push/server/Cargo.toml",
        "flare-push/worker/Cargo.toml",
        "flare-storage/writer/Cargo.toml",
        "flare-storage/reader/Cargo.toml",
        "flare-conversation/Cargo.toml",
        "flare-sync-orchestrator/Cargo.toml",
        "flare-message-ingest/Cargo.toml",
        "flare-orchestrator/Cargo.toml",
        "flare-capability/Cargo.toml",
        "flare-media/Cargo.toml",
    ];

    let manifests = runtime_manifests
        .iter()
        .map(|path| {
            let content = fs::read_to_string(root.join(path)).expect("read manifest");
            (*path, content)
        })
        .collect::<Vec<_>>();

    for (crate_name, expected_owner_hint) in [
        ("flare-call", "flare-signaling/gateway"),
        ("flare-im-capability-core", "flare-capability"),
        ("flare-im-message-pipeline", "flare-message-ingest"),
        ("flare-im-hooks", "flare-message-ingest"),
        ("flare-im-seq", "flare-message-ingest"),
    ] {
        let owners = manifests
            .iter()
            .filter_map(|(path, content)| {
                content
                    .contains(&format!("{crate_name} = {{ workspace = true }}"))
                    .then_some(*path)
            })
            .collect::<Vec<_>>();

        assert!(
            !owners.is_empty(),
            "{crate_name} must be used by at least one runtime service; expected an owner near {expected_owner_hint}"
        );
        assert!(
            owners.iter().any(|path| path.contains(expected_owner_hint)),
            "{crate_name} must keep its intended runtime owner near {expected_owner_hint}; current owners: {owners:?}"
        );
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
