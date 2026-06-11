use std::path::PathBuf;

#[test]
fn development_tls_material_is_not_checked_in() {
    let root = repository_root();
    let forbidden = [
        "certs/server.key",
        "certs/server.crt",
        "flare-core/certs/server.key",
        "flare-core/certs/server.crt",
        "flare-core/examples/certs/server.key",
        "flare-core/examples/certs/server.crt",
        "flare-im-core-sdk/certs/server.key",
        "flare-im-core-sdk/certs/server.crt",
        "flare-im-core-sdk/examples/certs/server.key",
        "flare-im-core-sdk/examples/certs/server.crt",
    ];

    let leaked = forbidden
        .iter()
        .filter(|path| root.join(path).exists())
        .copied()
        .collect::<Vec<_>>();

    assert!(
        leaked.is_empty(),
        "development TLS material must be generated locally, not checked in:\n{}",
        leaked.join("\n")
    );
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("flare-im-core root")
        .parent()
        .expect("repository root")
        .to_path_buf()
}
