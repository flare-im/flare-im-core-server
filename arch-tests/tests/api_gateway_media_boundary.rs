use std::fs;
use std::path::PathBuf;

#[test]
fn api_gateway_media_handlers_stay_split_by_adapter_boundary() {
    let root = workspace_root();
    let media_root = root.join("flare-api-gateway/src/interface/http/media_handler");

    for required in ["uploads.rs", "files.rs", "references.rs", "processing.rs"] {
        assert!(
            media_root.join(required).is_file(),
            "media HTTP adapter group is missing: {required}"
        );
    }
    // Object listing, bucket description, ACL and orphan cleanup are admin
    // operations (SEC-05): they must not come back to the user-token API gateway.
    assert!(
        !media_root.join("objects.rs").exists(),
        "object/bucket administration belongs to flare-admin-gateway, not the user API gateway"
    );
    assert!(
        root.join("flare-admin-gateway/src/interface/http/media_handler.rs")
            .is_file(),
        "admin-gateway media administration handler is missing"
    );

    let facade =
        fs::read_to_string(root.join("flare-api-gateway/src/interface/http/media_handler.rs"))
            .expect("read media handler facade");
    assert!(
        facade.lines().count() <= 80,
        "media_handler.rs must remain a thin facade over grouped HTTP adapters"
    );

    for forbidden in [
        "crate::domain::",
        "flare_media::domain",
        "MediaService::new",
        "MediaCommandHandler::new",
        "MediaQueryHandler::new",
    ] {
        assert!(
            !facade.contains(forbidden),
            "media_handler.rs facade must not own media domain/runtime wiring: {forbidden}"
        );
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}
