use flare_im_hooks::HookConfigLoader;
use std::path::PathBuf;

fn profile(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .join(name)
}

#[test]
fn unspecified_config_uses_explicit_empty_core_profile() {
    let cfg = HookConfigLoader::new().load().unwrap();
    assert!(cfg.pre_send.is_empty());
    assert!(cfg.post_send.is_empty());
    assert!(cfg.delivery.is_empty());
    assert!(cfg.recall.is_empty());
}

#[test]
fn explicit_missing_path_is_never_silently_ignored() {
    let missing = std::env::temp_dir().join(format!("flare-missing-{}", uuid::Uuid::new_v4()));
    assert!(
        HookConfigLoader::new()
            .add_candidate(missing)
            .load()
            .is_err()
    );
}

#[test]
fn invalid_toml_is_never_replaced_with_empty_profile() {
    // Existing source file exercises a parse error without temporary filesystem mutations.
    let cfg = HookConfigLoader::new().add_candidate(profile("../crates/flare-im-hooks/src/lib.rs"));
    assert!(cfg.load().is_err());
}

#[test]
fn social_profile_is_not_shadowed_by_default_paths() {
    let cfg = HookConfigLoader::new()
        .add_candidate(profile("hooks.social.toml"))
        .load()
        .unwrap();
    assert_eq!(cfg.pre_send.len(), 1);
}

#[test]
fn missing_overlay_fails_even_when_base_profile_is_valid() {
    let missing = std::env::temp_dir().join(format!("flare-missing-{}", uuid::Uuid::new_v4()));
    assert!(
        HookConfigLoader::new()
            .add_candidate(profile("hooks.core.toml"))
            .add_candidate(missing)
            .load()
            .is_err()
    );
}
