use flare_im_hooks::hooks::adapters::DefaultHookFactory;
use flare_im_hooks::hooks::hook_context_data::{HookContextData, set_hook_context_data};
use flare_im_hooks::{Ctx, HookConfigLoader, HookRegistry};
use flare_server_core::context::Context;
use std::path::PathBuf;
use std::sync::Arc;

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

struct TemporaryProfiles(PathBuf);

impl TemporaryProfiles {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("flare-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn write(&self, content: &str) -> PathBuf {
        let path = self.0.join("hooks.toml");
        std::fs::write(&path, content).unwrap();
        path
    }
}

impl Drop for TemporaryProfiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn explicit_empty_directory_is_not_an_empty_core_profile() {
    let profiles = TemporaryProfiles::new();
    assert!(
        HookConfigLoader::new()
            .add_candidate(&profiles.0)
            .load()
            .is_err()
    );
    // A consciously configured empty core profile remains valid, including overlays.
    profiles.write("# explicit core profile\n");
    assert!(
        HookConfigLoader::new()
            .add_candidate(&profiles.0)
            .load()
            .unwrap()
            .pre_send
            .is_empty()
    );
}

#[test]
fn unknown_policy_fields_are_rejected_at_every_level() {
    let profiles = TemporaryProfiles::new();
    for content in [
        "[[pre_sned]]\nname='typo'",
        "[[pre_send]]\nrequire_sucess=true",
        "[[pre_send]]\n[pre_send.selector]\ntenant=['tenant-a']",
        "[[pre_send]]\n[pre_send.transport]\ntype='grpc'\nendpoint='http://localhost'\nmetdata={}",
    ] {
        let path = profiles.write(content);
        assert!(HookConfigLoader::new().add_candidate(path).load().is_err());
    }
}

#[test]
fn invalid_config_never_echoes_secret_source_lines() {
    let profiles = TemporaryProfiles::new();
    let secret = "private-hook-token-not-for-logs";
    let path = profiles.write(&format!("[[pre_send]]\nenabled='{secret}'"));
    let error = HookConfigLoader::new()
        .add_candidate(path)
        .load()
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains(secret));
}

#[test]
fn all_shipped_hook_profiles_follow_the_strict_contract() {
    for name in [
        "hooks.core.toml",
        "hooks.social.toml",
        "hooks.toml",
        "hooks.example.toml",
        "hooks.social.example.toml",
        "hooks.business.example.toml",
    ] {
        HookConfigLoader::new()
            .add_candidate(profile(name))
            .load()
            .unwrap();
    }
}

#[tokio::test]
async fn social_profile_installs_one_mandatory_pre_send_for_all_chat_tenants() {
    let config = HookConfigLoader::new()
        .add_candidate(profile("hooks.social.toml"))
        .load()
        .unwrap();
    let registry = HookRegistry::new();
    config
        .install(registry.clone(), &DefaultHookFactory::new().unwrap())
        .await
        .unwrap();
    for tenant in ["0", "tenant-a", "tenant-b"] {
        for conversation_type in ["single", "single_chat", "direct", "group", "group_chat"] {
            let context = Context::with_request_id("social-profile-install").with_tenant_id(tenant);
            let ctx: Ctx = Arc::new(set_hook_context_data(
                context,
                HookContextData::new().with_conversation_type(conversation_type),
            ));
            let plans = registry.plan_pre_send(&ctx).await;
            assert_eq!(plans.len(), 1, "{tenant}/{conversation_type}");
            assert_eq!(plans[0].metadata().name.as_ref(), "social-policy-pre-send");
            assert!(plans[0].metadata().require_success);
        }
    }
}
