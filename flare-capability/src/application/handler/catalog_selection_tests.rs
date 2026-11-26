use super::*;
use flare_im_core_capability_core::plugin::manifest::ClientPlatform;
use flare_im_core_capability_core::plugin::ui::{
    BundleRuntime, MAX_UI_DESCRIPTOR_BYTES, engine_vocabulary,
};

fn descriptor(bundle: bool) -> ClientUiDescriptor {
    ClientUiDescriptor::new(
        &engine_vocabulary(),
        ClientPlatform::Web,
        1,
        "web",
        bundle.then(|| BundleRuntime {
            bridge: 1,
            engine: "browser".into(),
            engine_major: 120,
        }),
    )
}

fn is_invalid_argument(error: &CapabilityError) -> bool {
    matches!(error, CapabilityError::Flare(error) if error.code() == Some(ErrorCode::InvalidParameter))
}

fn snapshot(tenant: &str, version: u64) -> PluginRuntimeSnapshot {
    PluginRuntimeSnapshot {
        tenant_id: tenant.into(),
        organization_id: None,
        version,
        issued_at_ms: 0,
        valid_until_ms: 0,
        plugins: vec![],
    }
}

#[test]
fn a_catalog_request_needs_a_valid_descriptor() {
    let valid = serde_json::json!({"descriptor": descriptor(true)});
    assert_eq!(
        read_descriptor(Some(&valid), true).unwrap(),
        descriptor(true)
    );
    for payload in [
        None,
        Some(Value::Null),
        Some(serde_json::json!({})),
        Some(serde_json::json!({"descriptor": null})),
        Some(serde_json::json!({"descriptor": {"vocabulary": 2}})),
    ] {
        let error = read_descriptor(payload.as_ref(), true).unwrap_err();
        assert!(is_invalid_argument(&error), "{payload:?}: {error}");
    }
    // Out of bounds: a channel with a space.
    let mut bad = serde_json::to_value(descriptor(false)).unwrap();
    bad["distribution"] = serde_json::json!("Play Store");
    let error = read_descriptor(Some(&serde_json::json!({"descriptor": bad})), true).unwrap_err();
    assert!(is_invalid_argument(&error));
    // A newer client may say more than this Core reads.
    let mut newer = serde_json::to_value(descriptor(false)).unwrap();
    newer["gestures"] = serde_json::json!(["swipe"]);
    let read = read_descriptor(
        Some(&serde_json::json!({"descriptor": newer, "locale": "en-US"})),
        true,
    )
    .unwrap();
    assert_eq!(read.hash(), descriptor(false).hash());
}

#[test]
fn web_bundles_switched_off_drop_the_bundle_runtime() {
    let payload = serde_json::json!({"descriptor": descriptor(true)});
    let read = read_descriptor(Some(&payload), false).unwrap();
    assert!(read.bundle.is_none());
    assert_eq!(
        read.hash(),
        descriptor(false).hash(),
        "clients differing only in their bundle runtime share one selection"
    );
    assert!(
        read_descriptor(Some(&payload), true)
            .unwrap()
            .bundle
            .is_some()
    );
}

#[test]
fn the_selection_cache_is_keyed_by_tenant_version_and_descriptor_and_bounded() {
    let selection = CatalogSelection::new(true, 2);
    let with = descriptor(true);
    let without = descriptor(false);
    let first = selection.selection("app-a", &snapshot("app-a", 1), &with, &with.hash());
    let again = selection.selection("app-a", &snapshot("app-a", 1), &with, &with.hash());
    assert!(Arc::ptr_eq(&first, &again), "same key, same selection");
    assert_eq!(selection.cached(), 1);
    // Another descriptor and another projection version are other keys.
    let other = selection.selection("app-a", &snapshot("app-a", 1), &without, &without.hash());
    assert!(!Arc::ptr_eq(&first, &other));
    assert_eq!(selection.cached(), 2);
    // Touch the first; a third key evicts the least recently used (the second).
    let _ = selection.selection("app-a", &snapshot("app-a", 1), &with, &with.hash());
    let _ = selection.selection("app-a", &snapshot("app-a", 2), &with, &with.hash());
    assert_eq!(selection.cached(), 2);
    let kept = selection.selection("app-a", &snapshot("app-a", 1), &with, &with.hash());
    assert!(Arc::ptr_eq(&first, &kept), "recently used entry survives");
    let recomputed = selection.selection("app-a", &snapshot("app-a", 1), &without, &without.hash());
    assert!(
        !Arc::ptr_eq(&other, &recomputed),
        "evicted entry is recomputed"
    );
    // Tenants never share an entry.
    let tenant_b = selection.selection("app-b", &snapshot("app-b", 1), &with, &with.hash());
    assert!(!Arc::ptr_eq(&first, &tenant_b));
    assert_eq!(selection.cached(), 2);
}

#[test]
fn one_tenant_cycling_builds_evicts_only_its_own_selections() {
    let selection = CatalogSelection::new(true, SELECTION_CACHE_PER_TENANT * 4);
    let steady = descriptor(false);
    let kept = selection.selection("app-b", &snapshot("app-b", 1), &steady, &steady.hash());
    for build in 0..(SELECTION_CACHE_PER_TENANT as u64 * 3) {
        let mut cycled = descriptor(false);
        cycled.app_build = build + 2;
        let _ = selection.selection("app-a", &snapshot("app-a", 1), &cycled, &cycled.hash());
    }
    assert_eq!(selection.cached(), SELECTION_CACHE_PER_TENANT + 1);
    let again = selection.selection("app-b", &snapshot("app-b", 1), &steady, &steady.hash());
    assert!(
        Arc::ptr_eq(&kept, &again),
        "another tenant's selection survives"
    );
}

#[test]
fn the_descriptor_size_bound_counts_what_was_sent() {
    let mut padded = serde_json::to_value(descriptor(false)).unwrap();
    padded["padding"] = serde_json::json!("x".repeat(MAX_UI_DESCRIPTOR_BYTES));
    let error =
        read_descriptor(Some(&serde_json::json!({"descriptor": padded})), true).unwrap_err();
    assert!(is_invalid_argument(&error));
}

#[test]
fn descriptor_hash_labels_are_bounded() {
    let mut seen = HashSet::new();
    let hash = |i: usize| format!("sha256:{i:012x}{}", "0".repeat(52));
    for i in 0..MAX_DESCRIPTOR_LABELS {
        assert_eq!(
            descriptor_label(&hash(i), &mut seen).len(),
            DESCRIPTOR_LABEL_HEX
        );
    }
    assert_eq!(descriptor_label(&hash(0), &mut seen), "000000000000");
    assert_eq!(
        descriptor_label(&hash(MAX_DESCRIPTOR_LABELS), &mut seen),
        "other"
    );
    assert_eq!(descriptor_label("sha256:short", &mut seen), "other");
    assert_eq!(seen.len(), MAX_DESCRIPTOR_LABELS);
}
