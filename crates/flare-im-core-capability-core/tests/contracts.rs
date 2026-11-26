use flare_im_core_capability_core::{
    CapabilityDispatchResult, CapabilityInvokeMeta, ConversationKind, ResolveTrigger,
};
use serde_json::json;

#[test]
fn dispatch_result_keeps_success_and_failure_shapes_stable() {
    let ok = CapabilityDispatchResult::ok(
        "req-1",
        "plugin-1",
        "example.poll.vote",
        json!({"room": "r1"}),
    );
    assert!(ok.success);
    assert_eq!(ok.request_id, "req-1");
    assert_eq!(ok.plugin_id, "plugin-1");
    assert_eq!(ok.capability_id, "example.poll.vote");
    assert_eq!(ok.data["room"], "r1");
    assert!(ok.error.is_none());

    let fail = CapabilityDispatchResult::fail("req-2", "plugin-1", "example.poll.vote", "denied");
    assert!(!fail.success);
    assert_eq!(fail.error.as_deref(), Some("denied"));
}

#[test]
fn invocation_context_preserves_business_neutral_extension_fields() {
    let meta = CapabilityInvokeMeta::new("tenant-1", "req-1");

    assert_eq!(meta.tenant_id, "tenant-1");
    assert_eq!(meta.request_id, "req-1");
    assert_eq!(meta.ext, json!({}));
    assert_eq!(
        ConversationKind::Custom("channel".into()),
        ConversationKind::Custom("channel".into())
    );
    assert_eq!(ResolveTrigger::Broadcast, ResolveTrigger::Broadcast);
}
