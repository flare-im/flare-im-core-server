
use super::*;
use std::collections::HashMap;

fn service() -> MediaService {
    MediaService::new(
        None,
        None,
        None,
        None,
        None,
        None,
        MediaDomainConfig::new(
            3600,
            None,
            60,
            std::env::temp_dir().join("flare-media-service-tests"),
            3600,
            8 * 1024 * 1024,
        ),
    )
}

#[test]
fn default_user_upload_is_external_and_not_reference_managed() {
    let service = service();
    let payload = [];
    let context = UploadContext {
        file_id: "file-1",
        file_name: "avatar.png",
        mime_type: "image/png",
        file_size: 0,
        payload: &payload,
        file_category: "image".to_string(),
        user_id: "user-1",
        trace_id: None,
        namespace: None,
        business_tag: None,
        metadata: HashMap::new(),
    };

    assert!(!MediaService::is_message_lifecycle_context(&context));
    assert!(service.extract_reference_scope(&context).is_none());

    let (reference_count, status, grace_expires_at) =
        MediaService::initial_lifecycle_state(false, true, 60);
    assert_eq!(reference_count, 1);
    assert_eq!(status, MediaAssetStatus::Active);
    assert!(grace_expires_at.is_none());
}

#[test]
fn message_lifecycle_metadata_extracts_reference_scope() {
    let service = service();
    let payload = [];
    let mut metadata = HashMap::new();
    metadata.insert(
        MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string(),
        MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string(),
    );
    let context = UploadContext {
        file_id: "file-1",
        file_name: "message.png",
        mime_type: "image/png",
        file_size: 0,
        payload: &payload,
        file_category: "image".to_string(),
        user_id: "user-1",
        trace_id: None,
        namespace: None,
        business_tag: None,
        metadata,
    };

    let scope = service
        .extract_reference_scope(&context)
        .expect("message media should create a reference scope");
    assert_eq!(scope.namespace, MESSAGE_MEDIA_LIFECYCLE_SCOPE);
    assert_eq!(
        scope.business_tag.as_deref(),
        Some(MESSAGE_MEDIA_LIFECYCLE_SCOPE)
    );

    let ref_id = MediaService::deterministic_reference_id("tenant-a", "file-1", &scope);
    assert_eq!(
        ref_id,
        MediaService::deterministic_reference_id("tenant-a", "file-1", &scope)
    );
    assert_ne!(
        ref_id,
        MediaService::deterministic_reference_id("tenant-b", "file-1", &scope)
    );
}

#[test]
fn zero_reference_lifecycle_archives_message_media_only() {
    let mut external = MediaFileMetadata {
        file_id: "external-file".to_string(),
        status: MediaAssetStatus::Pending,
        reference_count: 0,
        ..Default::default()
    };
    MediaService::stamp_lifecycle_scope(&mut external.metadata, false);
    MediaService::apply_reference_lifecycle(&mut external, 60);
    assert_eq!(external.status, MediaAssetStatus::Active);
    assert!(external.grace_expires_at.is_none());

    let mut message = MediaFileMetadata {
        file_id: "message-file".to_string(),
        status: MediaAssetStatus::Active,
        reference_count: 0,
        ..Default::default()
    };
    MediaService::stamp_lifecycle_scope(&mut message.metadata, true);
    MediaService::apply_reference_lifecycle(&mut message, 60);
    assert_eq!(message.status, MediaAssetStatus::Pending);
    assert!(message.grace_expires_at.is_some());
}
