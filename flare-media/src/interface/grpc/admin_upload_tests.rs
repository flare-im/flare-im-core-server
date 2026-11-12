//! Real signed gRPC boundary and domain orchestration; object bytes stay in memory.
use super::*;
use crate::domain::{
    model::{MediaOwnerKind, MediaPurpose, ObjectStat, UploadContext},
    repository::MediaObjectRepository,
};
use flare_server_core::error::{ErrorCode, Result as DomainResult};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct Objects {
    bytes: Mutex<HashMap<String, Vec<u8>>>,
    fail_sign: AtomicBool,
    write_gate: Option<Arc<WriteGate>>,
}
#[derive(Default)]
struct WriteGate {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl MediaObjectRepository for Objects {
    async fn put_validated_object(&self, key: &str, _: &str, bytes: &[u8]) -> DomainResult<()> {
        self.bytes
            .lock()
            .unwrap()
            .insert(key.to_owned(), bytes.to_vec());
        if let Some(gate) = &self.write_gate {
            gate.entered.notify_one();
            gate.resume.notified().await;
        }
        Ok(())
    }
    async fn put_object(&self, ctx: &UploadContext<'_>) -> DomainResult<String> {
        let key = self.build_object_key_for(
            ctx.tenant_id,
            ctx.file_id,
            ctx.file_name,
            &ctx.file_category,
        );
        self.bytes
            .lock()
            .unwrap()
            .insert(key.clone(), ctx.payload.to_vec());
        Ok(key)
    }
    async fn delete_object(&self, key: &str) -> DomainResult<()> {
        self.bytes.lock().unwrap().remove(key);
        Ok(())
    }
    async fn presign_object(&self, key: &str, ttl: i64) -> DomainResult<String> {
        if self.fail_sign.load(Ordering::SeqCst) {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ServiceUnavailable,
                "signing unavailable"
            ));
        }
        Ok(format!(
            "https://storage.invalid/{key}?signature=signed&ttl={ttl}"
        ))
    }
    async fn presign_put_object(&self, key: &str, _: &str, _: i64, _: i64) -> DomainResult<String> {
        Ok(format!("https://storage.invalid/{key}?signature=put"))
    }
    async fn stat_object(&self, key: &str) -> DomainResult<Option<ObjectStat>> {
        Ok(self.bytes.lock().unwrap().get(key).map(|bytes| ObjectStat {
            size: Some(bytes.len() as i64),
            etag: None,
        }))
    }
    async fn read_object_bounded(&self, key: &str, limit: u64) -> DomainResult<Vec<u8>> {
        let bytes = self.bytes.lock().unwrap().get(key).unwrap().clone();
        if bytes.len() as u64 > limit {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "too large"
            ));
        }
        Ok(bytes)
    }
    fn base_url(&self) -> Option<String> {
        Some("https://unsigned.invalid".into())
    }
    fn cdn_base_url(&self) -> Option<String> {
        Some("https://unsigned-cdn.invalid".into())
    }
    fn use_presigned_urls(&self) -> bool {
        false
    }
    fn bucket_name(&self) -> Option<String> {
        Some("images".into())
    }
}

fn image() -> Vec<u8> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgba8(2, 2)
        .write_to(&mut bytes, image::ImageOutputFormat::Png)
        .unwrap();
    bytes.into_inner()
}
fn staff(h: &Harness, tenant: &str, actor: &str, permission: &str) -> String {
    h.signer
        .clone()
        .with_kid("gateway")
        .unwrap()
        .sign(
            &SignedContextIdentity::new(SignedActorType::Staff, actor, tenant, "image-upload")
                .with_staff_grant(None, Some(permission.into()), 1),
        )
        .unwrap()
}
fn init(bytes: &[u8]) -> flare_grpc_proto::media::AdminInitiateDirectUploadRequest {
    flare_grpc_proto::media::AdminInitiateDirectUploadRequest {
        purpose: 1,
        file_name: "头像.png".into(),
        mime_type: "image/png".into(),
        file_size: bytes.len() as i64,
    }
}

#[tokio::test]
async fn staff_image_upload_seals_bytes_and_fences_principals_and_tenants() {
    let objects = Arc::new(Objects::default());
    let h = serve_with(Some(objects.clone()), vec!["validator".into()]).await;
    let mut client = MediaServiceClient::new(h.channel.clone());
    let write = staff(&h, "tenant-a", "owner", "core:medias:write");
    let read = staff(&h, "tenant-a", "another-staff", "core:medias:read");
    let user = token(&h.signer, SignedActorType::User, "tenant-a", "owner");
    let bytes = image();
    assert_eq!(
        client
            .admin_initiate_direct_upload(raw(init(&bytes), Some(&user), "tenant-a", "owner"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .admin_initiate_direct_upload(raw(init(&bytes), Some(&read), "tenant-a", "owner"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let policy = client
        .get_admin_upload_policy(raw(
            flare_grpc_proto::media::AdminUploadPolicyRequest { purpose: 1 },
            Some(&write),
            "spoofed",
            "spoofed",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(policy.tenant_id, "tenant-a");
    assert_eq!(policy.max_bytes, 8 * 1024 * 1024);
    let session = client
        .admin_initiate_direct_upload(raw(init(&bytes), Some(&write), "spoofed", "spoofed"))
        .await
        .unwrap()
        .into_inner();
    assert!(session.object_key.starts_with("staging/"));
    for denied in [
        user.clone(),
        staff(&h, "tenant-a", "another-staff", "core:medias:write"),
        staff(&h, "tenant-b", "owner", "core:medias:write"),
    ] {
        assert!(
            client
                .get_direct_upload_status(raw(
                    GetDirectUploadStatusRequest {
                        upload_id: session.upload_id.clone()
                    },
                    Some(&denied),
                    "tenant-a",
                    "owner"
                ))
                .await
                .is_err()
        );
    }
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(session.object_key.clone(), bytes.clone());
    let completed = client
        .complete_direct_upload(raw(
            CompleteDirectUploadRequest {
                upload_id: session.upload_id.clone(),
            },
            Some(&write),
            "spoofed",
            "spoofed",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(completed.url.is_empty() && completed.cdn_url.is_empty());
    assert_eq!(completed.info.unwrap().owner_kind, 2);
    let metadata = h
        .metadata
        .load_metadata(
            &Context::root().with_tenant_id("tenant-a"),
            &session.file_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.owner_kind, MediaOwnerKind::Application);
    assert_eq!(metadata.owner_id.as_deref(), Some("tenant-a"));
    assert_eq!(metadata.purpose, MediaPurpose::Avatar);
    let final_key = metadata.storage_path.as_ref().unwrap();
    assert_ne!(final_key, &session.object_key);
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(session.object_key, b"changed after completion".to_vec());
    assert_eq!(
        objects.bytes.lock().unwrap().get(final_key).unwrap(),
        &bytes
    );
    let access = client
        .get_file_url(raw(
            GetFileUrlRequest {
                file_id: session.file_id.clone(),
                expires_in: 99999,
                ..Default::default()
            },
            Some(&user),
            "tenant-a",
            "owner",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(access.url.contains("ttl=3600"));
    assert!(access.cdn_url.is_empty());
    assert!(
        client
            .get_file_url(raw(
                GetFileUrlRequest {
                    file_id: session.file_id.clone(),
                    ..Default::default()
                },
                Some(&read),
                "tenant-a",
                "owner"
            ))
            .await
            .unwrap()
            .get_ref()
            .url
            .contains("ttl=300")
    );
    let foreign = token(&h.signer, SignedActorType::User, "tenant-b", "owner");
    assert_eq!(
        client
            .get_file_url(raw(
                GetFileUrlRequest {
                    file_id: session.file_id.clone(),
                    ..Default::default()
                },
                Some(&foreign),
                "tenant-a",
                "owner"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    objects.fail_sign.store(true, Ordering::SeqCst);
    assert!(
        client
            .get_file_url(raw(
                GetFileUrlRequest {
                    file_id: session.file_id,
                    ..Default::default()
                },
                Some(&user),
                "tenant-a",
                "owner"
            ))
            .await
            .is_err(),
        "never fall back to unsigned/CDN paths"
    );
}

#[tokio::test]
async fn invalid_image_never_registers_a_file_and_can_be_aborted() {
    let objects = Arc::new(Objects::default());
    let h = serve_with(Some(objects.clone()), Vec::new()).await;
    let mut client = MediaServiceClient::new(h.channel.clone());
    let write = staff(&h, "tenant-a", "owner", "core:medias:write");
    let bytes = b"not png despite declared content type";
    let session = client
        .admin_initiate_direct_upload(raw(init(bytes), Some(&write), "tenant-a", "owner"))
        .await
        .unwrap()
        .into_inner();
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(session.object_key.clone(), bytes.to_vec());
    assert_eq!(
        client
            .complete_direct_upload(raw(
                CompleteDirectUploadRequest {
                    upload_id: session.upload_id.clone()
                },
                Some(&write),
                "tenant-a",
                "owner"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert!(
        h.metadata
            .load_metadata(
                &Context::root().with_tenant_id("tenant-a"),
                &session.file_id
            )
            .await
            .unwrap()
            .is_none()
    );
    client
        .abort_direct_upload(raw(
            AbortDirectUploadRequest {
                upload_id: session.upload_id,
            },
            Some(&write),
            "tenant-a",
            "owner",
        ))
        .await
        .unwrap();
    assert!(
        !objects
            .bytes
            .lock()
            .unwrap()
            .contains_key(&session.object_key)
    );
}

#[tokio::test]
async fn abort_cannot_delete_a_final_object_while_completion_commits() {
    let gate = Arc::new(WriteGate::default());
    let objects = Arc::new(Objects {
        write_gate: Some(gate.clone()),
        ..Default::default()
    });
    let h = serve_with(Some(objects.clone()), Vec::new()).await;
    let mut client = MediaServiceClient::new(h.channel.clone());
    let write = staff(&h, "tenant-a", "owner", "core:medias:write");
    let bytes = image();
    let session = client
        .admin_initiate_direct_upload(raw(init(&bytes), Some(&write), "tenant-a", "owner"))
        .await
        .unwrap()
        .into_inner();
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(session.object_key.clone(), bytes);
    let mut completing = client.clone();
    let request = raw(
        CompleteDirectUploadRequest {
            upload_id: session.upload_id.clone(),
        },
        Some(&write),
        "tenant-a",
        "owner",
    );
    let completion = tokio::spawn(async move { completing.complete_direct_upload(request).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        client
            .abort_direct_upload(raw(
                AbortDirectUploadRequest {
                    upload_id: session.upload_id
                },
                Some(&write),
                "tenant-a",
                "owner"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    gate.resume.notify_one();
    completion.await.unwrap().unwrap();
    let asset = h
        .metadata
        .load_metadata(
            &Context::root().with_tenant_id("tenant-a"),
            &session.file_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        objects
            .bytes
            .lock()
            .unwrap()
            .contains_key(asset.storage_path.as_ref().unwrap())
    );
}

#[tokio::test]
async fn application_validator_is_explicit_narrow_and_tenant_bound() {
    let objects = Arc::new(Objects::default());
    let h = serve_with(Some(objects), vec!["validator".into()]).await;
    let app = Context::root().with_tenant_id("tenant-a");
    let mut metadata = MediaFileMetadata {
        file_id: "avatar".into(),
        owner_kind: MediaOwnerKind::Application,
        owner_id: Some("tenant-a".into()),
        purpose: MediaPurpose::Avatar,
        status: MediaAssetStatus::Active,
        mime_type: "image/png".into(),
        file_size: 10,
        storage_bucket: Some("images".into()),
        storage_path: Some("tenant-a/avatar".into()),
        ..Default::default()
    };
    h.metadata.save_metadata(&app, &metadata).await.unwrap();
    let mut client = MediaServiceClient::new(h.channel.clone());
    let allowed = token(&h.signer, SignedActorType::Service, "tenant-a", "validator");
    let request = || flare_grpc_proto::media::ValidateApplicationMediaRequest {
        file_id: "avatar".into(),
        purpose: 1,
    };
    assert_eq!(
        client
            .validate_application_media(raw(request(), Some(&allowed), "spoofed", "spoofed"))
            .await
            .unwrap()
            .get_ref()
            .file_id,
        "avatar"
    );
    let resolved = client
        .resolve_application_avatar_media(raw(
            flare_grpc_proto::media::ResolveApplicationAvatarMediaRequest {
                file_id: "avatar".into(),
                expires_in: 99999,
            },
            Some(&allowed),
            "spoofed",
            "spoofed",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(resolved.url.contains("ttl=300") && resolved.cdn_url.is_empty());
    assert_eq!(
        client
            .get_file_info(raw(
                GetFileInfoRequest {
                    file_id: "avatar".into()
                },
                Some(&allowed),
                "tenant-a",
                "validator"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    for (tenant, actor, code) in [
        ("tenant-a", "unconfigured", Code::PermissionDenied),
        ("tenant-b", "validator", Code::NotFound),
    ] {
        let denied = token(&h.signer, SignedActorType::Service, tenant, actor);
        assert_eq!(
            client
                .validate_application_media(raw(request(), Some(&denied), "tenant-a", "validator"))
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    metadata.owner_kind = MediaOwnerKind::User;
    metadata.owner_id = Some("validator".into());
    h.metadata.save_metadata(&app, &metadata).await.unwrap();
    assert_eq!(
        client
            .validate_application_media(raw(request(), Some(&allowed), "tenant-a", "validator"))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let empty = serve().await;
    let denied = token(
        &empty.signer,
        SignedActorType::Service,
        "tenant-a",
        "validator",
    );
    assert_eq!(
        MediaServiceClient::new(empty.channel.clone())
            .validate_application_media(raw(request(), Some(&denied), "tenant-a", "validator"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
async fn user_avatar_publication_seals_validated_bytes_and_returns_a_stable_url() {
    let objects = Arc::new(Objects::default());
    let h = serve_with(Some(objects.clone()), Vec::new()).await;
    let mut client = MediaServiceClient::new(h.channel.clone());
    let owner = token(&h.signer, SignedActorType::User, "tenant-a", "owner");
    let stranger = token(&h.signer, SignedActorType::User, "tenant-a", "stranger");
    let bytes = image();
    let request = InitiateDirectUploadRequest {
        metadata: Some(UploadFileMetadata {
            file_name: "avatar.png".into(),
            mime_type: "image/png".into(),
            file_size: bytes.len() as i64,
            namespace: "profile".into(),
            business_tag: "avatar".into(),
            metadata: HashMap::from([("purpose".into(), "avatar".into())]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let session = client
        .initiate_direct_upload(raw(request.clone(), Some(&owner), "spoofed", "spoofed"))
        .await
        .unwrap()
        .into_inner();
    assert!(session.object_key.starts_with("staging/avatars/"));
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(session.object_key.clone(), bytes.clone());
    let complete = client
        .complete_direct_upload(raw(
            CompleteDirectUploadRequest {
                upload_id: session.upload_id.clone(),
            },
            Some(&owner),
            "tenant-a",
            "owner",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(complete.url.contains("/avatars/"));
    assert!(!complete.url.contains('?'));
    let metadata = h
        .metadata
        .load_metadata(
            &Context::root().with_tenant_id("tenant-a"),
            &session.file_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.owner_kind, MediaOwnerKind::User);
    assert_eq!(metadata.purpose, MediaPurpose::Avatar);
    assert!(matches!(
        metadata.access_type,
        crate::domain::model::FileAccessType::Public
    ));
    let final_key = metadata.storage_path.as_ref().unwrap();
    assert!(final_key.starts_with("avatars/"));
    assert_eq!(
        objects.bytes.lock().unwrap().get(final_key).unwrap(),
        &bytes
    );
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(session.object_key.clone(), b"changed staging".to_vec());
    assert_eq!(
        objects.bytes.lock().unwrap().get(final_key).unwrap(),
        &bytes
    );
    let mut urls = Vec::new();
    for expiry in [5, 7200] {
        urls.push(
            client
                .get_file_url(raw(
                    GetFileUrlRequest {
                        file_id: session.file_id.clone(),
                        expires_in: expiry,
                        ..Default::default()
                    },
                    Some(&stranger),
                    "tenant-a",
                    "stranger",
                ))
                .await
                .unwrap()
                .into_inner()
                .url,
        );
    }
    assert_eq!(urls[0], urls[1]);
    assert!(!urls[0].contains("signature"));
    let mut invalid = request;
    invalid.metadata.as_mut().unwrap().namespace = "im.message".into();
    assert_eq!(
        client
            .initiate_direct_upload(raw(invalid, Some(&owner), "tenant-a", "owner"))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let fresh = client
        .initiate_direct_upload(raw(
            InitiateDirectUploadRequest {
                metadata: Some(UploadFileMetadata {
                    file_name: "bad.png".into(),
                    mime_type: "image/png".into(),
                    file_size: bytes.len() as i64,
                    namespace: "profile".into(),
                    business_tag: "avatar".into(),
                    metadata: HashMap::from([("purpose".into(), "avatar".into())]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Some(&owner),
            "tenant-a",
            "owner",
        ))
        .await
        .unwrap()
        .into_inner();
    objects
        .bytes
        .lock()
        .unwrap()
        .insert(fresh.object_key.clone(), vec![0; bytes.len()]);
    assert!(
        client
            .complete_direct_upload(raw(
                CompleteDirectUploadRequest {
                    upload_id: fresh.upload_id
                },
                Some(&owner),
                "tenant-a",
                "owner"
            ))
            .await
            .is_err()
    );
    assert!(
        h.metadata
            .load_metadata(&Context::root().with_tenant_id("tenant-a"), &fresh.file_id)
            .await
            .unwrap()
            .is_none()
    );
}
