use super::*;
use crate::domain::model::AdminImageUploadPolicy;
use flare_server_core::{SignedActorType, VerifiedContext};

impl MediaService {
    pub fn admin_upload_policy(
        &self,
        ctx: &Context,
        purpose: MediaPurpose,
    ) -> Result<AdminImageUploadPolicy> {
        require_tenant(ctx)?;
        tenant::staff_actor(
            ctx,
            flare_server_core::auth::core_admin::permission::MEDIAS_WRITE,
        )?;
        if !purpose.is_application_image() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "application image purpose required"
            ));
        }
        self.config
            .admin_image_upload
            .validate()
            .map_err(|message| {
                flare_server_core::flare_err!(ErrorCode::ConfigurationError, message)
            })?;
        Ok(self.config.admin_image_upload.clone())
    }

    pub async fn initiate_admin_direct_upload(
        &self,
        ctx: &Context,
        purpose: MediaPurpose,
        file_name: &str,
        mime_type: &str,
        file_size: i64,
    ) -> Result<DirectUploadSessionState> {
        let policy = self.admin_upload_policy(ctx, purpose)?;
        if self.metadata_store.is_none() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "persistent image metadata required"
            ));
        }
        if file_name.trim().is_empty()
            || file_name.chars().count() > 255
            || file_name.chars().any(char::is_control)
            || file_size <= 0
            || file_size as u64 > policy.max_bytes
            || !policy
                .allowed_mime_types
                .iter()
                .any(|allowed| allowed == mime_type)
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "image does not satisfy upload policy"
            ));
        }
        let actor_id = tenant::staff_actor(
            ctx,
            flare_server_core::auth::core_admin::permission::MEDIAS_WRITE,
        )?;
        let metadata = flare_grpc_proto::media::UploadFileMetadata {
            file_name: file_name.to_owned(),
            mime_type: mime_type.to_owned(),
            file_size,
            file_type: flare_grpc_proto::media::FileType::Image as i32,
            namespace: "application".into(),
            business_tag: purpose.as_str().into(),
            ..Default::default()
        };
        self.initiate_direct_upload_as(
            ctx,
            &metadata,
            0,
            None,
            None,
            None,
            actor_id,
            UploadActorKind::Staff,
            purpose,
        )
        .await
    }

    fn require_application_validator(&self, ctx: &Context) -> Result<()> {
        let tenant = require_tenant(ctx)?;
        let valid = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .is_some_and(|claims| {
                claims.actor_type == SignedActorType::Service
                    && claims.tenant_id == tenant
                    && self
                        .config
                        .application_media_validators
                        .iter()
                        .any(|actor| actor == &claims.actor_id)
            });
        if !valid {
            return Err(flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "application media validator denied"
            ));
        }
        Ok(())
    }

    pub async fn validate_application_media(
        &self,
        ctx: &Context,
        file_id: &str,
        purpose: MediaPurpose,
    ) -> Result<MediaFileMetadata> {
        self.require_application_validator(ctx)?;
        if !purpose.is_application_image() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "application image purpose required"
            ));
        }
        let metadata = self.load_scoped_metadata(ctx, file_id).await?;
        if metadata.owner_kind != MediaOwnerKind::Application
            || metadata.owner_id.as_deref() != ctx.tenant_id()
            || metadata.purpose != purpose
            || metadata.status != MediaAssetStatus::Active
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::MessageNotFound,
                "application media not found"
            ));
        }
        Ok(metadata)
    }

    pub async fn resolve_application_avatar_media(
        &self,
        ctx: &Context,
        file_id: &str,
        expires_in: i64,
    ) -> Result<PresignedUrl> {
        let metadata = self
            .validate_application_media(ctx, file_id, MediaPurpose::Avatar)
            .await?;
        self.presign_authorized_metadata(
            ctx,
            &metadata,
            if expires_in > 0 {
                expires_in.min(300)
            } else {
                300
            },
        )
        .await
    }

    pub(super) async fn seal_admin_image_object(
        &self,
        session: &UploadSession,
        object_key: &str,
    ) -> Result<String> {
        if session.actor_kind != UploadActorKind::Staff && session.purpose != MediaPurpose::Avatar {
            return Ok(object_key.to_owned());
        }
        let policy = &self.config.admin_image_upload;
        policy.validate().map_err(|message| {
            flare_server_core::flare_err!(ErrorCode::ConfigurationError, message)
        })?;
        let repository = self.object_repo.as_ref().ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "image object storage required"
            )
        })?;
        let stat = repository.stat_object(object_key).await?.ok_or_else(|| {
            flare_server_core::flare_err!(ErrorCode::InvalidParameter, "image object missing")
        })?;
        let size = stat
            .size
            .filter(|size| *size > 0 && *size as u64 <= policy.max_bytes)
            .ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "image size violates policy"
                )
            })?;
        if Some(size) != session.total_size {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "image size violates policy"
            ));
        }
        let bytes = repository
            .read_object_bounded(object_key, policy.max_bytes)
            .await?;
        if bytes.len() as i64 != size {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "image size changed during validation"
            ));
        }
        let declared_mime = session.mime_type.clone();
        let image_policy = policy.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            validate_image_bytes(&bytes, &declared_mime, &image_policy)?;
            Ok::<_, flare_server_core::error::FlareError>(bytes)
        })
        .await
        .map_err(|error| {
            map_infra_error(
                error,
                ErrorCode::InternalError,
                "image validation worker failed",
            )
        })??;
        // Only server writes to the final key: an unexpired browser PUT can change
        // the staging object, never the validated bytes referenced by a user.
        let final_key = Self::admin_image_final_key(session)?;
        repository
            .put_validated_object(final_key, &session.mime_type, &bytes)
            .await?;
        Ok(final_key.to_owned())
    }

    pub(super) fn admin_image_final_key(session: &UploadSession) -> Result<&str> {
        session
            .object_key
            .as_deref()
            .and_then(|key| key.strip_prefix("staging/"))
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::InternalError,
                    "administrative staging object missing"
                )
            })
    }
}

pub(super) fn validate_image_bytes(
    bytes: &[u8],
    declared_mime: &str,
    policy: &AdminImageUploadPolicy,
) -> Result<()> {
    let invalid = || {
        flare_server_core::flare_err!(ErrorCode::InvalidParameter, "invalid or unsupported image")
    };
    let format = image::guess_format(bytes).map_err(|_| invalid())?;
    let mime = match format {
        image::ImageFormat::Png => "image/png",
        image::ImageFormat::Jpeg => "image/jpeg",
        image::ImageFormat::WebP => "image/webp",
        image::ImageFormat::Gif => "image/gif",
        _ => return Err(invalid()),
    };
    if declared_mime != mime
        || !policy
            .allowed_mime_types
            .iter()
            .any(|allowed| allowed == mime)
    {
        return Err(invalid());
    }
    let mut limits = image::io::Limits::default();
    limits.max_image_width = Some(policy.max_dimension);
    limits.max_image_height = Some(policy.max_dimension);
    // Include bounded compressed-input/decoder overhead so a small pixel policy
    // does not reject a tiny valid image merely for its PNG tables/buffers.
    limits.max_alloc = Some(
        policy
            .max_pixels
            .saturating_mul(16)
            .saturating_add(policy.max_bytes.saturating_mul(2)),
    );
    image_probe::dimensions(bytes, format, limits.clone(), policy)?;
    if format == image::ImageFormat::Gif {
        use image::{AnimationDecoder, ImageDecoder};
        let mut decoder = image::codecs::gif::GifDecoder::new(std::io::Cursor::new(bytes))
            .map_err(|_| invalid())?;
        decoder.set_limits(limits).map_err(|_| invalid())?;
        for frame in decoder.into_frames() {
            frame.map_err(|_| invalid())?;
        }
        return Ok(());
    }
    let mut reader = image::io::Reader::with_format(std::io::Cursor::new(bytes), format);
    reader.limits(limits);
    reader.decode().map_err(|_| invalid())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(width, height)
            .write_to(&mut bytes, image::ImageOutputFormat::Png)
            .unwrap();
        bytes.into_inner()
    }

    #[test]
    fn image_validation_checks_actual_content_dimensions_and_complete_decode() {
        let policy = AdminImageUploadPolicy::default();
        let bytes = png(3, 2);
        validate_image_bytes(&bytes, "image/png", &policy).unwrap();
        assert!(validate_image_bytes(&bytes, "image/jpeg", &policy).is_err());
        assert!(validate_image_bytes(b"not an image", "image/png", &policy).is_err());
        assert!(validate_image_bytes(&bytes[..bytes.len() / 2], "image/png", &policy).is_err());
        assert!(
            validate_image_bytes(
                &bytes,
                "image/png",
                &AdminImageUploadPolicy {
                    max_dimension: 2,
                    ..policy.clone()
                }
            )
            .is_err()
        );
        assert!(
            validate_image_bytes(
                &bytes,
                "image/png",
                &AdminImageUploadPolicy {
                    max_pixels: 5,
                    ..policy.clone()
                }
            )
            .is_err()
        );
        assert!(
            validate_image_bytes(
                &bytes,
                "image/png",
                &AdminImageUploadPolicy {
                    allowed_mime_types: vec!["image/jpeg".into()],
                    ..policy
                }
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_configuration_fails_closed() {
        let defaults = AdminImageUploadPolicy::default();
        defaults.validate().unwrap();
        assert!(
            AdminImageUploadPolicy {
                max_bytes: 0,
                ..defaults.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AdminImageUploadPolicy {
                max_pixels: 0,
                ..defaults.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AdminImageUploadPolicy {
                max_dimension: 0,
                ..defaults.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AdminImageUploadPolicy {
                allowed_mime_types: vec!["image/svg+xml".into()],
                ..defaults
            }
            .validate()
            .is_err()
        );
    }
}
