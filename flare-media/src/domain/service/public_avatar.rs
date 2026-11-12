use super::*;
use crate::domain::model::{AdminImageUploadPolicy, PUBLIC_AVATAR_METADATA_KEY};
impl MediaService {
    pub(super) fn user_upload_purpose(
        metadata: &HashMap<String, String>,
        namespace: Option<&str>,
        tag: Option<&str>,
    ) -> Result<MediaPurpose> {
        match metadata
            .get("purpose")
            .map(String::as_str)
            .unwrap_or("general")
        {
            "general" => {
                if metadata.get("access_type").is_some_and(|v| v != "private") {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::InvalidParameter,
                        "public access requires explicit avatar purpose"
                    ));
                }
                Ok(MediaPurpose::General)
            }
            "avatar" if namespace == Some("profile") && tag == Some("avatar") => {
                Ok(MediaPurpose::Avatar)
            }
            _ => Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "unsupported user media purpose"
            )),
        }
    }
    pub(super) fn public_avatar_policy(
        &self,
        mime: &str,
        size: i64,
    ) -> Result<AdminImageUploadPolicy> {
        let policy = self.config.admin_image_upload.clone();
        policy.validate().map_err(|message| {
            flare_server_core::flare_err!(ErrorCode::ConfigurationError, message)
        })?;
        if size <= 0
            || size as u64 > policy.max_bytes
            || !policy
                .allowed_mime_types
                .iter()
                .any(|allowed| allowed == mime)
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "avatar violates image policy"
            ));
        }
        let stable_base = self
            .object_repo
            .as_ref()
            .and_then(|repo| repo.cdn_base_url().or_else(|| repo.base_url()));
        if self.metadata_store.is_none()
            || stable_base
                .as_deref()
                .is_none_or(|base| !is_absolute_http_url(base))
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ConfigurationError,
                "public avatar object storage and metadata required"
            ));
        }
        Ok(policy)
    }
    pub(super) async fn validate_public_avatar_payload(
        &self,
        context: &mut UploadContext<'_>,
        purpose: MediaPurpose,
    ) -> Result<()> {
        // This internal marker never trusts upload metadata.
        context.metadata.remove(PUBLIC_AVATAR_METADATA_KEY);
        if purpose != MediaPurpose::Avatar {
            return Ok(());
        }
        let policy = self.public_avatar_policy(context.mime_type, context.file_size)?;
        if context.payload.len() as i64 != context.file_size {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "avatar size mismatch"
            ));
        }
        let bytes = context.payload.to_vec();
        let mime = context.mime_type.to_owned();
        tokio::task::spawn_blocking(move || {
            super::admin_upload::validate_image_bytes(&bytes, &mime, &policy)
        })
        .await
        .map_err(|_| {
            flare_server_core::flare_err!(
                ErrorCode::InternalError,
                "avatar validation worker failed"
            )
        })??;
        context
            .metadata
            .insert(PUBLIC_AVATAR_METADATA_KEY.into(), "true".into());
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn avatar_publication_is_explicit_and_never_a_chat_attachment_flag() {
        let public = HashMap::from([("purpose".into(), "avatar".into())]);
        assert_eq!(
            MediaService::user_upload_purpose(&public, Some("profile"), Some("avatar")).unwrap(),
            MediaPurpose::Avatar
        );
        assert!(
            MediaService::user_upload_purpose(&public, Some("im.message"), Some("chat_attachment"))
                .is_err()
        );
        assert!(
            MediaService::user_upload_purpose(
                &HashMap::from([("access_type".into(), "public".into())]),
                None,
                None
            )
            .is_err()
        );
        assert_eq!(
            MediaService::user_upload_purpose(&HashMap::new(), None, None).unwrap(),
            MediaPurpose::General
        );
    }
}
