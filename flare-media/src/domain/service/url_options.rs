use super::*;
use flare_im_contracts::media_response::MediaResponseHeaders;

impl MediaService {
    /// Apply response options only after the existing User/Staff and tenant
    /// authorization path succeeds. URL generation is stateless and counted by
    /// that path once; the replacement signature adds no second usage event.
    pub async fn create_file_url_with_options(
        &self,
        ctx: &Context,
        file_id: &str,
        expires_in: i64,
        message: Option<(&str, i64)>,
        download: bool,
        headers: &MediaResponseHeaders,
    ) -> Result<PresignedUrl> {
        let approved = match message {
            Some((conversation_id, sequence)) => {
                self.create_message_presigned_url(
                    ctx,
                    file_id,
                    expires_in,
                    conversation_id,
                    sequence,
                )
                .await?
            }
            None => self.create_presigned_url(ctx, file_id, expires_in).await?,
        };
        if !download && headers.is_empty() {
            return Ok(approved);
        }
        let metadata = self.load_scoped_metadata(ctx, file_id).await?;
        headers.validate_content_type(&metadata.mime_type)?;
        let headers = if download {
            headers.clone().with_download(&metadata.file_name)?
        } else {
            headers.clone()
        };
        let (url, expires_at) = if metadata.storage_bucket().is_some() {
            let repository = self.object_repo.as_ref().ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "signed media storage unavailable"
                )
            })?;
            // Preserve the original message/retention TTL, including Staff's
            // short inspection window, and never let an override extend it.
            let now = Utc::now();
            let ttl = (approved.expires_at - now)
                .num_seconds()
                .clamp(1, 7 * 24 * 3600);
            let url = repository
                .presign_object_with_response_headers(
                    metadata.storage_path().unwrap_or(file_id),
                    ttl,
                    &headers,
                )
                .await?;
            if !is_absolute_http_url(&url) {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "signed media URL unavailable"
                ));
            }
            (url, (now + Duration::seconds(ttl)).min(approved.expires_at))
        } else {
            let mut url = url::Url::parse(&approved.url).map_err(|_| {
                flare_server_core::flare_err!(
                    ErrorCode::ConfigurationError,
                    "absolute local media URL required"
                )
            })?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::ConfigurationError,
                    "HTTP local media URL required"
                ));
            }
            let retained: Vec<(String, String)> = url
                .query_pairs()
                .into_owned()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "download" | "response-content-type" | "response-cache-control"
                    )
                })
                .collect();
            url.set_query(None);
            {
                let mut query = url.query_pairs_mut();
                query.extend_pairs(retained);
                if download {
                    query.append_pair("download", "true");
                }
                if let Some(value) = &headers.content_type {
                    query.append_pair("response-content-type", value);
                }
                if let Some(value) = &headers.cache_control {
                    query.append_pair("response-cache-control", value);
                }
            }
            (url.to_string(), approved.expires_at)
        };
        Ok(PresignedUrl {
            url,
            cdn_url: String::new(),
            expires_at,
        })
    }
}
