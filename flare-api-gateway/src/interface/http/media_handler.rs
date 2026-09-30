pub(crate) mod files;
pub(crate) mod identity;
pub(crate) mod processing;
pub(crate) mod references;
pub(crate) mod uploads;

pub use files::{delete_file, get_file_info, get_file_url, serve_file};
pub use processing::{process_image, process_video};
pub use references::{create_reference, delete_reference, list_references};
pub use uploads::{
    abort_direct_upload, abort_multipart_upload, commit_direct_upload_parts,
    complete_direct_upload, complete_multipart_upload, generate_upload_url,
    get_direct_upload_status, initiate_direct_upload, initiate_multipart_upload,
    presign_direct_upload_parts, upload_file, upload_multipart_chunk,
};

/// Media identifiers must be scoped by token claims, never an unbound caller header.
pub async fn require_media_identity(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let valid = request
        .extensions()
        .get::<flare_server_core::AuthenticatedPrincipal>()
        .is_some_and(|principal| {
            !principal.user_id.trim().is_empty()
                && principal.tenant_id.as_deref().is_some_and(|tenant| {
                    !tenant.is_empty() && tenant.trim() == tenant && tenant != "*"
                })
        });
    if !valid {
        return flare_im_service_kit::gateway_auth::auth_error_response(
            flare_server_core::AuthError::Forbidden(
                "media requires a tenant-bound user identity".to_string(),
            ),
            "api-gateway",
        );
    }
    next.run(request).await
}
