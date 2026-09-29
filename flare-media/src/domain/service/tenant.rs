use super::*;

pub(crate) fn require_tenant(ctx: &Context) -> Result<&str> {
    let tenant = ctx.tenant_id().ok_or_else(|| {
        flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            "media tenant context is required"
        )
    })?;
    validate_tenant(tenant)
}

pub(crate) fn validate_tenant(tenant: &str) -> Result<&str> {
    if tenant.is_empty() || tenant.trim() != tenant || tenant == "*" {
        return Err(flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            "a concrete media tenant is required"
        ));
    }
    Ok(tenant)
}

pub(crate) fn require_owner(ctx: &Context) -> Result<&str> {
    ctx.user_id()
        .filter(|user| !user.is_empty())
        .ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "authenticated media owner is required"
            )
        })
}

impl MediaService {
    pub(super) fn authorize_file_write(ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
        require_tenant(ctx)?;
        if metadata.owner_id.as_deref() != Some(require_owner(ctx)?) {
            return Err(flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "media mutation requires the authenticated owner"
            ));
        }
        Ok(())
    }
    pub(super) fn authorize_upload_session(ctx: &Context, session: &UploadSession) -> Result<()> {
        if session.tenant_id.is_empty()
            || session.tenant_id != require_tenant(ctx)?
            || session.user_id != require_owner(ctx)?
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::MessageNotFound,
                "upload session not found; restart uploads created before tenant isolation"
            ));
        }
        Ok(())
    }

    pub(super) fn authorize_file_read(ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
        require_tenant(ctx)?;
        if matches!(metadata.access_type, FileAccessType::Public)
            || metadata
                .owner_id
                .as_deref()
                .is_some_and(|owner| !owner.is_empty() && Some(owner) == ctx.user_id())
        {
            return Ok(());
        }
        Err(flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            "media access is not authorized"
        ))
    }
}
