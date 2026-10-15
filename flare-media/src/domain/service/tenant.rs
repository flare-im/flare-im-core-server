use super::*;
use flare_server_core::context::ActorType;

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

/// 可以当作媒资属主的身份：签名上下文里 `actor_type=User` 的 IM 用户，且 `user_id` 就是签名的
/// `actor_id`。签名层也给 TenantAdmin 填 `user_id`（员工 id），员工、服务与系统主体的数字 id
/// 和 IM 用户号段可能重叠，所以只看 `user_id` 会让员工凭同号冒充用户属主。
pub(crate) fn authenticated_user(ctx: &Context) -> Option<&str> {
    let actor = ctx
        .actor()
        .filter(|actor| actor.actor_type == ActorType::User)?;
    ctx.user_id()
        .filter(|user| !user.is_empty() && *user == &*actor.actor_id)
}

pub(crate) fn require_owner(ctx: &Context) -> Result<&str> {
    authenticated_user(ctx).ok_or_else(|| {
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

    pub(super) async fn authorize_file_read(
        &self,
        ctx: &Context,
        metadata: &MediaFileMetadata,
    ) -> Result<()> {
        require_tenant(ctx)?;
        if matches!(metadata.access_type, FileAccessType::Public)
            || metadata
                .owner_id
                .as_deref()
                .is_some_and(|owner| !owner.is_empty() && Some(owner) == authenticated_user(ctx))
        {
            return Ok(());
        }
        if let Some(user) = authenticated_user(ctx)
            && let Some(store) = &self.metadata_store
            && store.has_read_grant(ctx, &metadata.file_id, user).await?
        {
            return Ok(());
        }
        Err(flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            "media access is not authorized"
        ))
    }
}
