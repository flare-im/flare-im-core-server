//! Offline delivery domain contracts.

use async_trait::async_trait;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_server_core::FlareError;

#[async_trait]
pub trait DeviceTokenRepository: Send + Sync {
    async fn list_user_tokens(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError>;

    async fn remove_device_token(
        &self,
        ctx: &Ctx,
        token: &DevicePushToken,
    ) -> Result<(), FlareError>;
}
