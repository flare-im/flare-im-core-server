//! Offline delivery domain contracts.

use async_trait::async_trait;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_proto::PushTaskEnvelope;
use flare_server_core::FlareError;

/// Delivery port owned by the offline push use case, not by its MQ transport adapter.
#[async_trait]
pub trait OfflinePushExecutor: Send + Sync {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError>;
}

#[async_trait]
pub trait DeviceTokenRepository: Send + Sync {
    /// Recheck the authority before vendor IO, not just a cached list.
    async fn is_current(&self, token: &DevicePushToken) -> Result<bool, FlareError>;
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

    /// Accepted receipts suppress already-accepted devices on a partial retry. They do not
    /// prove display/delivery and cannot eliminate the vendor-ACK/receipt crash window.
    async fn was_accepted(
        &self,
        token: &DevicePushToken,
        task: &PushTaskEnvelope,
    ) -> Result<bool, FlareError>;
    async fn record_accepted(
        &self,
        token: &DevicePushToken,
        task: &PushTaskEnvelope,
    ) -> Result<(), FlareError>;
}
