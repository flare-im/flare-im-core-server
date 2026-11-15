//! Offline delivery domain contracts.

use async_trait::async_trait;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_proto::PushTaskEnvelope;
use flare_proto::common::PushDisposition;
use flare_server_core::{ErrorCode, FlareError};

/// Delivery port owned by the offline push use case, not by its MQ transport adapter.
#[async_trait]
pub trait OfflinePushExecutor: Send + Sync {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError>;
}

/// The disposition the router decided for this task. An unset value, or one this build does
/// not know, is not an executable task.
pub fn task_disposition(envelope: &PushTaskEnvelope) -> Option<PushDisposition> {
    PushDisposition::try_from(envelope.disposition)
        .ok()
        .filter(|disposition| *disposition != PushDisposition::Unspecified)
}

/// An offline task targets exactly one device and says what the notification must do there.
/// A task missing either is rejected with a terminal error: falling back to "every device of
/// the user, ringing" is precisely the behaviour per-device planning exists to remove.
pub fn validate_offline_task(envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
    if envelope.device_id.trim().is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "offline push task has no target device",
        ));
    }
    if task_disposition(envelope).is_none() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "offline push task has no disposition",
        ));
    }
    Ok(())
}

#[async_trait]
pub trait DeviceTokenRepository: Send + Sync {
    /// Recheck the authority before vendor IO, not just a cached list.
    async fn is_current(&self, token: &DevicePushToken) -> Result<bool, FlareError>;

    /// Push bindings of one device: at most one per vendor channel. There is deliberately no
    /// per-user listing; a task never reaches a device it was not planned for. A binding its
    /// device has not been active on for the inactivity window is absent (and removed).
    async fn list_device_tokens(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError>;

    async fn remove_device_token(
        &self,
        ctx: &Ctx,
        token: &DevicePushToken,
    ) -> Result<(), FlareError>;

    /// Accepted receipts suppress an already-accepted binding when the task is redelivered.
    /// They do not prove display/delivery and cannot eliminate the vendor-ACK/receipt crash
    /// window.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn task(device_id: &str, disposition: i32) -> PushTaskEnvelope {
        PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: device_id.into(),
            disposition,
            ..Default::default()
        }
    }

    #[test]
    fn a_task_needs_a_target_device_and_a_known_disposition() {
        for disposition in [
            PushDisposition::Loud,
            PushDisposition::Quiet,
            PushDisposition::Clear,
            PushDisposition::BadgeOnly,
            PushDisposition::RecallReplace,
            PushDisposition::Digest,
        ] {
            let envelope = task("device-a", disposition as i32);
            assert!(validate_offline_task(&envelope).is_ok());
            assert_eq!(task_disposition(&envelope), Some(disposition));
        }
        for (device_id, disposition) in [
            ("", PushDisposition::Loud as i32),
            ("   ", PushDisposition::Loud as i32),
            ("device-a", PushDisposition::Unspecified as i32),
            ("device-a", 9_999),
        ] {
            let error = validate_offline_task(&task(device_id, disposition)).unwrap_err();
            // Only a terminal error reaches the DLQ; a retryable one would be redelivered up
            // to the cap for a task no retry can repair.
            assert!(!error.is_retryable());
        }
    }
}
