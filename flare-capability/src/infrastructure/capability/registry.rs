//! 能力扩展注册表（基础设施侧统一容器）。

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::domain::capability::{PreSendGuard, RecipientResolver, RtcCapability};

use super::{PreSendGuardRuntime, RecipientResolverRuntime, RtcCapabilityRouter};

pub struct CapabilityExtensionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

#[derive(Default)]
pub struct RegistryInner {
    pub pre_send: PreSendGuardRuntime,
    pub recipient: RecipientResolverRuntime,
    pub rtc: RtcCapabilityRouter,
}

impl CapabilityExtensionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryInner::default())),
        }
    }

    pub async fn pre_send(&self) -> PreSendGuardRuntime {
        self.inner.read().await.pre_send.clone()
    }

    pub async fn recipient(&self) -> RecipientResolverRuntime {
        self.inner.read().await.recipient.clone()
    }

    pub async fn rtc_router(&self) -> RtcCapabilityRouter {
        self.inner.read().await.rtc.clone()
    }

    pub async fn register_pre_send_guard(&self, guard: Arc<dyn PreSendGuard>) {
        self.inner.read().await.pre_send.register(guard).await;
    }

    pub async fn register_recipient_resolver(&self, resolver: Arc<dyn RecipientResolver>) {
        self.inner.read().await.recipient.register(resolver).await;
    }

    pub async fn set_rtc_backend(&self, rtc: Option<Arc<dyn RtcCapability>>) {
        self.inner.read().await.rtc.set_backend(rtc).await;
    }
}

impl Default for CapabilityExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CapabilityExtensionRegistry {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
