//! ServiceRegistry 适配器
//!
//! 将 `flare_core_transport::discovery::ServiceRegistry` 适配为
//! `flare_core_runtime::registry::ServiceRegistry` trait 实现

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::ServiceRegistry as TransportRegistry;
use flare_core_runtime::error::RegistryError;
use flare_core_runtime::registry::{ServiceInfo, ServiceRegistry as RegistryTrait};

/// ServiceRegistry 适配器
///
/// 将 `flare_core_transport::discovery::ServiceRegistry` 包装为
/// `flare_core_runtime::registry::ServiceRegistry` trait 实现
pub struct ServiceRegistryAdapter {
    inner: TransportRegistry,
    service_info: ServiceInfo,
}

impl ServiceRegistryAdapter {
    /// 创建新的适配器
    pub fn new(registry: TransportRegistry) -> Self {
        // 从 registry 获取服务实例信息
        let instance = registry.instance();
        let service_info = ServiceInfo::new(
            &instance.service_type,
            &instance.instance_id,
            instance.address,
        )
        .with_ttl(Duration::from_secs(30));

        Self {
            inner: registry,
            service_info,
        }
    }

    /// 获取内部 registry
    pub fn into_inner(self) -> TransportRegistry {
        self.inner
    }

    /// 将 TransportRegistry 转换为 Box<dyn RegistryTrait>
    pub fn into_boxed(registry: TransportRegistry) -> Box<dyn RegistryTrait> {
        Box::new(Self::new(registry))
    }
}

impl RegistryTrait for ServiceRegistryAdapter {
    fn register<'a>(
        &'a mut self,
        service: &'a ServiceInfo,
    ) -> Pin<Box<dyn Future<Output = Result<(), RegistryError>> + Send + 'a>> {
        Box::pin(async move {
            // TransportRegistry 的注册已经在构造时完成
            // 这里只需要更新 service_info
            self.service_info = service.clone();
            Ok(())
        })
    }

    fn deregister<'a>(
        &'a mut self,
        service: &'a ServiceInfo,
    ) -> Pin<Box<dyn Future<Output = Result<(), RegistryError>> + Send + 'a>> {
        Box::pin(async move {
            // 调用 shutdown 进行注销
            self.inner
                .shutdown()
                .await
                .map_err(|e| RegistryError::DeregistrationFailed {
                    service_id: service.id.clone(),
                    reason: e.to_string(),
                })?;
            Ok(())
        })
    }

    fn send_heartbeat<'a>(
        &'a mut self,
        service_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), RegistryError>> + Send + 'a>> {
        Box::pin(async move {
            self.inner
                .heartbeat()
                .await
                .map_err(|e| RegistryError::HeartbeatFailed {
                    service_id: service_id.to_string(),
                    reason: e.to_string(),
                })?;
            Ok(())
        })
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = Result<(), RegistryError>> + Send + '_>> {
        let service_id = self.service_info.id.clone();
        Box::pin(async move {
            self.inner
                .shutdown()
                .await
                .map_err(|e| RegistryError::DeregistrationFailed {
                    service_id,
                    reason: e.to_string(),
                })?;
            Ok(())
        })
    }
}

/// 辅助函数：将 Option<TransportRegistry> 转换为 Option<Box<dyn RegistryTrait>>
pub fn adapt_registry(registry: Option<TransportRegistry>) -> Option<Box<dyn RegistryTrait>> {
    registry.map(ServiceRegistryAdapter::into_boxed)
}
