//! 设备推送绑定（Port）。
//!
//! 离线任务按设备发：一台设备有推送绑定才可能收到推送。绑定由 Push Proxy 登记、
//! Push Worker 投递时读取；路由只需要知道「哪些设备有绑定」，不读令牌本身。

use std::collections::HashMap;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_server_core::error::FlareError;

use crate::domain::model::DeviceProfile;

/// 一个用户有推送绑定的设备，去重后的设备标识。
pub type BoundDevices = Arc<[String]>;

#[async_trait::async_trait]
pub trait DeviceBindingRepository: Send + Sync {
    /// 这批用户各自有推送绑定的设备。没有任何绑定的用户不出现在结果里。
    /// 长期不活跃（设备一直没核对过）的绑定视为不存在。
    ///
    /// 结果允许是短时间内的旧值：绑定很少变化，而这个查询在每条消息的路由路径上。
    /// 旧值的代价是有界的——刚登记的绑定晚一个缓存周期才开始收推送；刚吊销的绑定多出的
    /// 任务会在 Worker 读绑定时落空。
    async fn bound_devices(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, BoundDevices>, FlareError>;

    /// 一个用户有推送绑定的设备，直接读权威数据并刷新缓存。
    ///
    /// 设备交接用它：设备刚转入后台就来交接，这时拿旧值会把刚登记好绑定的设备判成
    /// 「没有绑定」，这次补提示就丢了。交接远比消息稀疏，付得起一次直读。
    async fn bound_devices_fresh(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<BoundDevices, FlareError>;

    /// 这批用户各自有推送绑定的设备，连同提示要用的登记信息（时区、是不是 iOS），按设备标识。
    /// 没有任何绑定的用户不出现在结果里。新鲜度与 [`Self::bound_devices`] 相同。
    ///
    /// 缺省实现只有设备标识：时区未知、不按 iOS 处理。
    async fn device_profiles(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, Vec<DeviceProfile>>, FlareError> {
        let bound = self.bound_devices(ctx, tenant_id, user_ids).await?;
        Ok(bound
            .into_iter()
            .map(|(user_id, devices)| {
                let profiles = devices
                    .iter()
                    .map(|device_id| DeviceProfile::unknown(device_id.as_str()))
                    .collect();
                (user_id, profiles)
            })
            .collect())
    }
}
