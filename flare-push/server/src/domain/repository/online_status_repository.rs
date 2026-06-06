//! 在线状态仓储接口
//!
//! 提供查询设备在线状态的能力。
//!
//! ## 设计
//! - 领域层接口，无外部依赖
//! - Infrastructure 层提供具体实现（查询 Redis）

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_server_core::error::Result;

use crate::domain::model::DeviceInfo;

/// 在线状态仓储 Trait
///
/// ## 职责
/// - 查询设备在线状态
/// - 获取在线设备列表
#[async_trait]
pub trait OnlineStatusRepository: Send + Sync {
    /// 获取所有在线设备
    async fn get_all_online_devices(&self, ctx: &Ctx) -> Result<Vec<DeviceInfo>>;

    /// 获取指定用户的在线设备
    async fn get_devices_by_users(&self, ctx: &Ctx, user_ids: &[String])
    -> Result<Vec<DeviceInfo>>;

    /// 获取指定设备的详情
    async fn get_devices_by_ids(&self, ctx: &Ctx, device_ids: &[String])
    -> Result<Vec<DeviceInfo>>;
}
