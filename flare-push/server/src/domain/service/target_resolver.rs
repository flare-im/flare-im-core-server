//! 推送目标解析器领域服务
//!
//! 根据 PushTargetType 解析推送目标设备列表。
//!
//! ## 职责
//! - 全量推送：查询所有在线设备
//! - 用户列表推送：查询指定用户的在线设备
//! - 设备列表推送：直接返回设备列表

use anyhow::Result;
use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::PushEnvelope;

use crate::domain::model::DeviceInfo;

/// 推送目标解析器 Trait
///
/// ## 设计
/// - 领域层接口，无外部依赖
/// - Infrastructure 层提供具体实现
#[async_trait]
pub trait TargetResolver: Send + Sync {
    /// 解析推送目标设备列表
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `envelope`: 推送信封
    ///
    /// # 返回
    /// - `Ok(Vec<DeviceInfo>)`: 目标设备列表
    /// - `Err`: 解析失败
    async fn resolve(&self, ctx: &Ctx, envelope: &PushEnvelope) -> Result<Vec<DeviceInfo>>;
}
