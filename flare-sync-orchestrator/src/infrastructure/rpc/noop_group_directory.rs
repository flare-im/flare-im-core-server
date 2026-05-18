//! 默认群目录同步：未接线外部实现时返回空增量（核心不依赖社交服务）。

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_server_core::error::FlareError;
use prost_types::Timestamp;

use crate::application::ports::{GroupDirectoryPage, GroupDirectorySyncPort};

#[derive(Clone, Copy, Default)]
pub struct NoopGroupDirectorySync;

#[async_trait]
impl GroupDirectorySyncPort for NoopGroupDirectorySync {
    async fn sync_group_directory(
        &self,
        _ctx: &Ctx,
        since_version: u64,
        _since_updated_at: Option<Timestamp>,
        _limit: i32,
    ) -> Result<GroupDirectoryPage, FlareError> {
        Ok(GroupDirectoryPage {
            server_version: since_version,
            ..Default::default()
        })
    }
}
