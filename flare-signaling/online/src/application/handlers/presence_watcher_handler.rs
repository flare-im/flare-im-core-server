//! 在线状态监听端口编排（应用层），委托领域端口 `PresenceWatcher`。
//!
//! 接口层仅依赖本模块，不直接调用基础设施适配器。

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::domain::repository::{PresenceChangeEvent, PresenceWatcher};

#[derive(Clone)]
pub struct OnlinePresenceWatcherHandler<PW: PresenceWatcher + Send + Sync> {
    inner: Arc<PW>,
}

impl<PW: PresenceWatcher + Send + Sync> OnlinePresenceWatcherHandler<PW> {
    pub fn new(inner: Arc<PW>) -> Self {
        Self { inner }
    }

    pub async fn watch_presence(
        &self,
        user_ids: &[String],
    ) -> Result<mpsc::Receiver<anyhow::Result<PresenceChangeEvent>>> {
        self.inner.watch_presence(user_ids).await
    }
}
