//! 插件实体绑定的登记端口：插件卡片（`flare.plugin.message.v1`）落库后，把卡片载荷里的实体 ID
//! 绑定到这条消息，插件之后才能为该实体发布共享状态（`EVENT_MESSAGE_PLUGIN_STATE`）。

use std::future::Future;
use std::pin::Pin;

use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

use crate::domain::model::Message;

pub trait PluginEntityBindingRepository: Send + Sync {
    /// 为 `messages`（同一租户、已落库）里的插件卡片登记绑定，返回登记的条数。不是插件卡片、
    /// 插件在租户内未启用、贡献没有声明 `entityPath` 或载荷里取不到合法实体的消息都跳过；
    /// 重复登记幂等。
    fn register<'a>(
        &'a self,
        ctx: &'a Ctx,
        tenant_id: &'a str,
        messages: &'a [Message],
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>>;
}
