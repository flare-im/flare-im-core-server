//! 处理领域事件命令（使用领域 Event，与 proto 解耦）

use crate::domain::model::Event;

/// 处理领域事件命令（撤回、编辑、删除、已读、反应、置顶、标记等）
#[derive(Debug, Clone)]
pub struct ProcessEventCommand {
    pub event: Event,
}
