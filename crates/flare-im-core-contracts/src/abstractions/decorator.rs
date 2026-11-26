//! 消息增强装饰器（Decorator 模式）
//!
//! 在消息链上对已读标记、@提及等做增强，不修改核心消息体结构，便于可插拔与测试。
//! 采用「按值传入、返回装饰后的 Message」避免异步与引用生命周期冲突。

use flare_proto::common::Message;

use flare_core_base::error::Result;

/// 消息装饰器端口：对 proto Message 做增强（如已读回执、@提及列表），返回装饰后的消息。
pub trait MessageDecorator: Send + Sync {
    /// 装饰/增强消息（可写 extra、attributes 等），返回同一消息或新实例。
    fn decorate(
        &self,
        message: Message,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Message>> + Send + '_>>;
}

/// 组合多个装饰器，按顺序执行
pub struct MessageDecoratorChain {
    decorators: Vec<Box<dyn MessageDecorator>>,
}

impl MessageDecoratorChain {
    pub fn new(decorators: Vec<Box<dyn MessageDecorator>>) -> Self {
        Self { decorators }
    }

    pub fn add(&mut self, d: Box<dyn MessageDecorator>) {
        self.decorators.push(d);
    }
}

impl MessageDecorator for MessageDecoratorChain {
    fn decorate(
        &self,
        message: Message,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Message>> + Send + '_>> {
        let decorators = &self.decorators;
        Box::pin(async move {
            let mut msg = message;
            for d in decorators.iter() {
                msg = d.decorate(msg).await?;
            }
            Ok(msg)
        })
    }
}

/// 无操作装饰器
pub struct NoopMessageDecorator;

impl MessageDecorator for NoopMessageDecorator {
    fn decorate(
        &self,
        message: Message,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Message>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(message)))
    }
}
