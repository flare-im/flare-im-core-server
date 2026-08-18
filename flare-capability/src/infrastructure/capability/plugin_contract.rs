//! 插件契约里的**约定常量**：标签键、健康协议取值、媒体控制能力 id。
//!
//! # 为什么要单独一个文件
//!
//! 这些字符串此前散落在四处（健康检查、发现配置过滤、静态装配、路由登记），
//! 同一个字面量重复出现。字面量重复的代价不是难看，是**改不动**：
//! 想换个能力 id 或加一种健康协议，得先找全所有出现的地方，而漏掉一处
//! 不会编译报错，只会在运行时表现为「某类插件的健康检查一直失败」。
//!
//! 集中之后，这些约定就有了名字，可以被引用、被搜索、被测试。

/// 标签键：插件用哪种协议做健康检查。
///
/// 缺省（未声明）时走通用协议 —— 这是「声明即边界」在健康检查上的体现：
/// 插件不声明特殊需求，就按通用方式对待，而不是让核心去猜它是什么。
pub const LABEL_HEALTH_PROTOCOL: &str = "health_protocol";

/// 健康协议取值：走 `SfuControl.HealthCheck`（带 draining 语义）。
///
/// 媒体面需要区分「活着」与「活着但正在摘除」——通用协议只回答前者，
/// 而通话中的实例被当成健康实例继续接新呼叫会导致扩缩容时断线。
pub const HEALTH_PROTOCOL_SFU_CONTROL: &str = "sfu_control";

/// 媒体控制面的能力 id。
///
/// 这是**媒体装配路径**自己的知识（等同于领域层 RTC 适配器认识
/// `rtc.call.video`），不是通用分发器的知识。集中在这里是为了消除重复，
/// 不代表核心通用路径可以引用它。
pub const MEDIA_CONTROL_CAPABILITY_ID: &str = "rtc.media.control";

/// 标签键：后端大类，供运维排查与选路观察使用。
pub const LABEL_BACKEND_CLASS: &str = "backend_class";

/// 后端大类取值：媒体控制面。
pub const BACKEND_CLASS_MEDIA_CONTROL: &str = "media_control";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::capability::PluginRouteBook;

    /// 普通插件不声明健康协议 —— 核心据此走通用检查，而不是去猜它是什么。
    #[test]
    fn plain_plugin_declares_no_health_protocol() {
        let book = PluginRouteBook::new();
        book.upsert(
            "0",
            flare_grpc_proto::capability::RegisteredPluginInstance {
                plugin_id: "p1".into(),
                capability_id: "vendorx.do".into(),
                grpc_authority: "127.0.0.1:1".into(),
                unverified: true,
                ..Default::default()
            },
        );

        let instances = book.list_filtered("0", "vendorx.do");
        assert!(
            !instances[0].labels.contains_key(LABEL_HEALTH_PROTOCOL),
            "普通插件不该被塞上媒体面的健康协议"
        );
    }
}
