//! 将媒体控制面后端登记到 [`PluginRouteBook`]，供 `CapabilityService` 客户端发现。

use std::collections::HashMap;

use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::capability::plugin_contract::{
    BACKEND_CLASS_MEDIA_CONTROL, HEALTH_PROTOCOL_SFU_CONTROL, LABEL_BACKEND_CLASS,
    LABEL_HEALTH_PROTOCOL,
};
use flare_grpc_proto::capability::RegisteredPluginInstance;

/// 登记当前已连接的媒体控制面 gRPC 入口。
pub fn register_plugin_route(
    plugin_routes: &PluginRouteBook,
    tenant_id: &str,
    plugin_id: &str,
    capability_id: &str,
    grpc_endpoint: &str,
) {
    let mut labels = HashMap::new();
    labels.insert(
        LABEL_BACKEND_CLASS.to_string(),
        BACKEND_CLASS_MEDIA_CONTROL.to_string(),
    );
    // 媒体面必须声明走 SfuControl 健康协议：通用协议只回答「活着」，
    // 而媒体面要区分「活着」与「活着但正在摘除」——把 draining 中的实例
    // 当成健康实例继续派新呼叫，扩缩容时就会断通话。
    //
    // 这条声明是健康检查器选择协议的**唯一依据**，漏了不会编译报错，
    // 只会静默退化成通用检查。
    labels.insert(
        LABEL_HEALTH_PROTOCOL.to_string(),
        HEALTH_PROTOCOL_SFU_CONTROL.to_string(),
    );

    let instance = RegisteredPluginInstance {
        plugin_id: plugin_id.to_string(),
        capability_id: capability_id.to_string(),
        grpc_authority: grpc_endpoint.to_string(),
        labels,
        // 由内部装配登记的端点同样没有清单：如实标 unverified。
        // 「内部的所以可信」是错觉——可信与否要看有没有可校验的声明，
        // 而不是看谁登记的。
        plugin_version: String::new(),
        api_version: String::new(),
        manifest_sha256: String::new(),
        declared_operations: Vec::new(),
        unverified: true,
    };
    plugin_routes.upsert(tenant_id, instance);
    tracing::info!(
        tenant_id = %tenant_id,
        plugin_id = %plugin_id,
        capability_id = %capability_id,
        endpoint = %grpc_endpoint,
        "registered media control backend in PluginRouteBook"
    );
}

#[cfg(test)]
mod tests {
    use super::register_plugin_route;
    use crate::infrastructure::capability::PluginRouteBook;
    use crate::infrastructure::capability::plugin_contract::{
        BACKEND_CLASS_MEDIA_CONTROL, HEALTH_PROTOCOL_SFU_CONTROL, LABEL_BACKEND_CLASS,
        LABEL_HEALTH_PROTOCOL, MEDIA_CONTROL_CAPABILITY_ID,
    };

    /// 媒体登记必须声明 SfuControl 健康协议。
    ///
    /// 漏声明不会编译报错，只会让健康检查静默退化成通用协议 —— 通用协议只回答
    /// 「活着」，于是 draining 中的实例会被当成健康实例继续派新呼叫，
    /// 扩缩容时断通话。这条断言就是把「漏声明」变成红灯。
    #[test]
    fn media_registration_declares_sfu_health_protocol() {
        let book = PluginRouteBook::new();
        register_plugin_route(
            &book,
            "0",
            "media-control",
            MEDIA_CONTROL_CAPABILITY_ID,
            "127.0.0.1:1",
        );

        let instances = book.list_filtered("0", MEDIA_CONTROL_CAPABILITY_ID);
        assert_eq!(instances.len(), 1, "媒体端点未登记");
        assert_eq!(
            instances[0]
                .labels
                .get(LABEL_HEALTH_PROTOCOL)
                .map(String::as_str),
            Some(HEALTH_PROTOCOL_SFU_CONTROL),
            "媒体端点必须声明 sfu_control 健康协议，否则健康检查会静默退化"
        );
        assert_eq!(
            instances[0]
                .labels
                .get(LABEL_BACKEND_CLASS)
                .map(String::as_str),
            Some(BACKEND_CLASS_MEDIA_CONTROL)
        );
    }
}
