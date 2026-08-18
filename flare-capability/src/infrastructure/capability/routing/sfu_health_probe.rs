//! SFU 控制面的健康探针：把媒体面的探活实现从通用健康检查器里搬出来。
//!
//! 通用检查器此前内嵌着 `SfuControlClient` 与 draining 判定。选择依据后来改成了
//! 插件声明的标签，但实现还留在通用路径里 —— 加第二种特殊探活语义的插件时，
//! 仍然只能回到那个文件加分支。现在它由组合根注册，通用侧对它一无所知。

use std::time::Duration;

use async_trait::async_trait;
use flare_grpc_proto::sfu_control::HealthCheckRequest;
use flare_grpc_proto::sfu_control::sfu_control_client::SfuControlClient;
use tonic::Request;

use crate::domain::capability::PluginHealthProbe;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use crate::infrastructure::capability::plugin_contract::HEALTH_PROTOCOL_SFU_CONTROL;

pub struct SfuControlHealthProbe;

#[async_trait]
impl PluginHealthProbe for SfuControlHealthProbe {
    fn protocol(&self) -> &str {
        HEALTH_PROTOCOL_SFU_CONTROL
    }

    async fn probe(&self, grpc_authority: &str, timeout: Duration) -> Result<(), String> {
        let channel = tokio::time::timeout(timeout, resolve_plugin_channel(grpc_authority))
            .await
            .map_err(|_| format!("plugin channel resolve timeout: {grpc_authority}"))?
            .map_err(|e| e.to_string())?;

        let mut client = SfuControlClient::new(channel);
        let response = tokio::time::timeout(
            timeout,
            client.health_check(Request::new(HealthCheckRequest {})),
        )
        .await
        .map_err(|_| "sfu health_check timeout".to_string())?
        .map_err(|e| e.to_string())?
        .into_inner();

        // draining 判为不健康：媒体面要区分「活着」与「活着但正在摘除」。
        // 注意这只让实例被**降权**（排到候选末尾），不是硬排除 —— 全部实例都在
        // draining 时仍可兜底命中，这与「可用性失败降级」一致。
        if response.draining {
            Err("sfu health_check: instance is draining".to_string())
        } else {
            Ok(())
        }
    }
}
