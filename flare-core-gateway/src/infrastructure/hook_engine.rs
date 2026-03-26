//! # Hook Engine gRPC客户端
//!
//! 提供Hook Engine的gRPC客户端，用于Gateway代理转发HookService请求

use anyhow::Result;
use flare_proto::hooks::hook_service_client::HookServiceClient;
use flare_proto::hooks::*;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

/// Hook Engine gRPC客户端
pub struct HookEngineClient {
    client: HookServiceClient<Channel>,
}

impl HookEngineClient {
    /// 创建Hook Engine客户端
    pub async fn new(endpoint: String) -> Result<Self> {
        let client = HookServiceClient::connect(endpoint)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Hook Engine: {}", e))?;

        Ok(Self { client })
    }

    /// 创建Hook配置
    pub async fn create_hook_config(
        &mut self,
        request: Request<CreateHookConfigRequest>,
    ) -> Result<Response<CreateHookConfigResponse>, Status> {
        self.client.create_hook_config(request).await
    }

    /// 获取Hook配置
    pub async fn get_hook_config(
        &mut self,
        request: Request<GetHookConfigRequest>,
    ) -> Result<Response<GetHookConfigResponse>, Status> {
        self.client.get_hook_config(request).await
    }

    /// 更新Hook配置
    pub async fn update_hook_config(
        &mut self,
        request: Request<UpdateHookConfigRequest>,
    ) -> Result<Response<UpdateHookConfigResponse>, Status> {
        self.client.update_hook_config(request).await
    }

    /// 列出Hook配置
    pub async fn list_hook_configs(
        &mut self,
        request: Request<ListHookConfigsRequest>,
    ) -> Result<Response<ListHookConfigsResponse>, Status> {
        self.client.list_hook_configs(request).await
    }

    /// 删除Hook配置
    pub async fn delete_hook_config(
        &mut self,
        request: Request<DeleteHookConfigRequest>,
    ) -> Result<Response<DeleteHookConfigResponse>, Status> {
        self.client.delete_hook_config(request).await
    }

    /// 设置Hook状态
    pub async fn set_hook_status(
        &mut self,
        request: Request<SetHookStatusRequest>,
    ) -> Result<Response<SetHookStatusResponse>, Status> {
        self.client.set_hook_status(request).await
    }

    /// 获取Hook统计
    pub async fn get_hook_statistics(
        &mut self,
        request: Request<GetHookStatisticsRequest>,
    ) -> Result<Response<GetHookStatisticsResponse>, Status> {
        self.client.get_hook_statistics(request).await
    }

    /// 查询Hook执行历史
    pub async fn query_hook_executions(
        &mut self,
        request: Request<QueryHookExecutionsRequest>,
    ) -> Result<Response<QueryHookExecutionsResponse>, Status> {
        self.client.query_hook_executions(request).await
    }
}

/// Hook Engine客户端包装器（支持Arc和Send+Sync）
pub struct HookEngineClientWrapper {
    endpoint: String,
}

impl HookEngineClientWrapper {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    /// 创建客户端（每次调用创建新连接）
    pub async fn create_client(&self) -> Result<HookEngineClient> {
        HookEngineClient::new(self.endpoint.clone()).await
    }
}
