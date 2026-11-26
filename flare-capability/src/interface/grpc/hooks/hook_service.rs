//! Hook 治理应用服务：配置 CRUD、点位目录、部署摘要、测试调用、统计、执行记录查询（由
//! `CapabilityService.Administer` 经 [`super::capability_administer`] 调用）。
//!
//! 范围：`tenant_id` 空是平台 Hook。平台范围的 Staff 上下文签的是 [`PLATFORM_SCOPE_TENANT`]，
//! Administer 已核对过调用方能不能动这个范围。

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, CreateHookConfigResponse, DeleteHookConfigRequest,
    DeleteHookConfigResponse, DeploymentHook, GetHookConfigRequest, GetHookConfigResponse,
    GetHookStatisticsRequest, GetHookStatisticsResponse, HookConfig, HookExecution, HookPointEntry,
    HookProfile, HookSelector, HookStatistics, HookTransport, ListHookConfigsRequest,
    ListHookConfigsResponse, ListHookPointsResponse, ListHookProfilesResponse,
    QueryHookExecutionsRequest, QueryHookExecutionsResponse, SetHookStatusRequest,
    SetHookStatusResponse, TestHookConfigRequest, TestHookConfigResponse, UpdateHookConfigRequest,
    UpdateHookConfigResponse,
};
use flare_im_core_hooks::hooks::execution::execution_hook_id;
use flare_im_core_hooks::hooks::{HOOK_POINTS, HookDefinition, StoredHook, test_call};
use flare_server_core::auth::core_admin::PLATFORM_SCOPE_TENANT;
use flare_server_core::error::Result as FlareResult;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_server_core::utils::require_ctx_from_request;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::application::handler::HookQueryHandler;
use crate::domain::hook_integration::validate_hook_item;
use crate::domain::model::{
    HookConfigItem, HookExecutionFilter, HookExecutionScope, HookExecutionStatistics,
    HookExecutionTimeRange, HookSelectorConfig, HookTransportConfig, LoggableName,
    StoredHookExecution, administrable_hook_type, validate_hook_name,
};
use crate::infrastructure::persistence::postgres_config::{
    HookDeploymentSummaryRow, PostgresHookConfigRepository,
};
use chrono::Utc;
use flare_proto::common::{Pagination, TimeRange};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

/// 调用方的范围：应用 ID，`None` 是平台。
fn extract_tenant_id<T>(request: &Request<T>) -> Option<String> {
    require_ctx_from_request(request)
        .ok()?
        .tenant_id()
        .filter(|tenant| *tenant != PLATFORM_SCOPE_TENANT)
        .map(str::to_string)
}

/// 库里一行的范围：空串和 NULL 都是平台。
fn row_scope(tenant_id: Option<&str>) -> Option<&str> {
    tenant_id.filter(|tenant| !tenant.is_empty())
}

/// Hook 治理（写/读模型入口），非独立 gRPC service。写入提交后 Ingest 约一秒内热加载，这里不执行
/// Hook。
pub struct HookServiceServer {
    repository: Arc<PostgresHookConfigRepository>,
    /// `hook_executions` 的读侧；没有时统计为 0、执行记录为空。
    executions: Option<Arc<HookQueryHandler>>,
}

impl HookServiceServer {
    pub fn new(repository: Arc<PostgresHookConfigRepository>) -> Self {
        Self {
            repository,
            executions: None,
        }
    }

    /// 统计与执行记录从 `hook_executions` 读（见 [`HookQueryHandler`]）。
    pub fn with_executions(mut self, executions: Arc<HookQueryHandler>) -> Self {
        self.executions = Some(executions);
        self
    }

    /// 按 hook_id（数字 id 或 `hook_type:name`）找到调用方范围里的这条 Hook，返回统计与查询的范围。
    /// 平台范围还能按名字查部署 Hook：它们不在 `hook_configs` 里，执行记录按名字归属。
    async fn execution_scope(
        &self,
        tenant_id: Option<&str>,
        hook_id: &str,
    ) -> Result<HookExecutionScope, Status> {
        if hook_id.is_empty() {
            return Err(Status::invalid_argument("hook_id is required"));
        }
        let by_name = if hook_id.parse::<i64>().is_ok() {
            None
        } else {
            Some(hook_id.split_once(':').ok_or_else(|| {
                Status::invalid_argument(
                    "Invalid hook_id format, expected numeric id or 'hook_type:name'",
                )
            })?)
        };
        let row = match (hook_id.parse::<i64>(), by_name) {
            (Ok(id), _) => self.repository.get_by_id(id).await,
            (_, Some((hook_type, name))) => {
                self.repository
                    .get_by_name(tenant_id, hook_type, name)
                    .await
            }
            _ => unreachable!("a hook id is numeric or split above"),
        }
        .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
        .map(|(row, _)| row);
        let Some(row) = row else {
            if let (None, Some((hook_type, name))) = (tenant_id, by_name)
                && self
                    .repository
                    .global_name_exists(hook_type, name)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to check hook name: {}", e)))?
            {
                return Ok(HookExecutionScope {
                    tenant_id: None,
                    hook_type: hook_type.to_string(),
                    hook_name: name.to_string(),
                    hook_id: execution_hook_id(hook_type, name, None),
                });
            }
            return Err(Status::not_found("Hook config not found"));
        };
        if row_scope(row.tenant_id.as_deref()) != tenant_id {
            return Err(Status::permission_denied("hook belongs to another tenant"));
        }
        Ok(HookExecutionScope {
            tenant_id: tenant_id.map(str::to_string),
            hook_id: execution_hook_id(&row.hook_type, &row.name, Some(row.id)),
            hook_type: row.hook_type,
            hook_name: row.name,
        })
    }

    /// 不能和平台或部署 Hook 同类型同名：执行记录与统计按名字归属，同名会串。停用的平台行也占着
    /// 名字。
    async fn reject_global_name(&self, hook_type: &str, name: &str) -> Result<(), Status> {
        let reserved = self
            .repository
            .global_name_exists(hook_type, name)
            .await
            .map_err(|e| Status::internal(format!("Failed to check hook name: {}", e)))?;
        if reserved {
            return Err(Status::already_exists(format!(
                "hook name '{}' is reserved by a global {hook_type} hook",
                LoggableName(name)
            )));
        }
        Ok(())
    }

    pub async fn grpc_create_hook_config(
        &self,
        request: Request<CreateHookConfigRequest>,
    ) -> Result<Response<CreateHookConfigResponse>, Status> {
        let ctx = require_ctx_from_request(&request)
            .map_err(|_| Status::internal("Context not found"))?;
        let req = request.into_inner();

        // 应用 Hook 只能指向公网 HTTPS，要出站校验、出网开关和数量上限。这些没有之前建不出来。
        if !req.tenant_id.is_empty() {
            return Err(Status::failed_precondition(APPLICATION_HOOKS_UNAVAILABLE));
        }

        // 验证必需字段
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        // 名称每次执行都进日志：控制字符与超长名称在登记时就拒绝，错误里不带取值。
        validate_hook_name(&req.name).map_err(Status::invalid_argument)?;
        if req.hook_type.is_empty() {
            return Err(Status::invalid_argument("hook_type is required"));
        }

        // 只登记目录里的点位（见 `administrable_hook_type`）。
        administrable_hook_type(&req.hook_type).map_err(Status::invalid_argument)?;

        if req.transport.is_none() {
            return Err(Status::invalid_argument("transport is required"));
        }

        // 转换protobuf类型到内部类型
        let hook_item = protobuf_to_hook_config_item(&req, None)
            .map_err(|e| Status::invalid_argument(format!("Invalid hook config: {}", e)))?;
        // 存进去却加载不了的行会让 Ingest 里这个应用的配置停在上一份。
        validate_hook_item(&hook_item)
            .map_err(|e| Status::invalid_argument(format!("Invalid hook config: {}", e)))?;
        self.reject_global_name(&req.hook_type, &hook_item.name)
            .await?;

        // 保存到数据库（优先从 Context 提取，其次从请求参数）
        let created_by = ctx.user_id().map(|s: &str| s).or(None);

        let hook_id = self
            .repository
            .save(None, &req.hook_type, &hook_item, created_by)
            .await
            .map_err(|e| Status::internal(format!("Failed to save hook config: {}", e)))?;

        // 构建响应
        let hook_config =
            hook_config_item_to_protobuf(&hook_id.to_string(), "", &req.hook_type, &hook_item)
                .map_err(|e| Status::internal(format!("Failed to convert hook config: {}", e)))?;

        Ok(Response::new(CreateHookConfigResponse {
            config: Some(hook_config),
        }))
    }

    pub async fn grpc_get_hook_config(
        &self,
        request: Request<GetHookConfigRequest>,
    ) -> Result<Response<GetHookConfigResponse>, Status> {
        // 先提取租户ID（在into_inner()之前）
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();

        if req.hook_id.is_empty() {
            return Err(Status::invalid_argument("hook_id is required"));
        }

        // 解析hook_id（格式：hook_type:name 或 id）
        // 先尝试作为数字ID解析
        let hook_id_parsed = req.hook_id.parse::<i64>();

        let (row, hook_item) = if let Ok(id) = hook_id_parsed {
            // 作为数字ID查询
            self.repository
                .get_by_id(id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?
        } else {
            // 作为hook_type:name格式解析
            let parts: Vec<&str> = req.hook_id.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Status::invalid_argument(
                    "Invalid hook_id format, expected numeric id or 'hook_type:name'",
                ));
            }

            let hook_type = parts[0];
            let name = parts[1];

            self.repository
                .get_by_name(tenant_id.as_deref(), hook_type, name)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?
        };

        // 转换为protobuf类型
        let hook_config = hook_config_item_to_protobuf(
            &row.id.to_string(),
            row.tenant_id.as_deref().unwrap_or(""),
            &row.hook_type,
            &hook_item,
        )
        .map_err(|e| Status::internal(format!("Failed to convert hook config: {}", e)))?;

        Ok(Response::new(GetHookConfigResponse {
            config: Some(hook_config),
        }))
    }

    pub async fn grpc_update_hook_config(
        &self,
        request: Request<UpdateHookConfigRequest>,
    ) -> Result<Response<UpdateHookConfigResponse>, Status> {
        // 先提取租户ID（在into_inner()之前）
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();

        if req.hook_id.is_empty() {
            return Err(Status::invalid_argument("hook_id is required"));
        }
        // 改名与创建同一规则，查库之前就拒绝。不改名的更新照常：规则之前存下的名称仍可改其余字段。
        if !req.name.is_empty() {
            validate_hook_name(&req.name).map_err(Status::invalid_argument)?;
        }

        // 解析hook_id（格式：hook_type:name 或 id）
        let hook_id_parsed = req.hook_id.parse::<i64>();

        let (row, mut hook_item) = if let Ok(id) = hook_id_parsed {
            // 作为数字ID查询
            self.repository
                .get_by_id(id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?
        } else {
            // 作为hook_type:name格式解析
            let parts: Vec<&str> = req.hook_id.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Status::invalid_argument(
                    "Invalid hook_id format, expected numeric id or 'hook_type:name'",
                ));
            }

            let hook_type = parts[0];
            let name = parts[1];

            self.repository
                .get_by_name(tenant_id.as_deref(), hook_type, name)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?
        };

        // 合并更新字段
        if !req.name.is_empty() {
            hook_item.name = req.name.clone();
        }
        if let Some(order) = req.order {
            hook_item.order = order;
        }
        if let Some(required) = req.required {
            hook_item.require_success = required;
        }
        if let Some(ref transport) = req.transport {
            hook_item.transport = updated_transport(transport, &hook_item.transport)
                .map_err(Status::invalid_argument)?;
            hook_item.timeout_ms = transport.timeout_ms as u64;
        }
        if let Some(ref selector) = req.selector {
            hook_item.selector = selector_config(selector);
        }

        validate_hook_item(&hook_item)
            .map_err(|e| Status::invalid_argument(format!("Invalid hook config: {}", e)))?;
        // 平台行自己占着原名：只有改名才查。
        if row_scope(row.tenant_id.as_deref()).is_some() || hook_item.name != row.name {
            self.reject_global_name(&row.hook_type, &hook_item.name)
                .await?;
        }

        // 更新数据库
        let updated = self
            .repository
            .update(row.id, &hook_item)
            .await
            .map_err(|e| Status::internal(format!("Failed to update hook config: {}", e)))?;

        if !updated {
            return Err(Status::not_found("Hook config not found"));
        }

        // 构建响应
        let hook_config = hook_config_item_to_protobuf(
            &row.id.to_string(),
            row.tenant_id.as_deref().unwrap_or(""),
            &row.hook_type,
            &hook_item,
        )
        .map_err(|e| Status::internal(format!("Failed to convert hook config: {}", e)))?;

        Ok(Response::new(UpdateHookConfigResponse {
            config: Some(hook_config),
        }))
    }

    pub async fn grpc_list_hook_configs(
        &self,
        request: Request<ListHookConfigsRequest>,
    ) -> Result<Response<ListHookConfigsResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = row_scope(Some(req.tenant_id.as_str()));

        // 查询Hook配置（支持enabled_only过滤和租户过滤）
        let hook_type_filter = if req.hook_type.is_empty() {
            None
        } else {
            Some(req.hook_type.as_str())
        };

        let rows = self
            .repository
            .query(tenant_id, hook_type_filter, req.enabled_only)
            .await
            .map_err(|e| Status::internal(format!("Failed to query hook configs: {}", e)))?;

        // 转换为protobuf类型
        let mut configs = Vec::new();

        for row in rows {
            let hook_item: crate::domain::model::HookConfigItem = row
                .clone()
                .try_into()
                .map_err(|e| Status::internal(format!("Failed to convert hook config: {}", e)))?;

            // 应用enabled_only过滤（如果查询时没有过滤）
            if !req.enabled_only || hook_item.enabled {
                configs.push(
                    hook_config_item_to_protobuf(
                        &row.id.to_string(),
                        row.tenant_id.as_deref().unwrap_or(""),
                        &row.hook_type,
                        &hook_item,
                    )
                    .map_err(|e| {
                        Status::internal(format!("Failed to convert hook config: {}", e))
                    })?,
                );
            }
        }

        // 应用分页限制
        let limit = req
            .pagination
            .as_ref()
            .map(|p| p.limit as usize)
            .unwrap_or(100)
            .min(1000); // 最多1000条

        let total_count = configs.len();
        let configs: Vec<_> = configs.into_iter().take(limit).collect();

        // 更新分页信息
        let mut pagination = req.pagination.unwrap_or_default();
        pagination.has_more = total_count > configs.len();
        pagination.total_size = total_count as i64;

        Ok(Response::new(ListHookConfigsResponse {
            configs,
            pagination: Some(pagination),
        }))
    }

    pub async fn grpc_delete_hook_config(
        &self,
        request: Request<DeleteHookConfigRequest>,
    ) -> Result<Response<DeleteHookConfigResponse>, Status> {
        // 先提取租户ID（在into_inner()之前）
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();

        if req.hook_id.is_empty() {
            return Err(Status::invalid_argument("hook_id is required"));
        }

        // 解析hook_id（格式：hook_type:name 或 id）
        let hook_id_parsed = req.hook_id.parse::<i64>();

        let deleted = if let Ok(id) = hook_id_parsed {
            // 作为数字ID查询并删除
            if let Some((row, _)) = self
                .repository
                .get_by_id(id)
                .await
                .map_err(|_| Status::internal("Failed to get hook config"))?
            {
                self.repository
                    .delete_by_id(row.id)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to delete hook config: {}", e)))?
            } else {
                false
            }
        } else {
            // 作为hook_type:name格式解析
            let parts: Vec<&str> = req.hook_id.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Status::invalid_argument(
                    "Invalid hook_id format, expected numeric id or 'hook_type:name'",
                ));
            }

            let hook_type = parts[0];
            let name = parts[1];

            // 删除Hook配置
            self.repository
                .delete(tenant_id.as_deref(), hook_type, name)
                .await
                .map_err(|e| Status::internal(format!("Failed to delete hook config: {}", e)))?
        };

        if !deleted {
            return Err(Status::not_found("Hook config not found"));
        }

        Ok(Response::new(DeleteHookConfigResponse { success: true }))
    }

    pub async fn grpc_set_hook_status(
        &self,
        request: Request<SetHookStatusRequest>,
    ) -> Result<Response<SetHookStatusResponse>, Status> {
        // 先提取租户ID（在into_inner()之前）
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();

        if req.hook_id.is_empty() {
            return Err(Status::invalid_argument("hook_id is required"));
        }

        // 解析hook_id（格式：hook_type:name 或 id）
        let hook_id_parsed = req.hook_id.parse::<i64>();

        let row = if let Ok(id) = hook_id_parsed {
            self.repository
                .get_by_id(id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?
                .0
        } else {
            // 作为hook_type:name格式解析，需要先查询获取ID
            let parts: Vec<&str> = req.hook_id.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Status::invalid_argument(
                    "Invalid hook_id format, expected numeric id or 'hook_type:name'",
                ));
            }

            let hook_type = parts[0];
            let name = parts[1];

            let (row, _) = self
                .repository
                .get_by_name(tenant_id.as_deref(), hook_type, name)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?;

            row
        };

        // 更新数据库中的enabled字段
        let updated = self
            .repository
            .update_enabled(row.id, req.enabled)
            .await
            .map_err(|e| Status::internal(format!("Failed to update hook status: {}", e)))?;

        if !updated {
            return Err(Status::not_found("Hook config not found"));
        }

        Ok(Response::new(SetHookStatusResponse { success: true }))
    }

    /// 点位目录。
    pub fn hook_points(&self) -> ListHookPointsResponse {
        ListHookPointsResponse {
            points: HOOK_POINTS
                .iter()
                .map(|point| HookPointEntry {
                    id: point.id.to_string(),
                    hook_type: point.hook_type.to_string(),
                    summary: point.summary.to_string(),
                })
                .collect(),
        }
    }

    /// 部署 Hook：每个服务启动时写的摘要，这里只读。
    pub async fn hook_profiles(&self) -> Result<ListHookProfilesResponse, Status> {
        let rows = self
            .repository
            .deployment_summaries()
            .await
            .map_err(|_| Status::unavailable("hook deployment summary is unavailable"))?;
        Ok(ListHookProfilesResponse {
            profiles: rows.into_iter().map(hook_profile).collect(),
        })
    }

    /// 用样例载荷调用目标一次，不进发送路径。有 `hook_id` 时用已存的目标，请求里的 `transport` 改它，
    /// 规则和更新一样：目标变了就不沿用只写不读的取值。
    pub async fn test_hook_config(
        &self,
        req: TestHookConfigRequest,
        request_id: &str,
    ) -> Result<TestHookConfigResponse, Status> {
        let stored = if req.hook_id.is_empty() {
            None
        } else {
            let id = req
                .hook_id
                .parse::<i64>()
                .map_err(|_| Status::invalid_argument("hook_id must be numeric"))?;
            let (row, item) = self
                .repository
                .get_by_id(id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get hook config: {}", e)))?
                .ok_or_else(|| Status::not_found("Hook config not found"))?;
            Some((row.hook_type, item))
        };
        let hook_type = stored
            .as_ref()
            .map(|(hook_type, _)| hook_type.as_str())
            .unwrap_or(req.hook_type.as_str());
        administrable_hook_type(hook_type).map_err(Status::invalid_argument)?;
        let (transport, timeout_ms) = match (&req.transport, &stored) {
            (Some(transport), Some((_, item))) => (
                updated_transport(transport, &item.transport).map_err(Status::invalid_argument)?,
                transport.timeout_ms as u64,
            ),
            (None, Some((_, item))) => (item.transport.clone(), item.timeout_ms),
            (Some(transport), None) => (
                new_transport(transport)
                    .map_err(|e| Status::invalid_argument(format!("Invalid target: {e}")))?,
                transport.timeout_ms as u64,
            ),
            (None, None) => return Err(Status::invalid_argument("transport is required")),
        };
        let item = HookConfigItem {
            name: TEST_HOOK_NAME.to_string(),
            description: None,
            enabled: true,
            order: 0,
            timeout_ms,
            require_success: true,
            selector: HookSelectorConfig::default(),
            transport,
            metadata: HashMap::new(),
        };
        validate_hook_item(&item)
            .map_err(|e| Status::invalid_argument(format!("Invalid target: {}", e)))?;
        // 走和 Ingest 读库一样的解析：能测通的目标，保存后也加载得了。
        let definition = StoredHook {
            id: 0,
            tenant_id: None,
            name: item.name.clone(),
            description: None,
            order: 0,
            timeout_ms: item.timeout_ms as i64,
            require_success: true,
            selector: serde_json::Value::Object(Default::default()),
            transport: serde_json::to_value(&item.transport)
                .map_err(|_| Status::internal("Failed to convert hook target"))?,
            metadata: None,
        }
        .definition()
        .map_err(|_| Status::invalid_argument("Invalid target"))?;
        let definition = HookDefinition {
            timeout_ms: definition.timeout_ms.min(MAX_TEST_TIMEOUT_MS),
            ..definition
        };
        let report = test_call(&definition, TEST_HOOK_TENANT, request_id).await;
        Ok(TestHookConfigResponse {
            outcome: report.outcome.as_str().to_string(),
            latency_ms: i64::try_from(report.latency.as_millis()).unwrap_or(i64::MAX),
            code: report.code,
        })
    }

    pub async fn grpc_get_hook_statistics(
        &self,
        request: Request<GetHookStatisticsRequest>,
    ) -> Result<Response<GetHookStatisticsResponse>, Status> {
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();
        let scope = self
            .execution_scope(tenant_id.as_deref(), &req.hook_id)
            .await?;
        let range = time_range(req.time_range.as_ref(), STATISTICS_DEFAULT_SPAN)?;

        let statistics = match self.executions.as_ref() {
            Some(executions) => executions
                .statistics(&scope, &range)
                .await
                .map_err(execution_read_error)?,
            None => HookExecutionStatistics::default(),
        };

        Ok(Response::new(GetHookStatisticsResponse {
            statistics: Some(domain_to_protobuf_statistics(req.hook_id, statistics)),
        }))
    }

    pub async fn grpc_query_hook_executions(
        &self,
        request: Request<QueryHookExecutionsRequest>,
    ) -> Result<Response<QueryHookExecutionsResponse>, Status> {
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();
        let scope = self
            .execution_scope(tenant_id.as_deref(), &req.hook_id)
            .await?;
        let range = time_range(req.time_range.as_ref(), EXECUTIONS_DEFAULT_SPAN)?;
        let limit = req
            .pagination
            .as_ref()
            .map(|p| p.limit)
            .filter(|limit| *limit > 0)
            .unwrap_or(DEFAULT_EXECUTION_PAGE)
            .min(MAX_EXECUTION_PAGE);

        let (executions, has_more) = match self.executions.as_ref() {
            Some(executions) => {
                // 多取一条判断后面还有没有。
                let mut rows = executions
                    .executions(&HookExecutionFilter {
                        scope,
                        range,
                        message_id: Some(req.message_id.clone()).filter(|id| !id.is_empty()),
                        success_only: req.success_only,
                        limit: limit as u32 + 1,
                    })
                    .await
                    .map_err(execution_read_error)?;
                let has_more = rows.len() > limit as usize;
                rows.truncate(limit as usize);
                (rows, has_more)
            }
            None => (Vec::new(), false),
        };

        Ok(Response::new(QueryHookExecutionsResponse {
            executions: executions
                .into_iter()
                .map(|execution| domain_to_protobuf_execution(&req.hook_id, execution))
                .collect(),
            pagination: Some(Pagination {
                limit,
                has_more,
                ..Default::default()
            }),
        }))
    }
}

/// 部署开出网之前，应用 Hook 建不出来（H3）。
const APPLICATION_HOOKS_UNAVAILABLE: &str = "application hooks are not available: they need outbound validation, an egress switch and quotas, which this deployment does not have";

/// 测试调用样例消息的名字与所属应用。
const TEST_HOOK_NAME: &str = "hook-test";
const TEST_HOOK_TENANT: &str = "hook-test";
/// 测试调用的时限上限：要在管理分区的请求预算（5 秒）内答完，慢目标报超时。
const MAX_TEST_TIMEOUT_MS: u64 = 3_000;

/// 统计没给时间范围时取最近 24 小时（`GetHookStatisticsRequest.time_range` 的约定）。
const STATISTICS_DEFAULT_SPAN: Duration = Duration::from_secs(86_400);
/// 执行记录没给时间范围时取保留期上限内的全部（90 天）。
const EXECUTIONS_DEFAULT_SPAN: Duration = Duration::from_secs(90 * 86_400);
const DEFAULT_EXECUTION_PAGE: i32 = 100;
const MAX_EXECUTION_PAGE: i32 = 1_000;

/// 管理接口的时间范围：越界（负数、晚于 9999 年末）或倒置为 INVALID_ARGUMENT。
fn time_range(
    range: Option<&TimeRange>,
    default_span: Duration,
) -> Result<HookExecutionTimeRange, Status> {
    HookExecutionTimeRange::from_millis(
        range.and_then(|r| r.start_time),
        range.and_then(|r| r.end_time),
        SystemTime::now(),
        default_span,
    )
    .map_err(Status::invalid_argument)
}

/// 读执行记录失败：存储层判为无效参数的（时间换算越界）回 INVALID_ARGUMENT，其余为存储不可用。
/// 错误原文不进应答。
fn execution_read_error(error: FlareError) -> Status {
    if error.code() == Some(ErrorCode::InvalidParameter) {
        Status::invalid_argument("time is out of range")
    } else {
        Status::unavailable("hook execution storage is unavailable")
    }
}

/// 将统计数据转换为protobuf类型（计数按采样权重估计；Hook 没有限流，限流次数为 0）
fn domain_to_protobuf_statistics(
    hook_id: String,
    stats: HookExecutionStatistics,
) -> HookStatistics {
    HookStatistics {
        hook_id,
        total_executions: stats.total_executions,
        success_count: stats.success_count,
        failure_count: stats.failure_count,
        avg_latency_ms: stats.avg_latency_ms,
        p99_latency_ms: stats.p99_latency_ms,
        rate_limit_count: 0,
        circuit_break_count: stats.circuit_open_count,
        error_count_by_code: stats.error_count_by_code,
    }
}

/// 将protobuf类型转换为内部HookConfigItem类型
fn protobuf_to_hook_config_item(
    req: &CreateHookConfigRequest,
    _existing: Option<&HookConfigItem>,
) -> FlareResult<HookConfigItem> {
    let transport = req.transport.as_ref().ok_or_else(|| {
        ErrorBuilder::new(ErrorCode::InvalidParameter, "transport is required").build_error()
    })?;

    Ok(HookConfigItem {
        name: req.name.clone(),
        description: None,
        enabled: true,
        order: req.order,
        timeout_ms: transport.timeout_ms as u64,
        require_success: req.required,
        selector: req
            .selector
            .as_ref()
            .map(selector_config)
            .unwrap_or_default(),
        transport: new_transport(transport)?,
        metadata: std::collections::HashMap::new(),
    })
}

fn selector_config(selector: &HookSelector) -> HookSelectorConfig {
    HookSelectorConfig {
        tenants: selector.tenants.clone(),
        conversation_types: selector.conversation_types.clone(),
        message_types: selector.message_types.clone(),
    }
}

/// 请求里的目标：没有已存的值可沿用，读回掩码不算取值。
fn new_transport(transport: &HookTransport) -> FlareResult<HookTransportConfig> {
    match transport.r#type.as_str() {
        "grpc" => {
            // 优先级：service_name > endpoint（直接地址）
            let endpoint = if transport.service_name.is_empty() {
                Some(transport.endpoint.clone())
            } else {
                None
            };
            let service_name = if !transport.service_name.is_empty() {
                Some(transport.service_name.clone())
            } else {
                None
            };

            // 解析负载均衡策略
            let load_balance = if !transport.load_balance.is_empty() {
                crate::domain::model::LoadBalanceStrategy::from_str(&transport.load_balance).ok()
            } else {
                None
            };

            Ok(HookTransportConfig::Grpc {
                endpoint,
                service_name,
                registry_type: if !transport.registry_type.is_empty() {
                    Some(transport.registry_type.clone())
                } else {
                    None
                },
                namespace: if !transport.namespace.is_empty() {
                    Some(transport.namespace.clone())
                } else {
                    None
                },
                load_balance,
                metadata: reject_read_back_mask("metadata", &transport.metadata)?,
            })
        }
        "webhook" => {
            if clear_signing_secret_requested(transport).map_err(invalid_parameter)?
                && !transport.secret.is_empty()
            {
                return Err(invalid_parameter(CLEAR_WITH_NEW_SECRET.to_string()));
            }
            Ok(HookTransportConfig::Webhook {
                endpoint: transport.endpoint.clone(),
                secret: if transport.secret.is_empty() {
                    None
                } else {
                    Some(transport.secret.clone())
                },
                headers: reject_read_back_mask("header", &transport.headers)?,
            })
        }
        _ => Err(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "Unsupported transport type")
                .details(transport.r#type.clone())
                .build_error(),
        ),
    }
}

/// Webhook 配置读回时 `transport.metadata` 里说明签名密钥是否已设置的键（值为 `"true"`/`"false"`）。
/// 密钥本身从不读回。
pub(crate) const WEBHOOK_SECRET_CONFIGURED_KEY: &str = "signing_secret_configured";

/// Webhook 的 `update` 请求里 `transport.metadata` 的这个键为 `"true"` 时清除已存的签名密钥、
/// 回到不签名（`transport.secret` 须为空）。读回从不带这个键，照读回的配置写回不会清掉密钥。
pub(crate) const WEBHOOK_CLEAR_SECRET_KEY: &str = "clear_signing_secret";

/// 读回中 Webhook 自定义头与 gRPC metadata 的取值一律换成这个固定掩码（只给键名，与
/// `HookTransportConfig` 的 `Debug` 脱敏口径一致）。`update` 时值为空或等于它表示沿用已存的值。
pub(crate) const READ_BACK_VALUE_MASK: &str = "<redacted>";

const CLEAR_WITH_NEW_SECRET: &str = "transport.secret must be empty when metadata clear_signing_secret is \"true\": send a new secret to rotate it, or clear it, not both";

fn invalid_parameter(reason: String) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(ErrorCode::InvalidParameter, reason).build_error()
}

/// update 时投递目标变了而请求没有重新给出只写不读的取值：错误以此开头（`INVALID_ARGUMENT`）。
pub(crate) const TARGET_CHANGED: &str = "delivery target changed";

/// 一条 Hook 的投递目标：Webhook 的 URL，gRPC 的地址、服务名、注册中心与命名空间。
/// 负载均衡、超时等不改变发往哪里，不算在内；空串与未设视为相同。
#[derive(Debug, PartialEq, Eq)]
enum DeliveryTarget<'a> {
    Grpc {
        endpoint: Option<&'a str>,
        service_name: Option<&'a str>,
        registry_type: Option<&'a str>,
        namespace: Option<&'a str>,
    },
    Webhook(&'a str),
}

fn delivery_target(transport: &HookTransportConfig) -> DeliveryTarget<'_> {
    fn set(value: &Option<String>) -> Option<&str> {
        value.as_deref().filter(|value| !value.is_empty())
    }
    match transport {
        HookTransportConfig::Grpc {
            endpoint,
            service_name,
            registry_type,
            namespace,
            ..
        } => DeliveryTarget::Grpc {
            endpoint: set(endpoint),
            service_name: set(service_name),
            registry_type: set(registry_type),
            namespace: set(namespace),
        },
        HookTransportConfig::Webhook { endpoint, .. } => DeliveryTarget::Webhook(endpoint),
    }
}

/// 请求里为空或等于掩码的取值能沿用什么。
#[derive(Clone, Copy)]
enum Keep<'a> {
    /// 投递目标没变：沿用库里同名键的值（没有就报错）。
    Stored(Option<&'a HashMap<String, String>>),
    /// 投递目标变了：什么都不沿用，空值或掩码报错。否则有更新权限的人只要把端点改成自己的，
    /// 库里隐藏的 `Authorization`、`x-api-key` 或签名密钥就会随下一次投递发过去。
    Nothing,
}

/// `update` 要写入的传输配置：由请求与库里已存的配置得出。
///
/// 只写不读的取值（Webhook 自定义头、gRPC metadata、签名密钥）在请求里为空或为掩码时，投递
/// 目标不变才沿用库里的值；目标变了（见 [`DeliveryTarget`]，含换传输类型）就必须重新给出，
/// 签名密钥也可以显式 `clear_signing_secret`，否则报错（以 [`TARGET_CHANGED`] 开头，只带键名）。
fn updated_transport(
    transport: &HookTransport,
    stored: &HookTransportConfig,
) -> Result<HookTransportConfig, String> {
    let mut updated = match transport.r#type.as_str() {
        "grpc" => {
            // 优先级：service_name > endpoint（直接地址）
            let endpoint = if transport.service_name.is_empty() {
                Some(transport.endpoint.clone())
            } else {
                None
            };
            let service_name = if !transport.service_name.is_empty() {
                Some(transport.service_name.clone())
            } else {
                None
            };

            // 解析负载均衡策略
            let load_balance = if !transport.load_balance.is_empty() {
                crate::domain::model::LoadBalanceStrategy::from_str(&transport.load_balance).ok()
            } else {
                None
            };

            HookTransportConfig::Grpc {
                endpoint,
                service_name,
                registry_type: if !transport.registry_type.is_empty() {
                    Some(transport.registry_type.clone())
                } else {
                    None
                },
                namespace: if !transport.namespace.is_empty() {
                    Some(transport.namespace.clone())
                } else {
                    None
                },
                load_balance,
                metadata: HashMap::new(),
            }
        }
        "webhook" => HookTransportConfig::Webhook {
            endpoint: transport.endpoint.clone(),
            secret: None,
            headers: HashMap::new(),
        },
        other => return Err(format!("Unsupported transport type: {other}")),
    };
    let same_target = delivery_target(&updated) == delivery_target(stored);

    match &mut updated {
        HookTransportConfig::Grpc { metadata, .. } => {
            // 读回只给键名与掩码：值为空或等于掩码的键沿用已存的值（目标不变时）。
            *metadata = keep_stored_values(
                "metadata",
                &transport.metadata,
                if same_target {
                    Keep::Stored(stored_grpc_metadata(stored))
                } else {
                    Keep::Nothing
                },
            )?;
        }
        HookTransportConfig::Webhook {
            secret, headers, ..
        } => {
            *headers = keep_stored_values(
                "header",
                &transport.headers,
                if same_target {
                    Keep::Stored(stored_webhook_headers(stored))
                } else {
                    Keep::Nothing
                },
            )?;
            // 签名密钥只写不读（读回为空），空串表示沿用已存的密钥：照读回的配置原样写回不会
            // 把签名关掉。换密钥就传新的非空值；回到不签名要显式
            // `metadata["clear_signing_secret"] = "true"`（读回从不带这个键）。端点变了时空串
            // 不沿用：重新给出密钥或显式清除。
            *secret = match (
                transport.secret.is_empty(),
                clear_signing_secret_requested(transport)?,
            ) {
                (false, false) => Some(transport.secret.clone()),
                (true, true) => None,
                (true, false) => match stored_webhook_secret(stored) {
                    Some(_) if !same_target => {
                        return Err(format!(
                            "{TARGET_CHANGED}: send the signing secret again, or set metadata {WEBHOOK_CLEAR_SECRET_KEY}=\"true\" to deliver unsigned (an empty secret keeps the stored one only while the endpoint stays the same)"
                        ));
                    }
                    kept => kept,
                },
                (false, true) => return Err(CLEAR_WITH_NEW_SECRET.to_string()),
            };
        }
    }
    Ok(updated)
}

/// 已存配置是 Webhook 时它的签名密钥（更新时请求留空即沿用）。
fn stored_webhook_secret(transport: &HookTransportConfig) -> Option<String> {
    match transport {
        HookTransportConfig::Webhook { secret, .. } => {
            secret.clone().filter(|secret| !secret.is_empty())
        }
        HookTransportConfig::Grpc { .. } => None,
    }
}

/// 已存配置是 Webhook 时它的自定义头。
fn stored_webhook_headers(transport: &HookTransportConfig) -> Option<&HashMap<String, String>> {
    match transport {
        HookTransportConfig::Webhook { headers, .. } => Some(headers),
        HookTransportConfig::Grpc { .. } => None,
    }
}

/// 已存配置是 gRPC 时它的请求 metadata。
fn stored_grpc_metadata(transport: &HookTransportConfig) -> Option<&HashMap<String, String>> {
    match transport {
        HookTransportConfig::Grpc { metadata, .. } => Some(metadata),
        HookTransportConfig::Webhook { .. } => None,
    }
}

/// Webhook 请求要不要清除签名密钥：`metadata["clear_signing_secret"]` 为 `"true"` 才清，缺省或
/// `"false"` 不清，其他取值报错（错误里只有键名）。
fn clear_signing_secret_requested(transport: &HookTransport) -> Result<bool, String> {
    match transport
        .metadata
        .get(WEBHOOK_CLEAR_SECRET_KEY)
        .map(String::as_str)
    {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(format!(
            "metadata {WEBHOOK_CLEAR_SECRET_KEY} must be \"true\" or \"false\""
        )),
    }
}

/// 读回用：只留键名，值一律换成 [`READ_BACK_VALUE_MASK`]。
fn masked_values(values: &HashMap<String, String>) -> HashMap<String, String> {
    values
        .keys()
        .map(|key| (key.clone(), READ_BACK_VALUE_MASK.to_string()))
        .collect()
}

/// `stored` 里与 `key` 同名的值；HTTP 头与 gRPC metadata 的键不区分大小写，先找完全相同的。
fn stored_value<'a>(stored: Option<&'a HashMap<String, String>>, key: &str) -> Option<&'a String> {
    let stored = stored?;
    stored.get(key).or_else(|| {
        stored
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| value)
    })
}

/// `update`：请求里的头或 metadata 整组替换已存的，但值为空或等于读回掩码的键沿用已存的值
/// （照读回的配置写回不改任何取值）；这样的键没有已存的值、或投递目标变了（[`Keep::Nothing`]）
/// 时报错。请求里没有的键被删除。错误只带键名，不带任何取值。
fn keep_stored_values(
    what: &str,
    requested: &HashMap<String, String>,
    keep: Keep<'_>,
) -> Result<HashMap<String, String>, String> {
    requested
        .iter()
        .map(|(key, value)| {
            if !value.is_empty() && value != READ_BACK_VALUE_MASK {
                return Ok((key.clone(), value.clone()));
            }
            let stored = match keep {
                Keep::Stored(stored) => stored,
                Keep::Nothing => {
                    return Err(format!(
                        "{TARGET_CHANGED}: {what} '{key}' must be sent with its value (an empty value or {READ_BACK_VALUE_MASK} keeps the stored one only while the delivery target stays the same)"
                    ));
                }
            };
            stored_value(stored, key)
                .map(|kept| (key.clone(), kept.clone()))
                .ok_or_else(|| {
                    format!(
                        "{what} '{key}' has no stored value to keep: send its value (an empty value or {READ_BACK_VALUE_MASK} keeps the stored one)"
                    )
                })
        })
        .collect()
}

/// `create`：没有已存的值可沿用，值为读回掩码的头或 metadata（多半是照读回的配置新建）报错，
/// 不把掩码当成取值存下。
fn reject_read_back_mask(
    what: &str,
    requested: &HashMap<String, String>,
) -> FlareResult<HashMap<String, String>> {
    if let Some(key) = requested
        .iter()
        .find_map(|(key, value)| (value == READ_BACK_VALUE_MASK).then_some(key))
    {
        return Err(invalid_parameter(format!(
            "{what} '{key}' carries the read-back mask {READ_BACK_VALUE_MASK}: send its value"
        )));
    }
    Ok(requested.clone())
}

/// 将内部HookConfigItem类型转换为protobuf类型
fn hook_config_item_to_protobuf(
    hook_id: &str,
    tenant_id: &str,
    hook_type: &str,
    item: &HookConfigItem,
) -> FlareResult<HookConfig> {
    let now = Utc::now();

    Ok(HookConfig {
        hook_id: hook_id.to_string(),
        name: item.name.clone(),
        hook_type: hook_type.to_string(),
        tenant_id: tenant_id.to_string(),
        order: item.order,
        required: item.require_success,
        enabled: item.enabled,
        transport: Some(match &item.transport {
            HookTransportConfig::Grpc {
                endpoint,
                service_name,
                registry_type,
                namespace,
                load_balance,
                metadata,
            } => HookTransport {
                r#type: "grpc".to_string(),
                service_name: service_name.clone().unwrap_or_default(),
                endpoint: endpoint.clone().unwrap_or_default(),
                registry_type: registry_type.clone().unwrap_or_default(),
                namespace: namespace.clone().unwrap_or_default(),
                load_balance: load_balance
                    .as_ref()
                    .map(|lb| format!("{:?}", lb))
                    .unwrap_or_default(),
                secret: String::new(),
                headers: std::collections::HashMap::new(),
                timeout_ms: item.timeout_ms as i32,
                // 请求 metadata 常带凭据（如 `x-api-key`）：只读回键名与掩码。
                metadata: masked_values(metadata),
            },
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => HookTransport {
                r#type: "webhook".to_string(),
                service_name: String::new(),
                endpoint: endpoint.clone(),
                registry_type: String::new(),
                namespace: String::new(),
                load_balance: String::new(),
                // 签名密钥只写不读：create、get、list、update 的应答都不带它（W11-S1 F5），
                // 只在 metadata 里说明是否已设置。
                secret: String::new(),
                // 自定义头常带凭据（如 `Authorization`）：只读回键名与掩码。
                headers: masked_values(headers),
                timeout_ms: item.timeout_ms as i32,
                metadata: std::collections::HashMap::from([(
                    WEBHOOK_SECRET_CONFIGURED_KEY.to_string(),
                    secret
                        .as_deref()
                        .is_some_and(|secret| !secret.is_empty())
                        .to_string(),
                )]),
            },
        }),
        selector: Some(HookSelector {
            tenants: item.selector.tenants.clone(),
            conversation_types: item.selector.conversation_types.clone(),
            message_types: item.selector.message_types.clone(),
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
    })
}

/// `hook_deployment_summary` 的一行。`hooks` 是 Ingest 写的 JSON（`DeploymentHookSummary`）：读不懂的
/// 条目跳过，页面是只读的展示，不因一条坏数据打不开。
fn hook_profile(row: HookDeploymentSummaryRow) -> HookProfile {
    let hooks = row
        .hooks
        .as_array()
        .map(|hooks| hooks.iter().filter_map(deployment_hook).collect())
        .unwrap_or_default();
    HookProfile {
        service: row.service,
        source: row.source.unwrap_or_default(),
        instance_id: row.instance_id.unwrap_or_default(),
        loaded_at: Some(prost_types::Timestamp {
            seconds: row.loaded_at.timestamp(),
            nanos: row.loaded_at.timestamp_subsec_nanos() as i32,
        }),
        hooks,
    }
}

fn deployment_hook(value: &serde_json::Value) -> Option<DeploymentHook> {
    let text = |key: &str| value.get(key)?.as_str().map(str::to_string);
    let strings = |key: &str| -> Vec<String> {
        value
            .pointer(&format!("/selector/{key}"))
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    Some(DeploymentHook {
        name: text("name")?,
        hook_type: text("hook_type")?,
        description: text("description").unwrap_or_default(),
        enabled: value.get("enabled")?.as_bool()?,
        order: i32::try_from(value.get("order")?.as_i64()?).ok()?,
        required: value.get("required")?.as_bool()?,
        timeout_ms: value.get("timeout_ms")?.as_i64()?,
        target_type: value.pointer("/target/type")?.as_str()?.to_string(),
        target_endpoint: value.pointer("/target/endpoint")?.as_str()?.to_string(),
        selector: Some(HookSelector {
            tenants: strings("tenants"),
            conversation_types: strings("conversation_types"),
            message_types: strings("message_types"),
        }),
    })
}

/// 将执行记录转换为protobuf类型；`hook_id` 回请求里的写法。
fn domain_to_protobuf_execution(hook_id: &str, execution: StoredHookExecution) -> HookExecution {
    let executed_at = execution
        .executed_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    HookExecution {
        execution_id: execution.execution_id,
        hook_id: hook_id.to_string(),
        message_id: execution.message_id.unwrap_or_default(),
        success: execution.success,
        latency_ms: execution.latency_ms.unwrap_or_default(),
        error_code: execution.error_code.unwrap_or_default(),
        error_message: execution.error_message.unwrap_or_default(),
        executed_at: Some(prost_types::Timestamp {
            seconds: executed_at.as_secs() as i64,
            nanos: executed_at.subsec_nanos() as i32,
        }),
    }
}

#[cfg(test)]
mod execution_query_tests {
    use super::*;

    fn range(
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<HookExecutionTimeRange, Status> {
        time_range(
            Some(&TimeRange {
                start_time,
                end_time,
            }),
            STATISTICS_DEFAULT_SPAN,
        )
    }

    /// 管理员给的时间超出 9999 年末（例如 `end_time=9000000000000000`）回 INVALID_ARGUMENT，不会
    /// 进到存储层的时间换算里 panic。
    #[test]
    fn times_beyond_year_9999_are_invalid_arguments() {
        for (start, end) in [
            (None, Some(9_000_000_000_000_000)),
            (None, Some(i64::MAX)),
            (Some(9_000_000_000_000_000), None),
            (
                Some(0),
                Some(crate::domain::model::MAX_QUERY_TIME_MILLIS + 1),
            ),
            (Some(-1), None),
        ] {
            let status = range(start, end).unwrap_err();
            assert_eq!(
                status.code(),
                tonic::Code::InvalidArgument,
                "{start:?} {end:?}"
            );
        }
        assert!(range(Some(0), Some(crate::domain::model::MAX_QUERY_TIME_MILLIS)).is_ok());
        assert!(time_range(None, STATISTICS_DEFAULT_SPAN).is_ok());
    }

    /// 存储层的无效参数回 INVALID_ARGUMENT，其余存储错误回 UNAVAILABLE；错误原文都不进应答。
    #[test]
    fn storage_errors_map_to_fixed_statuses() {
        let invalid = execution_read_error(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "time is out of range").build_error(),
        );
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
        let down = execution_read_error(
            ErrorBuilder::new(ErrorCode::DatabaseError, "connect 10.0.0.9:5432 refused")
                .build_error(),
        );
        assert_eq!(down.code(), tonic::Code::Unavailable);
        assert!(!down.message().contains("10.0.0.9"));
    }
}

#[cfg(test)]
mod secret_tests {
    use super::*;
    use crate::interface::grpc::hooks::test_support::signed_webhook_hook;
    use prost::Message as _;

    const SECRET: &str = "w12w-never-read-back";

    /// 读回的配置不带签名密钥（W11-S1 F5）：应答编码后的字节里也找不到它，只说明已设置。
    #[test]
    fn webhook_configs_are_read_back_without_the_signing_secret() {
        for (secret, configured) in [(Some(SECRET), "true"), (None, "false")] {
            let item = signed_webhook_hook("wh", "http://127.0.0.1:9/hook", secret);
            let config = hook_config_item_to_protobuf("7", "tenant-a", "pre_send", &item).unwrap();
            let transport = config.transport.as_ref().unwrap();
            assert_eq!(transport.r#type, "webhook");
            assert!(transport.secret.is_empty());
            assert_eq!(
                transport
                    .metadata
                    .get(WEBHOOK_SECRET_CONFIGURED_KEY)
                    .map(String::as_str),
                Some(configured)
            );
            let encoded = config.encode_to_vec();
            assert!(
                !encoded
                    .windows(SECRET.len())
                    .any(|window| window == SECRET.as_bytes())
            );
        }
    }

    const HEADER_VALUE: &str = "Bearer w13h-header-never-read-back";
    const METADATA_VALUE: &str = "w13h-metadata-never-read-back";

    /// 读回的 Webhook 自定义头与 gRPC metadata 只有键名与固定掩码（修复前原样返回取值，
    /// 例如租户登记的 `Authorization: Bearer …`、`x-api-key`）。
    #[test]
    fn header_and_metadata_values_are_read_back_as_names_with_a_mask() {
        let mut webhook = signed_webhook_hook("wh", "http://127.0.0.1:9/hook", Some(SECRET));
        if let HookTransportConfig::Webhook { headers, .. } = &mut webhook.transport {
            headers.insert("Authorization".into(), HEADER_VALUE.into());
        }
        let mut grpc = signed_webhook_hook("g", "http://127.0.0.1:9", None);
        grpc.transport = HookTransportConfig::Grpc {
            endpoint: Some("http://127.0.0.1:9".into()),
            service_name: None,
            registry_type: None,
            namespace: None,
            load_balance: None,
            metadata: HashMap::from([("x-api-key".into(), METADATA_VALUE.into())]),
        };

        let config = hook_config_item_to_protobuf("7", "tenant-a", "pre_send", &webhook).unwrap();
        let transport = config.transport.as_ref().unwrap();
        assert_eq!(
            transport.headers,
            HashMap::from([(
                "Authorization".to_string(),
                READ_BACK_VALUE_MASK.to_string()
            )])
        );
        let encoded = config.encode_to_vec();

        let config = hook_config_item_to_protobuf("8", "tenant-a", "pre_send", &grpc).unwrap();
        let transport = config.transport.as_ref().unwrap();
        assert_eq!(
            transport.metadata,
            HashMap::from([("x-api-key".to_string(), READ_BACK_VALUE_MASK.to_string())])
        );
        for (bytes, value) in [
            (encoded, HEADER_VALUE),
            (config.encode_to_vec(), METADATA_VALUE),
        ] {
            assert!(
                !bytes
                    .windows(value.len())
                    .any(|window| window == value.as_bytes()),
                "a header or metadata value is read back"
            );
        }
    }

    /// update：值为空或等于掩码的键沿用已存的值（照读回写回不改取值），新值替换，请求里没有的
    /// 键删除；没有已存值可沿用时报错，错误只带键名。
    #[test]
    fn masked_or_empty_values_on_update_keep_the_stored_ones() {
        let stored = HashMap::from([
            ("Authorization".to_string(), HEADER_VALUE.to_string()),
            ("x-api-key".to_string(), METADATA_VALUE.to_string()),
            ("x-dropped".to_string(), "w13h-dropped".to_string()),
        ]);
        let requested = HashMap::from([
            (
                "Authorization".to_string(),
                READ_BACK_VALUE_MASK.to_string(),
            ),
            // 键名大小写不同也认（HTTP 头与 gRPC metadata 的键不区分大小写）。
            ("X-API-KEY".to_string(), String::new()),
            ("x-new".to_string(), "w13h-new".to_string()),
        ]);
        let kept = keep_stored_values("header", &requested, Keep::Stored(Some(&stored))).unwrap();
        assert_eq!(
            kept,
            HashMap::from([
                ("Authorization".to_string(), HEADER_VALUE.to_string()),
                ("X-API-KEY".to_string(), METADATA_VALUE.to_string()),
                ("x-new".to_string(), "w13h-new".to_string()),
            ])
        );

        for stored in [None, Some(&stored)] {
            let error = keep_stored_values(
                "metadata",
                &HashMap::from([("x-unknown".to_string(), READ_BACK_VALUE_MASK.to_string())]),
                Keep::Stored(stored),
            )
            .unwrap_err();
            assert!(error.contains("x-unknown"), "{error}");
            assert!(!error.contains("w13h"), "{error}");
        }
    }

    fn create_request(transport: HookTransport) -> CreateHookConfigRequest {
        CreateHookConfigRequest {
            name: "wh".into(),
            hook_type: "pre_send".into(),
            tenant_id: "tenant-a".into(),
            order: 900,
            transport: Some(transport),
            ..Default::default()
        }
    }

    /// create 没有已存值可沿用：掩码值报错（不把掩码当成取值存下）；清除签名密钥与新密钥同时
    /// 出现报错，清除标志只认 `"true"`/`"false"`。
    #[test]
    fn create_rejects_the_mask_and_a_contradictory_clear_flag() {
        let webhook = |secret: &str, headers: &[(&str, &str)], metadata: &[(&str, &str)]| {
            create_request(HookTransport {
                r#type: "webhook".into(),
                endpoint: "http://127.0.0.1:9/hook".into(),
                secret: secret.into(),
                headers: headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                metadata: metadata
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                timeout_ms: 1000,
                ..Default::default()
            })
        };
        let masked = webhook("", &[("Authorization", READ_BACK_VALUE_MASK)], &[]);
        let error = protobuf_to_hook_config_item(&masked, None).unwrap_err();
        assert!(error.to_string().contains("Authorization"), "{error}");

        let grpc = create_request(HookTransport {
            r#type: "grpc".into(),
            endpoint: "http://127.0.0.1:9".into(),
            metadata: HashMap::from([("x-api-key".into(), READ_BACK_VALUE_MASK.into())]),
            timeout_ms: 1000,
            ..Default::default()
        });
        assert!(protobuf_to_hook_config_item(&grpc, None).is_err());

        let both = webhook(SECRET, &[], &[(WEBHOOK_CLEAR_SECRET_KEY, "true")]);
        let error = protobuf_to_hook_config_item(&both, None).unwrap_err();
        assert!(!error.to_string().contains(SECRET), "{error}");
        let odd = webhook("", &[], &[(WEBHOOK_CLEAR_SECRET_KEY, "yes")]);
        assert!(protobuf_to_hook_config_item(&odd, None).is_err());

        // 照读回的 metadata（`signing_secret_configured`）新建、清除标志为 "true" 且无密钥：
        // 都是不签名的 Webhook。
        for metadata in [
            [(WEBHOOK_SECRET_CONFIGURED_KEY, "true")],
            [(WEBHOOK_CLEAR_SECRET_KEY, "true")],
        ] {
            let item = protobuf_to_hook_config_item(
                &webhook("", &[("X-Service", "core")], &metadata),
                None,
            )
            .unwrap();
            assert!(matches!(
                item.transport,
                HookTransportConfig::Webhook { secret: None, ref headers, .. }
                    if headers.get("X-Service").map(String::as_str) == Some("core")
            ));
        }
    }

    #[test]
    fn the_clear_flag_is_explicit() {
        let transport = |value: Option<&str>| HookTransport {
            r#type: "webhook".into(),
            metadata: value
                .map(|value| HashMap::from([(WEBHOOK_CLEAR_SECRET_KEY.into(), value.into())]))
                .unwrap_or_default(),
            ..Default::default()
        };
        assert_eq!(clear_signing_secret_requested(&transport(None)), Ok(false));
        assert_eq!(
            clear_signing_secret_requested(&transport(Some("false"))),
            Ok(false)
        );
        assert_eq!(
            clear_signing_secret_requested(&transport(Some("true"))),
            Ok(true)
        );
        assert!(clear_signing_secret_requested(&transport(Some("1"))).is_err());
    }

    const NEW_ENDPOINT: &str = "http://127.0.0.1:10/elsewhere";

    fn stored_webhook(secret: Option<&str>) -> HookTransportConfig {
        HookTransportConfig::Webhook {
            endpoint: "http://127.0.0.1:9/hook".into(),
            secret: secret.map(str::to_string),
            headers: HashMap::from([
                ("Authorization".to_string(), HEADER_VALUE.to_string()),
                ("X-Note".to_string(), "w14h-note".to_string()),
            ]),
        }
    }

    fn webhook_update(endpoint: &str, secret: &str, headers: &[(&str, &str)]) -> HookTransport {
        HookTransport {
            r#type: "webhook".into(),
            endpoint: endpoint.into(),
            secret: secret.into(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            timeout_ms: 1000,
            ..Default::default()
        }
    }

    fn assert_target_changed(error: &str, names: &[&str]) {
        assert!(error.starts_with(TARGET_CHANGED), "{error}");
        for name in names {
            assert!(error.contains(name), "{error}");
        }
        for value in [HEADER_VALUE, METADATA_VALUE, SECRET, "w14h-note"] {
            assert!(!error.contains(value), "{error}");
        }
    }

    /// 端点不变：掩码与空值沿用库里的头与密钥（W13-H 的契约不变）。
    #[test]
    fn the_same_endpoint_keeps_the_stored_values() {
        let stored = stored_webhook(Some(SECRET));
        let request = webhook_update(
            "http://127.0.0.1:9/hook",
            "",
            &[("Authorization", READ_BACK_VALUE_MASK), ("X-Note", "")],
        );
        let updated = updated_transport(&request, &stored).unwrap();
        assert!(matches!(
            updated,
            HookTransportConfig::Webhook { secret: Some(ref secret), ref headers, .. }
                if secret == SECRET
                    && headers.get("Authorization").map(String::as_str) == Some(HEADER_VALUE)
                    && headers.get("X-Note").map(String::as_str) == Some("w14h-note")
        ));
    }

    /// 端点变了：头的掩码或空值、空密钥都不沿用库里的值（否则改端点即可取回隐藏的
    /// `Authorization` 与签名密钥）；报错以「delivery target changed」开头、只带键名。重新给出取值、
    /// 新密钥或显式清除才放行。
    #[test]
    fn a_new_webhook_endpoint_does_not_inherit_header_values_or_the_secret() {
        let stored = stored_webhook(Some(SECRET));
        for value in [READ_BACK_VALUE_MASK, ""] {
            let request = webhook_update(
                NEW_ENDPOINT,
                "w14h-new-secret",
                &[("Authorization", value), ("X-Note", "w14h-new-note")],
            );
            let error = updated_transport(&request, &stored).unwrap_err();
            assert_target_changed(&error, &["header 'Authorization'"]);
        }

        let resupplied = [
            ("Authorization", "Bearer w14h-new"),
            ("X-Note", "w14h-new-note"),
        ];
        let error =
            updated_transport(&webhook_update(NEW_ENDPOINT, "", &resupplied), &stored).unwrap_err();
        assert_target_changed(&error, &["signing secret", WEBHOOK_CLEAR_SECRET_KEY]);

        let updated = updated_transport(
            &webhook_update(NEW_ENDPOINT, "w14h-new-secret", &resupplied),
            &stored,
        )
        .unwrap();
        assert!(matches!(
            updated,
            HookTransportConfig::Webhook { ref endpoint, secret: Some(ref secret), ref headers }
                if endpoint == NEW_ENDPOINT
                    && secret == "w14h-new-secret"
                    && headers.get("Authorization").map(String::as_str) == Some("Bearer w14h-new")
        ));

        let mut clear = webhook_update(NEW_ENDPOINT, "", &resupplied);
        clear
            .metadata
            .insert(WEBHOOK_CLEAR_SECRET_KEY.into(), "true".into());
        assert!(matches!(
            updated_transport(&clear, &stored).unwrap(),
            HookTransportConfig::Webhook { secret: None, .. }
        ));

        // 库里本来就没有密钥：新端点空密钥即不签名，没有可泄露的东西。
        assert!(matches!(
            updated_transport(
                &webhook_update(NEW_ENDPOINT, "", &resupplied),
                &stored_webhook(None)
            )
            .unwrap(),
            HookTransportConfig::Webhook { secret: None, .. }
        ));
        // 没有带取值的头也不需要重新给。
        assert!(updated_transport(&webhook_update(NEW_ENDPOINT, "w14h-s", &[]), &stored).is_ok());
    }

    fn stored_grpc() -> HookTransportConfig {
        HookTransportConfig::Grpc {
            endpoint: Some("http://127.0.0.1:9".into()),
            service_name: None,
            registry_type: None,
            namespace: None,
            load_balance: None,
            metadata: HashMap::from([("x-api-key".to_string(), METADATA_VALUE.to_string())]),
        }
    }

    fn grpc_update(change: impl FnOnce(&mut HookTransport)) -> HookTransport {
        let mut transport = HookTransport {
            r#type: "grpc".into(),
            endpoint: "http://127.0.0.1:9".into(),
            metadata: HashMap::from([("x-api-key".to_string(), READ_BACK_VALUE_MASK.to_string())]),
            timeout_ms: 1000,
            ..Default::default()
        };
        change(&mut transport);
        transport
    }

    /// gRPC：地址、服务名、注册中心、命名空间任一变化都算换了投递目标，metadata 的掩码不沿用；
    /// 只改负载均衡或超时不算。换传输类型同样不沿用。
    #[test]
    fn a_new_grpc_target_does_not_inherit_metadata_values() {
        let stored = stored_grpc();
        let kept = updated_transport(
            &grpc_update(|t| {
                t.load_balance = "random".into();
                t.timeout_ms = 2000;
            }),
            &stored,
        )
        .unwrap();
        assert!(matches!(
            kept,
            HookTransportConfig::Grpc { ref metadata, .. }
                if metadata.get("x-api-key").map(String::as_str) == Some(METADATA_VALUE)
        ));

        for change in [
            (|t: &mut HookTransport| t.endpoint = "http://127.0.0.1:10".into())
                as fn(&mut HookTransport),
            |t| t.service_name = "attacker-hook".into(),
            |t| t.registry_type = "consul".into(),
            |t| t.namespace = "other".into(),
        ] {
            let error = updated_transport(&grpc_update(change), &stored).unwrap_err();
            assert_target_changed(&error, &["metadata 'x-api-key'"]);
        }

        // 新目标上重新给出取值即可。
        let resupplied = updated_transport(
            &grpc_update(|t| {
                t.endpoint = "http://127.0.0.1:10".into();
                t.metadata.insert("x-api-key".into(), "w14h-new-key".into());
            }),
            &stored,
        )
        .unwrap();
        assert!(matches!(
            resupplied,
            HookTransportConfig::Grpc { ref metadata, .. }
                if metadata.get("x-api-key").map(String::as_str) == Some("w14h-new-key")
        ));

        // Webhook 改成 gRPC（或反过来）也是换目标。
        let error = updated_transport(
            &grpc_update(|t| {
                t.metadata = HashMap::from([("Authorization".into(), READ_BACK_VALUE_MASK.into())])
            }),
            &stored_webhook(Some(SECRET)),
        )
        .unwrap_err();
        assert_target_changed(&error, &["metadata 'Authorization'"]);
        let error = updated_transport(
            &webhook_update(
                "http://127.0.0.1:9",
                "",
                &[("x-api-key", READ_BACK_VALUE_MASK)],
            ),
            &stored,
        )
        .unwrap_err();
        assert_target_changed(&error, &["header 'x-api-key'"]);
    }

    #[test]
    fn an_empty_secret_on_update_keeps_the_stored_one() {
        let stored = signed_webhook_hook("wh", "http://127.0.0.1:9/hook", Some(SECRET));
        assert_eq!(
            stored_webhook_secret(&stored.transport).as_deref(),
            Some(SECRET)
        );
        let unsigned = signed_webhook_hook("wh", "http://127.0.0.1:9/hook", None);
        assert_eq!(stored_webhook_secret(&unsigned.transport), None);
    }
}

#[cfg(test)]
mod name_tests {
    //! 创建与改名时的名称规则（`validate_hook_name`）：在查库之前就拒绝。仓储指向不监听的端口，
    //! 规则放过的请求会走到存储并失败，所以 INVALID_ARGUMENT 只能来自名称校验。

    use super::*;
    use crate::domain::model::MAX_HOOK_NAME_CHARS;
    use flare_server_core::context::{Context, Ctx};
    use tonic::Code;

    fn governance() -> HookServiceServer {
        HookServiceServer::new(Arc::new(PostgresHookConfigRepository::unconnected()))
    }

    fn trusted<T>(payload: T) -> Request<T> {
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-1")
                .with_tenant_id("tenant-a")
                .with_user_id("employee-1"),
        );
        let mut request = Request::new(payload);
        request.extensions_mut().insert(ctx);
        request
    }

    fn create(name: &str) -> CreateHookConfigRequest {
        CreateHookConfigRequest {
            name: name.into(),
            hook_type: "pre_send".into(),
            // 平台范围：应用 Hook 在名称校验之前就回 FAILED_PRECONDITION。
            tenant_id: String::new(),
            order: 900,
            transport: Some(HookTransport {
                r#type: "webhook".into(),
                endpoint: "http://127.0.0.1:9/hook".into(),
                timeout_ms: 1000,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    const BAD_NAMES: [&str; 5] = [
        "dlp\nINFO forged line",
        "dlp\r\nWARN x",
        "dlp\t",
        "dlp\u{1b}[31m",
        "dlp\u{85}",
    ];

    #[tokio::test]
    async fn create_rejects_control_characters_and_over_long_names_before_storage() {
        let governance = governance();
        let too_long = "n".repeat(MAX_HOOK_NAME_CHARS + 1);
        for name in BAD_NAMES.into_iter().chain([too_long.as_str()]) {
            let status = governance
                .grpc_create_hook_config(trusted(create(name)))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument, "{name:?}: {status:?}");
            assert!(status.message().starts_with("name must"), "{status:?}");
            assert!(!status.message().contains("dlp"), "{status:?}");
        }
        // 同一请求换成合规名称（含中文与空格、恰好 128 个字符）就过了校验，止于存储。
        for name in ["内容审核 v2", "n".repeat(MAX_HOOK_NAME_CHARS).as_str()] {
            let status = governance
                .grpc_create_hook_config(trusted(create(name)))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::Internal, "{name:?}: {status:?}");
        }
    }

    #[tokio::test]
    async fn renaming_follows_the_same_rule_and_keeping_the_name_does_not() {
        let governance = governance();
        let update = |name: &str| UpdateHookConfigRequest {
            hook_id: "7".into(),
            name: name.into(),
            ..Default::default()
        };
        for name in BAD_NAMES {
            let status = governance
                .grpc_update_hook_config(trusted(update(name)))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument, "{name:?}: {status:?}");
        }
        // 不改名（name 为空）或改成合规名称：照常去库里找这一行。
        for name in ["", "dlp-v2"] {
            let status = governance
                .grpc_update_hook_config(trusted(update(name)))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::Internal, "{name:?}: {status:?}");
        }
    }
}
