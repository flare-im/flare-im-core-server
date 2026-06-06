//! Hook 治理应用服务：配置 CRUD、统计、执行记录查询（由 `CapabilityService.Administer` 经 [`super::capability_administer`] 调用）。

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, CreateHookConfigResponse, DeleteHookConfigRequest,
    DeleteHookConfigResponse, GetHookConfigRequest, GetHookConfigResponse,
    GetHookStatisticsRequest, GetHookStatisticsResponse, HookConfig, HookExecution,
    HookRetryPolicy, HookSelector, HookStatistics, HookTransport, ListHookConfigsRequest,
    ListHookConfigsResponse, QueryHookExecutionsRequest, QueryHookExecutionsResponse,
    SetHookStatusRequest, SetHookStatusResponse, UpdateHookConfigRequest, UpdateHookConfigResponse,
};
use flare_server_core::error::Result as FlareResult;
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use flare_server_core::utils::require_ctx_from_request;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::composition::hook_registry::CoreHookRegistry;
use crate::domain::model::{HookConfigItem, HookSelectorConfig, HookTransportConfig};
use crate::infrastructure::persistence::postgres_config::PostgresHookConfigRepository;
use chrono::Utc;
use std::str::FromStr;

/// 从 gRPC 请求中提取租户 ID。
///
/// 优先从 Context 中提取，如果没有则返回 None
fn extract_tenant_id<T>(request: &Request<T>) -> Option<String> {
    if let Ok(ctx) = require_ctx_from_request(request) {
        ctx.tenant_id().map(|s: &str| s.to_string())
    } else {
        None
    }
}

/// Hook 治理（写/读模型入口），非独立 gRPC service。
pub struct HookServiceServer {
    repository: Arc<PostgresHookConfigRepository>,
    registry: Arc<CoreHookRegistry>,
    metrics_collector: Option<Arc<crate::infrastructure::monitoring::MetricsCollector>>,
    execution_recorder: Option<Arc<crate::infrastructure::monitoring::ExecutionRecorder>>,
}

impl HookServiceServer {
    pub fn new(
        repository: Arc<PostgresHookConfigRepository>,
        registry: Arc<CoreHookRegistry>,
    ) -> Self {
        Self {
            repository,
            registry,
            metrics_collector: None,
            execution_recorder: None,
        }
    }

    pub fn with_monitoring(
        mut self,
        metrics_collector: Arc<crate::infrastructure::monitoring::MetricsCollector>,
        execution_recorder: Arc<crate::infrastructure::monitoring::ExecutionRecorder>,
    ) -> Self {
        self.metrics_collector = Some(metrics_collector);
        self.execution_recorder = Some(execution_recorder);
        self
    }

    pub async fn grpc_create_hook_config(
        &self,
        request: Request<CreateHookConfigRequest>,
    ) -> Result<Response<CreateHookConfigResponse>, Status> {
        let ctx = require_ctx_from_request(&request)
            .map_err(|_| Status::internal("Context not found"))?;
        let req = request.into_inner();

        // 提取租户ID（优先从 Context，其次从请求参数）
        let tenant_id = if !req.tenant_id.is_empty() {
            req.tenant_id.clone()
        } else if let Some(tenant_id) = ctx.tenant_id() {
            tenant_id.to_string()
        } else {
            return Err(Status::invalid_argument("tenant_id is required"));
        };

        // 验证必需字段
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        if req.hook_type.is_empty() {
            return Err(Status::invalid_argument("hook_type is required"));
        }

        // 验证hook_type有效性
        let valid_hook_types = [
            "pre_send",
            "post_send",
            "delivery",
            "recall",
            "message_read",
            "message_reaction",
            "conversation_lifecycle",
            "conversation_member",
            "presence",
            "push_pre_send",
            "push_post_send",
            "push_delivery",
            "user_login",
            "user_logout",
            "user_online",
            "user_offline",
            "custom",
        ];
        if !valid_hook_types.contains(&req.hook_type.as_str()) {
            return Err(Status::invalid_argument(format!(
                "Invalid hook_type: {}. Valid types: {:?}",
                req.hook_type, valid_hook_types
            )));
        }

        if req.transport.is_none() {
            return Err(Status::invalid_argument("transport is required"));
        }

        // 转换protobuf类型到内部类型
        let hook_item = protobuf_to_hook_config_item(&req, None)
            .map_err(|e| Status::invalid_argument(format!("Invalid hook config: {}", e)))?;

        // 保存到数据库（优先从 Context 提取，其次从请求参数）
        let created_by = ctx.user_id().map(|s: &str| s).or(None);

        let hook_id = self
            .repository
            .save(
                Some(tenant_id.as_str()),
                &req.hook_type,
                &hook_item,
                created_by,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to save hook config: {}", e)))?;

        // 通知配置监听器重新加载配置
        self.registry
            .reload_config()
            .await
            .map_err(|e| Status::internal(format!("Failed to reload config: {}", e)))?;

        // 构建响应
        let hook_config = hook_config_item_to_protobuf(
            &hook_id.to_string(),
            &tenant_id,
            &req.hook_type,
            &hook_item,
        )
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
        if req.priority != 0 {
            hook_item.priority = req.priority;
        }
        if let Some(ref transport) = req.transport {
            hook_item.transport = match transport.r#type.as_str() {
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
                        crate::domain::model::LoadBalanceStrategy::from_str(&transport.load_balance)
                            .ok()
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
                        metadata: transport.metadata.clone(),
                    }
                }
                "webhook" => HookTransportConfig::Webhook {
                    endpoint: transport.endpoint.clone(),
                    secret: if transport.secret.is_empty() {
                        None
                    } else {
                        Some(transport.secret.clone())
                    },
                    headers: transport.headers.clone(),
                },
                "local" => HookTransportConfig::Local {
                    target: transport.target.clone(),
                },
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "Unsupported transport type: {}",
                        transport.r#type
                    )));
                }
            };
            hook_item.timeout_ms = transport.timeout_ms as u64;
        }
        if let Some(ref selector) = req.selector {
            hook_item.selector = HookSelectorConfig {
                tenants: selector.tenants.clone(),
                conversation_types: selector.conversation_types.clone(),
                message_types: selector.message_types.clone(),
                user_ids: vec![],
                tags: std::collections::HashMap::new(),
            };
        }
        if let Some(ref retry_policy) = req.retry_policy {
            hook_item.max_retries = retry_policy.max_retries as u32;
            // 根据backoff_strategy推断error_policy（retry策略通常意味着容错）
            // 如果没有重试策略或重试次数为0，则使用fail_fast
            if retry_policy.max_retries > 0 {
                hook_item.error_policy = "retry".to_string();
            } else {
                hook_item.error_policy = "fail_fast".to_string();
            }
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

        // 通知配置监听器重新加载配置
        self.registry
            .reload_config()
            .await
            .map_err(|e| Status::internal(format!("Failed to reload config: {}", e)))?;

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
        let ctx = require_ctx_from_request(&request).ok();
        let req = request.into_inner();

        // 提取租户ID（优先从 Context，其次从请求参数）
        let tenant_id = if !req.tenant_id.is_empty() {
            Some(req.tenant_id.clone())
        } else if let Some(ref ctx) = ctx {
            ctx.tenant_id().map(|s: &str| s.to_string())
        } else {
            None
        };

        // 查询Hook配置（支持enabled_only过滤和租户过滤）
        let hook_type_filter = if req.hook_type.is_empty() {
            None
        } else {
            Some(req.hook_type.as_str())
        };

        let rows = self
            .repository
            .query(tenant_id.as_deref(), hook_type_filter, req.enabled_only)
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
            if let Ok(Some((row, _))) = self.repository.get_by_id(id).await {
                self.repository
                    .delete(row.tenant_id.as_deref(), &row.hook_type, &row.name)
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

        // 通知配置监听器重新加载配置
        self.registry
            .reload_config()
            .await
            .map_err(|e| Status::internal(format!("Failed to reload config: {}", e)))?;

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

        let hook_id = if let Ok(id) = hook_id_parsed {
            id
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

            row.id
        };

        // 更新数据库中的enabled字段
        let updated = self
            .repository
            .update_enabled(hook_id, req.enabled)
            .await
            .map_err(|e| Status::internal(format!("Failed to update hook status: {}", e)))?;

        if !updated {
            return Err(Status::not_found("Hook config not found"));
        }

        // 通知配置监听器重新加载配置
        self.registry
            .reload_config()
            .await
            .map_err(|e| Status::internal(format!("Failed to reload config: {}", e)))?;

        Ok(Response::new(SetHookStatusResponse { success: true }))
    }

    pub async fn grpc_get_hook_statistics(
        &self,
        request: Request<GetHookStatisticsRequest>,
    ) -> Result<Response<GetHookStatisticsResponse>, Status> {
        // 先提取租户ID（在into_inner()之前）
        let tenant_id = extract_tenant_id(&request);
        let req = request.into_inner();

        if req.hook_id.is_empty() {
            return Err(Status::invalid_argument("hook_id is required"));
        }

        // 解析hook_id（格式：hook_type:name 或 id）
        let hook_id_parsed = req.hook_id.parse::<i64>();

        let hook_id = if let Ok(id) = hook_id_parsed {
            // 作为数字ID使用
            id
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

            row.id
        };

        // 从监控系统查询统计数据
        let statistics = if let Some(ref metrics_collector) = self.metrics_collector {
            // 根据hook_id查询统计信息
            // hook_id可能是数字ID或hook_type:name格式，需要转换为hook名称
            let hook_name = if hook_id_parsed.is_ok() {
                // 如果是数字ID，需要先查询获取hook名称
                if let Ok(Some((row, _))) = self.repository.get_by_id(hook_id).await {
                    format!("{}:{}", row.hook_type, row.name)
                } else {
                    hook_id.to_string()
                }
            } else {
                // 如果是hook_type:name格式，直接使用
                req.hook_id.clone()
            };

            // 从MetricsCollector查询统计数据
            if let Some(stats) = metrics_collector.get_statistics(&hook_name).await {
                // 将domain model转换为protobuf类型
                domain_to_protobuf_statistics(hook_id.to_string(), &stats)
            } else {
                // 没有统计数据，返回空统计数据
                HookStatistics {
                    hook_id: hook_id.to_string(),
                    total_executions: 0,
                    success_count: 0,
                    failure_count: 0,
                    avg_latency_ms: 0.0,
                    p99_latency_ms: 0.0,
                    rate_limit_count: 0,
                    circuit_break_count: 0,
                    error_count_by_code: std::collections::HashMap::new(),
                }
            }
        } else {
            // 没有监控系统，返回空统计数据
            HookStatistics {
                hook_id: hook_id.to_string(),
                total_executions: 0,
                success_count: 0,
                failure_count: 0,
                avg_latency_ms: 0.0,
                p99_latency_ms: 0.0,
                rate_limit_count: 0,
                circuit_break_count: 0,
                error_count_by_code: std::collections::HashMap::new(),
            }
        };

        Ok(Response::new(GetHookStatisticsResponse {
            statistics: Some(statistics),
        }))
    }

    pub async fn grpc_query_hook_executions(
        &self,
        request: Request<QueryHookExecutionsRequest>,
    ) -> Result<Response<QueryHookExecutionsResponse>, Status> {
        let req = request.into_inner();

        // 从执行记录器查询历史记录
        let executions = if let Some(ref execution_recorder) = self.execution_recorder {
            // 解析hook_id（格式：hook_type:name 或 id）
            let hook_name = if !req.hook_id.is_empty() {
                let hook_id_parsed = req.hook_id.parse::<i64>();
                if let Ok(id) = hook_id_parsed {
                    // 如果是数字ID，需要先查询获取hook名称
                    if let Ok(Some((row, _))) = self.repository.get_by_id(id).await {
                        Some(format!("{}:{}", row.hook_type, row.name))
                    } else {
                        None
                    }
                } else {
                    // 如果是hook_type:name格式，直接使用
                    Some(req.hook_id.clone())
                }
            } else {
                None
            };

            // 确定查询限制
            let limit = req
                .pagination
                .as_ref()
                .map(|p| p.limit as usize)
                .unwrap_or(100)
                .min(1000); // 最多1000条

            // 从ExecutionRecorder查询执行记录
            let records = execution_recorder.query(hook_name.as_deref(), limit).await;

            // 过滤条件
            let filtered_records = records
                .into_iter()
                .filter(|r| {
                    // 如果指定了message_id，需要匹配（但domain model中没有message_id，暂时跳过）
                    // if !req.message_id.is_empty() && r.message_id != req.message_id {
                    //     return false;
                    // }

                    // 如果只查询成功的，过滤失败的
                    if req.success_only && !r.success {
                        return false;
                    }

                    // 时间范围过滤
                    if let Some(ref time_range) = req.time_range {
                        let executed_at_ms = r
                            .executed_at
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or_default();

                        // 检查是否在时间范围内
                        if let Some(start) = time_range.start_time
                            && executed_at_ms < start
                        {
                            return false;
                        }
                        if let Some(end) = time_range.end_time
                            && executed_at_ms > end
                        {
                            return false;
                        }
                    }

                    true
                })
                .collect::<Vec<_>>();

            // 转换为protobuf类型
            filtered_records
                .into_iter()
                .enumerate()
                .map(|(idx, record)| {
                    domain_to_protobuf_execution(
                        format!("exec-{}", idx), // execution_id
                        req.hook_id.clone(),     // hook_id
                        String::new(),           // message_id (domain model中没有，暂时为空)
                        record,
                    )
                })
                .collect()
        } else {
            // 没有执行记录器，返回空列表
            vec![]
        };

        Ok(Response::new(QueryHookExecutionsResponse {
            executions,
            pagination: req.pagination,
        }))
    }
}

/// 将统计数据转换为protobuf类型
fn domain_to_protobuf_statistics(
    hook_id: String,
    stats: &crate::domain::model::HookStatistics,
) -> HookStatistics {
    HookStatistics {
        hook_id,
        total_executions: stats.total_count as i64,
        success_count: stats.success_count as i64,
        failure_count: stats.failure_count as i64,
        avg_latency_ms: stats.avg_latency_ms,
        p99_latency_ms: 0.0,    // 暂时不计算P99延迟
        rate_limit_count: 0,    // 暂时不统计限流次数
        circuit_break_count: 0, // 暂时不统计熔断次数
        error_count_by_code: std::collections::HashMap::new(), // 暂时不统计错误码
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

    let selector = req
        .selector
        .as_ref()
        .map(|s| HookSelectorConfig {
            tenants: s.tenants.clone(),
            conversation_types: s.conversation_types.clone(),
            message_types: s.message_types.clone(),
            user_ids: vec![],
            tags: std::collections::HashMap::new(),
        })
        .unwrap_or_default();

    // 根据transport类型创建HookTransportConfig
    let transport_config = match transport.r#type.as_str() {
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
                metadata: transport.metadata.clone(),
            }
        }
        "webhook" => HookTransportConfig::Webhook {
            endpoint: transport.endpoint.clone(),
            secret: if transport.secret.is_empty() {
                None
            } else {
                Some(transport.secret.clone())
            },
            headers: transport.headers.clone(),
        },
        "local" => HookTransportConfig::Local {
            target: transport.target.clone(),
        },
        _ => {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "Unsupported transport type",
            )
            .details(transport.r#type.clone())
            .build_error());
        }
    };

    let retry_policy = req.retry_policy.as_ref();
    let max_retries = retry_policy.map(|p| p.max_retries as u32).unwrap_or(0);

    // 根据retry_policy推断error_policy
    // 如果有重试策略且max_retries > 0，则使用retry；否则使用fail_fast
    let error_policy = if max_retries > 0 {
        "retry".to_string()
    } else {
        "fail_fast".to_string()
    };

    Ok(HookConfigItem {
        name: req.name.clone(),
        version: None,
        description: None,
        enabled: true,
        priority: req.priority,
        group: None,
        timeout_ms: transport.timeout_ms as u64,
        max_retries,
        error_policy,
        require_success: true,
        selector,
        transport: transport_config,
        metadata: std::collections::HashMap::new(),
    })
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
        priority: item.priority,
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
                target: String::new(),
                timeout_ms: item.timeout_ms as i32,
                metadata: metadata.clone(),
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
                secret: secret.clone().unwrap_or_default(),
                headers: headers.clone(),
                target: String::new(),
                timeout_ms: item.timeout_ms as i32,
                metadata: std::collections::HashMap::new(),
            },
            HookTransportConfig::Local { target } => HookTransport {
                r#type: "local".to_string(),
                service_name: String::new(),
                endpoint: String::new(),
                registry_type: String::new(),
                namespace: String::new(),
                load_balance: String::new(),
                secret: String::new(),
                headers: std::collections::HashMap::new(),
                target: target.clone(),
                timeout_ms: item.timeout_ms as i32,
                metadata: std::collections::HashMap::new(),
            },
        }),
        selector: Some(HookSelector {
            tenants: item.selector.tenants.clone(),
            conversation_types: item.selector.conversation_types.clone(),
            message_types: item.selector.message_types.clone(),
            business_types: vec![], // 暂时不支持business_types
            hook_points: vec![],
            app_ids: vec![],
            conversation_ids: vec![],
        }),
        retry_policy: Some(HookRetryPolicy {
            max_retries: item.max_retries as i32,
            retry_interval_ms: 1000,                     // 默认值
            backoff_strategy: "exponential".to_string(), // 默认值
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
        hook_points: vec![],
        config_extensions: None,
    })
}

/// 将执行结果转换为protobuf类型
fn domain_to_protobuf_execution(
    execution_id: String,
    hook_id: String,
    message_id: String,
    result: crate::domain::model::HookExecutionResult,
) -> HookExecution {
    use std::time::SystemTime;

    // 将执行时间转换为Timestamp
    let executed_at = result
        .executed_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| prost_types::Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        })
        .unwrap_or_else(|_| prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        });

    // 解析错误信息
    let (_error_code, _error_message) = if let Some(ref err) = result.error_message {
        // 尝试从错误信息中提取错误码（格式：CODE: message）
        let parts: Vec<&str> = err.splitn(2, ':').collect();
        if parts.len() == 2 {
            (parts[0].trim().to_string(), parts[1].trim().to_string())
        } else {
            ("UNKNOWN".to_string(), err.clone())
        }
    } else {
        (String::new(), String::new())
    };

    HookExecution {
        execution_id,
        hook_id,
        message_id,
        success: result.success,
        latency_ms: result.latency_ms as i32,
        error_code: String::new(), // 暂时不填充错误码
        error_message: result.error_message.clone().unwrap_or_default(),
        executed_at: Some(executed_at),
    }
}
