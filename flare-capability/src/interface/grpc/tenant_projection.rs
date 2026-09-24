//! `flare.control.v1.TenantProjection` gRPC 实现（控制面 → 核的租户投影接收端）。
//!
//! - `UpsertTenant`：校验 → 按 version 幂等写 `tenants`；旧版本返回 `accepted=false`。
//! - `GetVersions`：对账；核不消费功能开关投影，`features_version` 恒为 0。
//! - `UpsertFeatureFlags`：核只有能力开关（`CapabilityService.SetTenantCapabilitySwitch`），返回 `Unimplemented`。
//!
//! 投影内容本身不做管理面鉴权：本服务只在内网监听，与控制面之间靠网络边界隔离（与
//! `CapabilityService` 的 `Register/Deregister` 同一约定）。

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::control::tenant_projection_server::TenantProjection;
use flare_grpc_proto::control::{
    FeatureFlagsProjectionUpsert, GetVersionsRequest, ProjectionAck, ProjectionVersions,
    TenantProjectionUpsert, TenantVersions,
};
use flare_server_core::error::grpc::IntoGrpc;
use tonic::{Request, Response, Status};

use crate::application::commands::apply_tenant_projection;
use crate::application::queries::tenant_projection_versions;
use crate::domain::model::TenantProjectionRecord;
use crate::domain::repository::TenantProjectionRepository;

pub struct TenantProjectionGrpcServer<R: TenantProjectionRepository + 'static> {
    repository: Arc<R>,
}

impl<R: TenantProjectionRepository + 'static> TenantProjectionGrpcServer<R> {
    pub fn new(repository: Arc<R>) -> Self {
        Self { repository }
    }
}

impl<R: TenantProjectionRepository + 'static> Clone for TenantProjectionGrpcServer<R> {
    fn clone(&self) -> Self {
        Self {
            repository: Arc::clone(&self.repository),
        }
    }
}

#[tonic::async_trait]
impl<R: TenantProjectionRepository + 'static> TenantProjection for TenantProjectionGrpcServer<R> {
    async fn upsert_tenant(
        &self,
        request: Request<TenantProjectionUpsert>,
    ) -> Result<Response<ProjectionAck>, Status> {
        let record = TenantProjectionRecord::from_proto(request.get_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let outcome = apply_tenant_projection(self.repository.as_ref(), &record)
            .await
            .into_grpc()?;
        Ok(Response::new(ProjectionAck {
            accepted: outcome.accepted,
            current_version: outcome.current_version,
        }))
    }

    async fn upsert_feature_flags(
        &self,
        _request: Request<FeatureFlagsProjectionUpsert>,
    ) -> Result<Response<ProjectionAck>, Status> {
        Err(Status::unimplemented(
            "flare-im-core does not consume feature flag projections; core capability switches go through CapabilityService.SetTenantCapabilitySwitch",
        ))
    }

    async fn get_versions(
        &self,
        request: Request<GetVersionsRequest>,
    ) -> Result<Response<ProjectionVersions>, Status> {
        let ids: Vec<String> = request
            .into_inner()
            .im_tenant_ids
            .into_iter()
            .map(flare_im_contracts::utils::normalize_tenant_id)
            .collect();
        let versions = tenant_projection_versions(self.repository.as_ref(), &ids)
            .await
            .into_grpc()?;
        let versions: HashMap<String, TenantVersions> = versions
            .into_iter()
            .map(|(tenant_id, tenant_version)| {
                (
                    tenant_id,
                    TenantVersions {
                        tenant_version,
                        features_version: 0,
                    },
                )
            })
            .collect();
        Ok(Response::new(ProjectionVersions { versions }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::TenantProjectionOutcome;
    use flare_grpc_proto::control::TenantStatus as ProtoStatus;
    use flare_server_core::error::Result as FlareResult;
    use std::sync::Mutex;

    /// 内存仓储：复刻「只接受更高版本」的写规则，用来验证接口层的幂等语义。
    #[derive(Default)]
    struct MemoryRepo {
        rows: Mutex<HashMap<String, (TenantProjectionRecord, u64)>>,
    }

    #[async_trait::async_trait]
    impl TenantProjectionRepository for MemoryRepo {
        async fn upsert_if_newer(
            &self,
            record: &TenantProjectionRecord,
        ) -> FlareResult<TenantProjectionOutcome> {
            let mut rows = self.rows.lock().unwrap();
            let current = rows.get(&record.tenant_id).map(|(_, v)| *v).unwrap_or(0);
            if record.version > current {
                rows.insert(record.tenant_id.clone(), (record.clone(), record.version));
                Ok(TenantProjectionOutcome {
                    accepted: true,
                    current_version: record.version,
                })
            } else {
                Ok(TenantProjectionOutcome {
                    accepted: false,
                    current_version: current,
                })
            }
        }

        async fn versions(&self, tenant_ids: &[String]) -> FlareResult<Vec<(String, u64)>> {
            let rows = self.rows.lock().unwrap();
            Ok(rows
                .iter()
                .filter(|(id, _)| tenant_ids.is_empty() || tenant_ids.contains(id))
                .map(|(id, (_, v))| (id.clone(), *v))
                .collect())
        }
    }

    fn upsert(tenant: &str, status: ProtoStatus, version: u64) -> TenantProjectionUpsert {
        TenantProjectionUpsert {
            im_tenant_id: tenant.to_string(),
            name: tenant.to_string(),
            status: status as i32,
            quota: None,
            settings_json: Vec::new(),
            version,
        }
    }

    fn server() -> (Arc<MemoryRepo>, TenantProjectionGrpcServer<MemoryRepo>) {
        let repo = Arc::new(MemoryRepo::default());
        (repo.clone(), TenantProjectionGrpcServer::new(repo))
    }

    #[tokio::test]
    async fn upsert_is_idempotent_by_version() {
        let (repo, server) = server();

        let ack = server
            .upsert_tenant(Request::new(upsert("acme", ProtoStatus::Active, 3)))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.current_version, 3);

        // 同版本重放：丢弃，报告本地版本。
        let ack = server
            .upsert_tenant(Request::new(upsert("acme", ProtoStatus::Suspended, 3)))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.accepted);
        assert_eq!(ack.current_version, 3);

        // 旧版本乱序到达：丢弃。
        let ack = server
            .upsert_tenant(Request::new(upsert("acme", ProtoStatus::Suspended, 2)))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.accepted);
        assert_eq!(ack.current_version, 3);
        assert_eq!(
            repo.rows.lock().unwrap()["acme"].0.status,
            flare_im_contracts::domain::tenant::TenantStatus::Active
        );

        // 更高版本：生效。
        let ack = server
            .upsert_tenant(Request::new(upsert("acme", ProtoStatus::Suspended, 4)))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.current_version, 4);
        assert_eq!(
            repo.rows.lock().unwrap()["acme"].0.status,
            flare_im_contracts::domain::tenant::TenantStatus::Suspended
        );
    }

    #[tokio::test]
    async fn invalid_projection_is_invalid_argument() {
        let (_, server) = server();
        let status = server
            .upsert_tenant(Request::new(upsert("acme", ProtoStatus::Active, 0)))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let status = server
            .upsert_tenant(Request::new(upsert("*", ProtoStatus::Active, 1)))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_versions_reports_tenant_version_and_zero_features_version() {
        let (_, server) = server();
        server
            .upsert_tenant(Request::new(upsert("a", ProtoStatus::Active, 5)))
            .await
            .unwrap();
        server
            .upsert_tenant(Request::new(upsert("b", ProtoStatus::Active, 8)))
            .await
            .unwrap();

        let all = server
            .get_versions(Request::new(GetVersionsRequest {
                im_tenant_ids: vec![],
            }))
            .await
            .unwrap()
            .into_inner()
            .versions;
        assert_eq!(all.len(), 2);
        assert_eq!(all["a"].tenant_version, 5);
        assert_eq!(all["a"].features_version, 0);

        let some = server
            .get_versions(Request::new(GetVersionsRequest {
                im_tenant_ids: vec!["b".to_string(), "missing".to_string()],
            }))
            .await
            .unwrap()
            .into_inner()
            .versions;
        assert_eq!(some.len(), 1);
        assert_eq!(some["b"].tenant_version, 8);
    }

    #[tokio::test]
    async fn feature_flags_are_unimplemented() {
        let (_, server) = server();
        let status = server
            .upsert_feature_flags(Request::new(FeatureFlagsProjectionUpsert::default()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }
}
