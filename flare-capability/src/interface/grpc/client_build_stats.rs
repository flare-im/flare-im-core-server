//! `flare.control.v1.ClientBuildStats` gRPC 实现：平台读一个应用在各接入网关上的（平台，构建）
//! 在线连接数（版本分布）。
//!
//! 鉴权：平台 worker 的控制断言（`FLARE_PLATFORM_PUBLIC_KEY`，主体 `flare-platform-worker`），
//! audience 是只读的 `flare-core-build-stats`（`CORE_BUILD_STATS_AUDIENCE`），与写投影的
//! `flare-core-projection` 互不通用。令牌只能针对一个应用：租户必须正是请求的 `app_id`，平台级 `*`
//! 拒绝。数据来自网关每 30s 写进 token_store Redis 的快照，最多滞后一个上报周期。

use std::sync::Arc;

use flare_grpc_proto::control::client_build_stats_server::ClientBuildStats;
use flare_grpc_proto::control::{
    ConnectionBuildCount, ConnectionBuilds, GetConnectionBuildsRequest,
};
use flare_im_core_service_kit::control_auth::{
    CORE_BUILD_STATS_AUDIENCE, PlatformActorKind, PlatformAssertionVerifier,
    require_platform_assertion_for,
};
use tonic::{Request, Response, Status};

use crate::domain::repository::connection_builds::{
    ConnectionBuildStats as ConnectionBuildSource, ConnectionBuildSummary,
};

#[derive(Clone)]
pub struct ClientBuildStatsGrpcServer {
    source: Arc<dyn ConnectionBuildSource>,
    control_verifier: Option<PlatformAssertionVerifier>,
}

impl ClientBuildStatsGrpcServer {
    pub fn new(
        source: Arc<dyn ConnectionBuildSource>,
        control_verifier: Option<PlatformAssertionVerifier>,
    ) -> Self {
        Self {
            source,
            control_verifier,
        }
    }
}

fn response(app_id: String, summary: ConnectionBuildSummary, now_ms: i64) -> ConnectionBuilds {
    ConnectionBuilds {
        app_id,
        builds: summary
            .rows
            .into_iter()
            .map(|row| ConnectionBuildCount {
                platform: row.platform,
                app_build: row.app_build.unwrap_or(0),
                connections: row.connections,
                older_builds: row.app_build.is_none(),
            })
            .collect(),
        gateways: u32::try_from(summary.gateways).unwrap_or(u32::MAX),
        collected_at_ms: now_ms,
    }
}

#[tonic::async_trait]
impl ClientBuildStats for ClientBuildStatsGrpcServer {
    async fn get_connection_builds(
        &self,
        request: Request<GetConnectionBuildsRequest>,
    ) -> Result<Response<ConnectionBuilds>, Status> {
        let actor = require_platform_assertion_for(
            &request,
            self.control_verifier.as_ref(),
            CORE_BUILD_STATS_AUDIENCE,
        )?;
        actor.require_kind(PlatformActorKind::ProjectionWorker)?;
        let app_id = request.into_inner().app_id;
        if !flare_im_core_contracts::domain::tenant::is_valid_tenant_id(&app_id)
            || app_id != flare_im_core_contracts::utils::normalize_tenant_id(&app_id)
        {
            return Err(Status::invalid_argument("app_id must be canonical"));
        }
        // 一张令牌只读一个应用：平台级 `*` 在这里不算数。
        actor.require_tenant(&app_id)?;
        let now_ms = now_ms();
        let summary = self
            .source
            .tenant_builds(&app_id, now_ms)
            .await
            .map_err(|_| Status::unavailable("connection build distribution unavailable"))?;
        Ok(Response::new(response(app_id, summary, now_ms)))
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_core_service_kit::gateway::connection_builds::ConnectionBuildRow;
    use std::sync::Mutex;

    /// 记下被问的租户，回答固定的汇总。
    #[derive(Default)]
    struct FixedSource {
        asked: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl ConnectionBuildSource for FixedSource {
        async fn tenant_builds(
            &self,
            tenant_id: &str,
            _now_ms: i64,
        ) -> flare_server_core::error::Result<ConnectionBuildSummary> {
            self.asked.lock().unwrap().push(tenant_id.to_string());
            Ok(ConnectionBuildSummary {
                rows: vec![
                    ConnectionBuildRow {
                        platform: "android".to_string(),
                        app_build: Some(201),
                        connections: 5,
                    },
                    ConnectionBuildRow {
                        platform: "android".to_string(),
                        app_build: None,
                        connections: 2,
                    },
                ],
                gateways: 3,
            })
        }
    }

    fn server() -> (Arc<FixedSource>, ClientBuildStatsGrpcServer) {
        let source = Arc::new(FixedSource::default());
        let key = SigningKey::from_bytes(&[71; 32]);
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let verifier = PlatformAssertionVerifier::from_public_key(&public).unwrap();
        (
            source.clone(),
            ClientBuildStatsGrpcServer::new(source, Some(verifier)),
        )
    }

    fn signed<T>(payload: T, tenant_id: &str, subject: &str) -> Request<T> {
        signed_for(
            payload,
            tenant_id,
            subject,
            flare_im_core_service_kit::control_auth::CORE_BUILD_STATS_AUDIENCE,
        )
    }

    fn signed_for<T>(payload: T, tenant_id: &str, subject: &str, audience: &str) -> Request<T> {
        let key = SigningKey::from_bytes(&[71; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let signer = flare_server_core::TokenService::from_ed25519_private_key(
            &private,
            flare_im_core_service_kit::control_auth::PLATFORM_ISSUER,
            60,
        )
        .unwrap();
        let token = signer
            .generate_with_ed25519(subject, Some(tenant_id), "control", audience)
            .unwrap();
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    fn ask(app_id: &str) -> GetConnectionBuildsRequest {
        GetConnectionBuildsRequest {
            app_id: app_id.to_string(),
        }
    }

    const WORKER: &str = flare_im_core_service_kit::control_auth::PROJECTION_WORKER_SUBJECT;

    #[tokio::test]
    async fn the_distribution_maps_the_older_total_to_a_flagged_row() {
        let (source, server) = server();
        let builds = server
            .get_connection_builds(signed(ask("t1"), "t1", WORKER))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(builds.app_id, "t1");
        assert_eq!(builds.gateways, 3);
        assert!(builds.collected_at_ms > 0);
        assert_eq!(
            builds.builds,
            vec![
                ConnectionBuildCount {
                    platform: "android".to_string(),
                    app_build: 201,
                    connections: 5,
                    older_builds: false,
                },
                ConnectionBuildCount {
                    platform: "android".to_string(),
                    app_build: 0,
                    connections: 2,
                    older_builds: true,
                },
            ]
        );
        assert_eq!(*source.asked.lock().unwrap(), vec!["t1".to_string()]);
    }

    #[tokio::test]
    async fn platform_wide_and_projection_credentials_cannot_read_the_distribution() {
        let (source, server) = server();
        let status = server
            .get_connection_builds(signed(ask("t1"), "*", WORKER))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        let status = server
            .get_connection_builds(signed_for(
                ask("t1"),
                "t1",
                WORKER,
                flare_im_core_service_kit::control_auth::CORE_PROJECTION_AUDIENCE,
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(source.asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn other_tenants_unsigned_calls_and_bad_ids_are_refused() {
        let (source, server) = server();
        let status = server
            .get_connection_builds(signed(ask("t2"), "t1", WORKER))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        let status = server
            .get_connection_builds(Request::new(ask("t1")))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);

        let status = server
            .get_connection_builds(signed(
                ask("t1"),
                "t1",
                flare_im_core_service_kit::control_auth::SCHEDULER_SUBJECT,
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        let status = server
            .get_connection_builds(signed(ask("bad id"), "*", WORKER))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(source.asked.lock().unwrap().is_empty());
    }
}
