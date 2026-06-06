//! 路由仓储接口（SVID → 业务端点）

use flare_server_core::error::Result;

use crate::domain::model::route::Route;

/// 路由仓储接口
///
/// 使用 Rust 2024 原生 async fn in traits
pub trait RouteRepository: Send + Sync {
    /// 保存路由
    async fn save(&self, route: Route) -> Result<()>;

    /// 根据服务 ID 查找路由
    async fn find_by_svid(&self, svid: &str) -> Result<Option<Route>>;

    /// 删除路由
    async fn delete(&self, svid: &str) -> Result<()>;
}

/// Noop 路由仓储实现（用于不需要路由查询的场景）
pub struct NoopRouteRepository;

impl RouteRepository for NoopRouteRepository {
    async fn save(&self, _route: Route) -> Result<()> {
        Ok(())
    }

    async fn find_by_svid(&self, _svid: &str) -> Result<Option<Route>> {
        Ok(None)
    }

    async fn delete(&self, _svid: &str) -> Result<()> {
        Ok(())
    }
}

/// 默认的路由仓储类型
pub type DefaultRouteRepository = NoopRouteRepository;
