//! 查询类型与查询服务（读侧 CQRS）

pub struct UserConnectionsQuery {
    pub user_id: String,
    pub platforms: Vec<String>,
    pub limit: i32,
}

impl UserConnectionsQuery {
    pub fn new(user_id: String, platforms: Vec<String>, limit: i32) -> Self {
        Self { user_id, platforms, limit }
    }
}

