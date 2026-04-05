//! 连接查询处理器

use std::sync::Arc;

use flare_im_core::error::Result;

use crate::application::queries::UserConnectionsQuery;
use crate::domain::model::ConnectionInfo;
use crate::domain::ports::IConnectionPort;

pub struct ConnectionQueryHandler {
    connection_port: Arc<dyn IConnectionPort>,
}

impl ConnectionQueryHandler {
    pub fn new(connection_port: Arc<dyn IConnectionPort>) -> Self {
        Self { connection_port }
    }
}

impl ConnectionQueryHandler {
    pub async fn query_user_connections(
        &self,
        query: UserConnectionsQuery,
    ) -> Result<Vec<ConnectionInfo>> {
        self.connection_port
            .list_user_connections(&query.user_id)
            .await
    }
}
