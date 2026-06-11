//! User capability grant value object.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCapabilityGrant {
    pub tenant_id: String,
    pub user_id: String,
    pub capability_id: String,
    pub granted_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub plan_code: Option<String>,
    pub source: Option<String>,
}

impl UserCapabilityGrant {
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        match self.expires_at {
            Some(expires_at) => expires_at > now,
            None => true,
        }
    }
}
