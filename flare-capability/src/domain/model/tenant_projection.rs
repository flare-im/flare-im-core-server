//! 租户投影（控制面 → 核）的领域记录与校验。
//!
//! 核不解释 `settings_json`，只透传存进 `tenants.config`；核只执行 `quota.core`，其余配额原样存储。

use flare_im_contracts::domain::tenant::{
    TenantCoreQuota, TenantQuota, TenantStatus, is_valid_tenant_id,
};
use flare_im_contracts::utils::normalize_tenant_id;

/// 一条待写入的租户投影。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantProjectionRecord {
    pub tenant_id: String,
    pub name: String,
    pub status: TenantStatus,
    pub quota: TenantQuota,
    /// 控制面 `settings_json`（已校验为 JSON 对象文本；空 → `{}`）。
    pub settings_json: String,
    /// 控制面全局单调版本，必须 ≥ 1。
    pub version: u64,
}

/// 校验失败原因（映射为 gRPC `InvalidArgument`）。
///
/// 手写 `Display` 而不派生 thiserror：IM 生产代码统一走 flare_server_core 的错误基座，
/// 架构测试 `error_boundary` 禁止在这里引入第二套错误宏。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantProjectionValidationError {
    InvalidTenantId(String),
    InvalidStatus(i32),
    ZeroVersion,
    InvalidSettings(String),
}

impl std::fmt::Display for TenantProjectionValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTenantId(id) => {
                write!(
                    f,
                    "im_tenant_id must match ^[0-9A-Za-z_-]{{1,32}}$ (got `{id}`)"
                )
            }
            Self::InvalidStatus(status) => {
                write!(
                    f,
                    "status must be ACTIVE / SUSPENDED / DELETING (got {status})"
                )
            }
            Self::ZeroVersion => f.write_str("version must be >= 1"),
            Self::InvalidSettings(reason) => {
                write!(f, "settings_json must be a JSON object: {reason}")
            }
        }
    }
}

impl std::error::Error for TenantProjectionValidationError {}

impl TenantProjectionRecord {
    /// 从 `flare.control.v1.TenantProjectionUpsert` 构造并校验。
    pub fn from_proto(
        upsert: &flare_grpc_proto::control::TenantProjectionUpsert,
    ) -> Result<Self, TenantProjectionValidationError> {
        use flare_grpc_proto::control::TenantStatus as ProtoStatus;

        let tenant_id = normalize_tenant_id(&upsert.im_tenant_id);
        if !is_valid_tenant_id(&tenant_id) {
            return Err(TenantProjectionValidationError::InvalidTenantId(
                upsert.im_tenant_id.clone(),
            ));
        }
        let status = match ProtoStatus::try_from(upsert.status) {
            Ok(ProtoStatus::Active) => TenantStatus::Active,
            Ok(ProtoStatus::Suspended) => TenantStatus::Suspended,
            Ok(ProtoStatus::Deleting) => TenantStatus::Deleting,
            _ => {
                return Err(TenantProjectionValidationError::InvalidStatus(
                    upsert.status,
                ));
            }
        };
        if upsert.version == 0 {
            return Err(TenantProjectionValidationError::ZeroVersion);
        }
        let settings_json = if upsert.settings_json.is_empty() {
            "{}".to_string()
        } else {
            let value: serde_json::Value = serde_json::from_slice(&upsert.settings_json)
                .map_err(|e| TenantProjectionValidationError::InvalidSettings(e.to_string()))?;
            if !value.is_object() {
                return Err(TenantProjectionValidationError::InvalidSettings(
                    "top-level value is not an object".to_string(),
                ));
            }
            value.to_string()
        };
        let quota = upsert
            .quota
            .as_ref()
            .map(|q| TenantQuota {
                max_users: q.max_users,
                max_groups: q.max_groups,
                max_group_members: q.max_group_members,
                core: q
                    .core
                    .as_ref()
                    .map(|c| TenantCoreQuota {
                        send_qps: c.send_qps,
                        sync_pull_qps: c.sync_pull_qps,
                        max_online_devices_per_user: c.max_online_devices_per_user,
                    })
                    .unwrap_or_default(),
            })
            .unwrap_or_default();

        Ok(Self {
            tenant_id,
            name: upsert.name.trim().to_string(),
            status,
            quota,
            settings_json,
            version: upsert.version,
        })
    }

    /// `tenants.quota` 列的 JSON 文本（固定 schema，与 Social 侧一致）。
    pub fn quota_json(&self) -> String {
        serde_json::to_string(&self.quota).unwrap_or_else(|_| "{}".to_string())
    }
}

/// 幂等写入结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantProjectionOutcome {
    /// `true` = 本次写入生效；`false` = 本地版本不低于来件，来件被丢弃。
    pub accepted: bool,
    /// 写入后（或丢弃时）本地持有的版本。
    pub current_version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::control::{CoreQuota, TenantProjectionUpsert, TenantQuota as ProtoQuota};

    fn upsert() -> TenantProjectionUpsert {
        TenantProjectionUpsert {
            im_tenant_id: "acme".to_string(),
            name: " Acme ".to_string(),
            status: flare_grpc_proto::control::TenantStatus::Active as i32,
            quota: Some(ProtoQuota {
                max_users: 100,
                max_groups: 0,
                max_group_members: 500,
                core: Some(CoreQuota {
                    send_qps: 50,
                    sync_pull_qps: 20,
                    max_online_devices_per_user: 3,
                }),
            }),
            settings_json: br#"{"locale":"zh-CN"}"#.to_vec(),
            version: 7,
        }
    }

    #[test]
    fn converts_and_normalizes() {
        let record = TenantProjectionRecord::from_proto(&upsert()).unwrap();
        assert_eq!(record.tenant_id, "acme");
        assert_eq!(record.name, "Acme");
        assert_eq!(record.status, TenantStatus::Active);
        assert_eq!(record.quota.core.send_qps, 50);
        assert_eq!(record.quota.max_group_members, 500);
        assert_eq!(record.version, 7);
        assert_eq!(record.settings_json, r#"{"locale":"zh-CN"}"#);
        let quota: serde_json::Value = serde_json::from_str(&record.quota_json()).unwrap();
        assert_eq!(quota["core"]["sync_pull_qps"], 20);
    }

    #[test]
    fn default_tenant_aliases_normalize_to_zero() {
        let mut u = upsert();
        u.im_tenant_id = "default".to_string();
        assert_eq!(
            TenantProjectionRecord::from_proto(&u).unwrap().tenant_id,
            "0"
        );
        u.im_tenant_id = String::new();
        assert_eq!(
            TenantProjectionRecord::from_proto(&u).unwrap().tenant_id,
            "0"
        );
    }

    #[test]
    fn rejects_bad_input() {
        let mut u = upsert();
        u.im_tenant_id = "*".to_string();
        assert!(matches!(
            TenantProjectionRecord::from_proto(&u),
            Err(TenantProjectionValidationError::InvalidTenantId(_))
        ));

        let mut u = upsert();
        u.status = 0;
        assert!(matches!(
            TenantProjectionRecord::from_proto(&u),
            Err(TenantProjectionValidationError::InvalidStatus(0))
        ));

        let mut u = upsert();
        u.version = 0;
        assert_eq!(
            TenantProjectionRecord::from_proto(&u),
            Err(TenantProjectionValidationError::ZeroVersion)
        );

        let mut u = upsert();
        u.settings_json = b"[1,2]".to_vec();
        assert!(matches!(
            TenantProjectionRecord::from_proto(&u),
            Err(TenantProjectionValidationError::InvalidSettings(_))
        ));

        let mut u = upsert();
        u.settings_json = Vec::new();
        u.quota = None;
        let record = TenantProjectionRecord::from_proto(&u).unwrap();
        assert_eq!(record.settings_json, "{}");
        assert_eq!(record.quota, TenantQuota::default());
    }
}
