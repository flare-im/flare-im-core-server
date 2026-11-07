//! 客户端策略投影（控制面 → 核）的领域记录与校验。
//!
//! `access_serial` 与 `release_generation` 是只进不退的水位：客户端只前进，内容回滚在控制面表现为
//! 更大的 serial。水位前进才产生租户通知，`build_policies` 单独变化不通知。

use std::collections::BTreeMap;

use flare_im_contracts::domain::tenant::{ClientBuildPolicy, is_valid_tenant_id};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::tenant_notice::{ACCESS_TOPIC, RELEASE_TOPIC};

/// 一个应用最多带多少个平台的构建策略。
pub const MAX_BUILD_POLICY_PLATFORMS: usize = 32;
/// 一个平台最多封禁多少段构建号。
pub const MAX_BLOCKED_BUILD_RANGES: usize = 64;
/// 平台键的最大字节数。
pub const MAX_PLATFORM_KEY_BYTES: usize = 32;

/// 一条待写入的客户端策略投影。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientPolicyRecord {
    pub tenant_id: String,
    pub version: u64,
    pub access_serial: u64,
    pub release_generation: u64,
    pub build_policies: BTreeMap<String, ClientBuildPolicy>,
}

/// 本地已有的版本与水位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientPolicyWatermarks {
    pub version: u64,
    pub access_serial: u64,
    pub release_generation: u64,
}

/// 投影提交后要发出的租户通知。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientPolicyNotice {
    pub topic: &'static str,
    pub version: u64,
}

/// 校验失败原因（映射为 gRPC `InvalidArgument`）。手写 `Display` 的原因见
/// [`super::tenant_projection::TenantProjectionValidationError`]。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientPolicyValidationError {
    InvalidTenantId,
    ZeroVersion,
    ValueOutOfRange(&'static str),
    TooManyPlatforms,
    InvalidPlatform,
    TooManyBlockedRanges,
    InvalidBlockedRange,
    NegativeEffectiveTime,
}

impl std::fmt::Display for ClientPolicyValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTenantId => f.write_str("app_id must be a canonical tenant id"),
            Self::ZeroVersion => f.write_str("version must be >= 1"),
            Self::ValueOutOfRange(field) => write!(f, "{field} is out of range"),
            Self::TooManyPlatforms => write!(
                f,
                "build_policies holds at most {MAX_BUILD_POLICY_PLATFORMS} platforms"
            ),
            Self::InvalidPlatform => {
                f.write_str("build_policies keys must match ^[a-z0-9_-]{1,32}$")
            }
            Self::TooManyBlockedRanges => write!(
                f,
                "a platform blocks at most {MAX_BLOCKED_BUILD_RANGES} build ranges"
            ),
            Self::InvalidBlockedRange => f.write_str("a blocked range must have from <= to"),
            Self::NegativeEffectiveTime => {
                f.write_str("min_build_effective_from_ms must not be negative")
            }
        }
    }
}

impl std::error::Error for ClientPolicyValidationError {}

fn valid_platform(platform: &str) -> bool {
    (1..=MAX_PLATFORM_KEY_BYTES).contains(&platform.len())
        && platform
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

fn fits_bigint(value: u64, field: &'static str) -> Result<(), ClientPolicyValidationError> {
    i64::try_from(value)
        .map(|_| ())
        .map_err(|_| ClientPolicyValidationError::ValueOutOfRange(field))
}

impl ClientPolicyRecord {
    /// 校验一条来件；`app_id` 必须已是规范写法，不做别名归一。
    pub fn new(
        tenant_id: String,
        version: u64,
        access_serial: u64,
        release_generation: u64,
        build_policies: BTreeMap<String, ClientBuildPolicy>,
    ) -> Result<Self, ClientPolicyValidationError> {
        if !is_valid_tenant_id(&tenant_id) || normalize_tenant_id(&tenant_id) != tenant_id {
            return Err(ClientPolicyValidationError::InvalidTenantId);
        }
        if version == 0 {
            return Err(ClientPolicyValidationError::ZeroVersion);
        }
        fits_bigint(version, "version")?;
        fits_bigint(access_serial, "access_serial")?;
        fits_bigint(release_generation, "release_generation")?;
        if build_policies.len() > MAX_BUILD_POLICY_PLATFORMS {
            return Err(ClientPolicyValidationError::TooManyPlatforms);
        }
        for (platform, policy) in &build_policies {
            if !valid_platform(platform) {
                return Err(ClientPolicyValidationError::InvalidPlatform);
            }
            if policy.min_build_effective_from_ms < 0 {
                return Err(ClientPolicyValidationError::NegativeEffectiveTime);
            }
            if policy.blocked.len() > MAX_BLOCKED_BUILD_RANGES {
                return Err(ClientPolicyValidationError::TooManyBlockedRanges);
            }
            if policy.blocked.iter().any(|range| range.from > range.to) {
                return Err(ClientPolicyValidationError::InvalidBlockedRange);
            }
        }
        Ok(Self {
            tenant_id,
            version,
            access_serial,
            release_generation,
            build_policies,
        })
    }

    /// 相对本地水位前进了的通知；水位持平或更小的不通知，`0` 表示从未发布，也不通知。
    pub fn notices_since(
        &self,
        previous: Option<ClientPolicyWatermarks>,
    ) -> Vec<ClientPolicyNotice> {
        let (access_serial, release_generation) = previous
            .map(|previous| (previous.access_serial, previous.release_generation))
            .unwrap_or_default();
        let mut notices = Vec::with_capacity(2);
        if self.access_serial > access_serial {
            notices.push(ClientPolicyNotice {
                topic: ACCESS_TOPIC,
                version: self.access_serial,
            });
        }
        if self.release_generation > release_generation {
            notices.push(ClientPolicyNotice {
                topic: RELEASE_TOPIC,
                version: self.release_generation,
            });
        }
        notices
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::domain::tenant::ClientBuildRange;

    fn record(version: u64, access_serial: u64, release_generation: u64) -> ClientPolicyRecord {
        ClientPolicyRecord::new(
            "app-a".into(),
            version,
            access_serial,
            release_generation,
            BTreeMap::new(),
        )
        .unwrap()
    }

    fn platform(key: &str, policy: ClientBuildPolicy) -> BTreeMap<String, ClientBuildPolicy> {
        BTreeMap::from([(key.to_string(), policy)])
    }

    fn with(
        policies: BTreeMap<String, ClientBuildPolicy>,
    ) -> Result<ClientPolicyRecord, ClientPolicyValidationError> {
        ClientPolicyRecord::new("app-a".into(), 1, 1, 0, policies)
    }

    #[test]
    fn only_watermarks_that_move_forward_announce() {
        let previous = Some(ClientPolicyWatermarks {
            version: 4,
            access_serial: 10,
            release_generation: 3,
        });
        assert_eq!(
            record(5, 11, 3).notices_since(previous),
            vec![ClientPolicyNotice {
                topic: ACCESS_TOPIC,
                version: 11
            }]
        );
        assert_eq!(
            record(5, 10, 4).notices_since(previous),
            vec![ClientPolicyNotice {
                topic: RELEASE_TOPIC,
                version: 4
            }]
        );
        assert_eq!(record(5, 11, 4).notices_since(previous).len(), 2);
        assert!(record(5, 10, 3).notices_since(previous).is_empty());
        assert!(record(5, 9, 2).notices_since(previous).is_empty());
    }

    #[test]
    fn a_first_projection_announces_what_was_published() {
        assert_eq!(record(1, 7, 0).notices_since(None).len(), 1);
        assert_eq!(record(1, 7, 2).notices_since(None).len(), 2);
        assert!(record(1, 0, 0).notices_since(None).is_empty());
    }

    #[test]
    fn identity_and_counters_are_validated() {
        let build = |tenant: &str, version, serial, generation| {
            ClientPolicyRecord::new(tenant.into(), version, serial, generation, BTreeMap::new())
        };
        assert_eq!(
            build("default", 1, 1, 1),
            Err(ClientPolicyValidationError::InvalidTenantId)
        );
        assert_eq!(
            build("*", 1, 1, 1),
            Err(ClientPolicyValidationError::InvalidTenantId)
        );
        assert_eq!(
            build("app-a", 0, 1, 1),
            Err(ClientPolicyValidationError::ZeroVersion)
        );
        for (version, serial, generation, field) in [
            (u64::MAX, 1, 1, "version"),
            (1, u64::MAX, 1, "access_serial"),
            (1, 1, u64::MAX, "release_generation"),
        ] {
            assert_eq!(
                build("app-a", version, serial, generation),
                Err(ClientPolicyValidationError::ValueOutOfRange(field))
            );
        }
    }

    #[test]
    fn build_policies_are_bounded() {
        let ok = ClientBuildPolicy {
            min_build: 100,
            min_build_effective_from_ms: 1_700_000_000_000,
            blocked: vec![ClientBuildRange { from: 5, to: 5 }],
        };
        assert!(with(platform("ios", ok.clone())).is_ok());
        assert_eq!(
            with(platform("iOS", ok.clone())),
            Err(ClientPolicyValidationError::InvalidPlatform)
        );
        assert_eq!(
            with(platform("", ok.clone())),
            Err(ClientPolicyValidationError::InvalidPlatform)
        );
        assert_eq!(
            with(platform(&"x".repeat(33), ok.clone())),
            Err(ClientPolicyValidationError::InvalidPlatform)
        );
        assert_eq!(
            with(platform(
                "ios",
                ClientBuildPolicy {
                    min_build_effective_from_ms: -1,
                    ..ok.clone()
                }
            )),
            Err(ClientPolicyValidationError::NegativeEffectiveTime)
        );
        assert_eq!(
            with(platform(
                "ios",
                ClientBuildPolicy {
                    blocked: vec![ClientBuildRange { from: 6, to: 5 }],
                    ..ok.clone()
                }
            )),
            Err(ClientPolicyValidationError::InvalidBlockedRange)
        );
        assert_eq!(
            with(platform(
                "ios",
                ClientBuildPolicy {
                    blocked: vec![ClientBuildRange { from: 1, to: 1 }; 65],
                    ..ok.clone()
                }
            )),
            Err(ClientPolicyValidationError::TooManyBlockedRanges)
        );
        let many = (0..33)
            .map(|i| (format!("p{i}"), ok.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            with(many),
            Err(ClientPolicyValidationError::TooManyPlatforms)
        );
    }
}
