//! CONNECT 版本门禁：客户端构建低于最低构建号（已生效）或落在封禁区间时，要求升级。
//!
//! 策略来自应用的 `client_policy` 投影，按发布清单平台段（`web`、`android`、`ios`、`tauri`、
//! `electron`）取；没报构建号的连接按 0 判（预发布期不留宽容窗口）。

use flare_im_core_contracts::domain::tenant::TenantClientPolicy;
use flare_proto::client_platform::{normalize_device_platform, release_platform};

/// 为什么要求升级。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeReason {
    /// 低于已生效的最低构建号
    BelowMinBuild,
    /// 落在封禁区间里
    BlockedBuild,
}

impl UpgradeReason {
    /// `handshake_rejected_total{reason}` 的取值。
    pub fn metric_label(self) -> &'static str {
        match self {
            Self::BelowMinBuild => "below_min_build",
            Self::BlockedBuild => "blocked_build",
        }
    }
}

/// 门禁的拒绝：客户端至少要升级到 `min_build`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradeRequired {
    pub min_build: u64,
    pub reason: UpgradeReason,
}

/// 判定一条连接是否必须升级；放行返回 `None`。
///
/// - 平台没有策略：放行。
/// - `min_build` 为 0 不限；`min_build_effective_from_ms` 在 `now_ms` 之后时还没生效（0 = 立即生效）。
/// - 封禁区间是闭区间。命中时要求的构建号取「已生效的最低构建号」与「命中区间之后的第一个构建」
///   中较大的那个，客户端照着它升级就不会再落回封禁区间。
pub fn upgrade_verdict(
    policy: &TenantClientPolicy,
    platform: &str,
    build: u64,
    now_ms: i64,
) -> Option<UpgradeRequired> {
    let build_policy = policy.build_policies.get(platform)?;
    let min_build =
        if build_policy.min_build > 0 && build_policy.min_build_effective_from_ms <= now_ms {
            build_policy.min_build
        } else {
            0
        };
    if build < min_build {
        return Some(UpgradeRequired {
            min_build,
            reason: UpgradeReason::BelowMinBuild,
        });
    }
    let past_blocked = build_policy
        .blocked
        .iter()
        .filter(|range| range.from <= build && build <= range.to)
        .map(|range| range.to.saturating_add(1))
        .max()?;
    Some(UpgradeRequired {
        min_build: min_build.max(past_blocked),
        reason: UpgradeReason::BlockedBuild,
    })
}

/// 门禁与构建分布用的平台：CONNECT 设备平台按 `flare_proto::client_platform::release_platform` 映射成
/// 发布清单平台段（别名先归一，与业务网关同一处）。没有清单的平台（鸿蒙等）和认不出外壳的桌面
/// （`distribution` 不是 `tauri` / `electron`）不设门禁，返回 `None`。
pub fn gate_platform(device_platform: &str, distribution: Option<&str>) -> Option<&'static str> {
    release_platform(device_platform, distribution)
}

/// 构建分布统计里的平台标签：发布清单平台段；鸿蒙记 `harmonyos`，其余认不出的（包括认不出外壳的
/// 桌面）记 `other`（客户端自报的平台字符串不进指标标签）。
pub fn stats_platform(device_platform: Option<&str>, distribution: Option<&str>) -> &'static str {
    let Some(device_platform) = device_platform else {
        return "other";
    };
    gate_platform(device_platform, distribution).unwrap_or_else(|| {
        if normalize_device_platform(device_platform) == Some("harmonyos") {
            "harmonyos"
        } else {
            "other"
        }
    })
}

/// 一条已在线连接的门禁输入，取自它 CONNECT 时的设备信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveClientBuild {
    pub connection_id: String,
    /// CONNECT 设备平台（未归一）
    pub device_platform: String,
    /// CONNECT 元数据 `distribution`
    pub distribution: Option<String>,
    /// CONNECT 元数据 `app_build`，没报按 0
    pub app_build: u64,
}

impl LiveClientBuild {
    /// 按当前策略判这条连接：与 CONNECT 门禁同一套规则（平台映射、构建号缺省、生效时间）。
    pub fn verdict(&self, policy: &TenantClientPolicy, now_ms: i64) -> Option<UpgradeRequired> {
        let platform = gate_platform(&self.device_platform, self.distribution.as_deref())?;
        upgrade_verdict(policy, platform, self.app_build, now_ms)
    }
}

/// 下限抬高后必须断开的在线连接：在 CONNECT 时会被拒的那些，带上各自要升级到的构建号。
/// 顺序与输入一致。
pub fn connections_below_floor<'a>(
    policy: &TenantClientPolicy,
    connections: impl IntoIterator<Item = &'a LiveClientBuild>,
    now_ms: i64,
) -> Vec<(&'a LiveClientBuild, UpgradeRequired)> {
    connections
        .into_iter()
        .filter_map(|connection| {
            connection
                .verdict(policy, now_ms)
                .map(|verdict| (connection, verdict))
        })
        .collect()
}

/// CONNECT 上报的构建号：没报、不是十进制整数都按 0。
pub fn reported_build(value: Option<&str>) -> u64 {
    value
        .map(str::trim)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_core_contracts::domain::tenant::{ClientBuildPolicy, ClientBuildRange};
    use std::collections::BTreeMap;

    const NOW: i64 = 1_800_000_000_000;

    fn policy(platform: &str, build_policy: ClientBuildPolicy) -> TenantClientPolicy {
        TenantClientPolicy {
            tenant_id: "t1".to_string(),
            build_policies: BTreeMap::from([(platform.to_string(), build_policy)]),
            ..TenantClientPolicy::default()
        }
    }

    fn below(min_build: u64) -> Option<UpgradeRequired> {
        Some(UpgradeRequired {
            min_build,
            reason: UpgradeReason::BelowMinBuild,
        })
    }

    fn blocked(min_build: u64) -> Option<UpgradeRequired> {
        Some(UpgradeRequired {
            min_build,
            reason: UpgradeReason::BlockedBuild,
        })
    }

    #[test]
    fn judgment_table() {
        let min_only = policy(
            "android",
            ClientBuildPolicy {
                min_build: 200,
                ..ClientBuildPolicy::default()
            },
        );
        let future_min = policy(
            "android",
            ClientBuildPolicy {
                min_build: 200,
                min_build_effective_from_ms: NOW + 1,
                ..ClientBuildPolicy::default()
            },
        );
        let reached_min = policy(
            "android",
            ClientBuildPolicy {
                min_build: 200,
                min_build_effective_from_ms: NOW,
                ..ClientBuildPolicy::default()
            },
        );
        let with_blocked = policy(
            "android",
            ClientBuildPolicy {
                min_build: 100,
                min_build_effective_from_ms: 0,
                blocked: vec![
                    ClientBuildRange { from: 210, to: 212 },
                    ClientBuildRange { from: 212, to: 215 },
                ],
            },
        );
        let blocked_with_future_min = policy(
            "android",
            ClientBuildPolicy {
                min_build: 300,
                min_build_effective_from_ms: NOW + 60_000,
                blocked: vec![ClientBuildRange { from: 210, to: 212 }],
            },
        );
        let unlimited = policy("android", ClientBuildPolicy::default());

        for (name, policy, platform, build, expected) in [
            ("below min", &min_only, "android", 199, below(200)),
            ("at min", &min_only, "android", 200, None),
            ("above min", &min_only, "android", 201, None),
            (
                "missing build counts as 0",
                &min_only,
                "android",
                0,
                below(200),
            ),
            ("min not effective yet", &future_min, "android", 1, None),
            (
                "min effective right now",
                &reached_min,
                "android",
                199,
                below(200),
            ),
            ("unknown platform", &min_only, "ios", 1, None),
            ("no limits", &unlimited, "android", 0, None),
            (
                "just before blocked range",
                &with_blocked,
                "android",
                209,
                None,
            ),
            (
                "blocked range start",
                &with_blocked,
                "android",
                210,
                blocked(213),
            ),
            (
                "blocked range end",
                &with_blocked,
                "android",
                212,
                blocked(216),
            ),
            (
                "overlapping range end",
                &with_blocked,
                "android",
                215,
                blocked(216),
            ),
            (
                "just after blocked range",
                &with_blocked,
                "android",
                216,
                None,
            ),
            (
                "below min before blocked",
                &with_blocked,
                "android",
                99,
                below(100),
            ),
            (
                "blocked while min is pending",
                &blocked_with_future_min,
                "android",
                211,
                blocked(213),
            ),
        ] {
            assert_eq!(
                upgrade_verdict(policy, platform, build, NOW),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn blocked_build_asks_for_at_least_the_effective_min_build() {
        let policy = policy(
            "web",
            ClientBuildPolicy {
                min_build: 500,
                min_build_effective_from_ms: 0,
                blocked: vec![ClientBuildRange { from: 600, to: 601 }],
            },
        );
        assert_eq!(upgrade_verdict(&policy, "web", 600, NOW), blocked(602));
        let policy = TenantClientPolicy {
            build_policies: BTreeMap::from([(
                "web".to_string(),
                ClientBuildPolicy {
                    min_build: 900,
                    min_build_effective_from_ms: 0,
                    blocked: vec![ClientBuildRange { from: 0, to: 10 }],
                },
            )]),
            ..TenantClientPolicy::default()
        };
        // 0 低于最低构建号，按最低构建号报
        assert_eq!(upgrade_verdict(&policy, "web", 0, NOW), below(900));
    }

    #[test]
    fn platforms_follow_the_shared_release_mapping() {
        assert_eq!(gate_platform("pc", Some("electron")), Some("electron"));
        assert_eq!(gate_platform("pc", Some("tauri")), Some("tauri"));
        assert_eq!(gate_platform("pc", None), None);
        assert_eq!(gate_platform("pc", Some("direct")), None);
        assert_eq!(gate_platform("desktop", Some("tauri")), Some("tauri"));
        assert_eq!(gate_platform("iphone", None), Some("ios"));
        assert_eq!(stats_platform(Some("pc"), None), "other");
        assert_eq!(stats_platform(Some("harmony"), None), "harmonyos");
        assert_eq!(gate_platform("web", None), Some("web"));
        assert_eq!(gate_platform("harmonyos", None), None);
        assert_eq!(stats_platform(Some("harmonyos"), None), "harmonyos");
        assert_eq!(stats_platform(Some("android"), Some("play")), "android");
        assert_eq!(stats_platform(Some("toaster"), None), "other");
        assert_eq!(stats_platform(None, None), "other");
    }

    fn live(id: &str, platform: &str, distribution: Option<&str>, build: u64) -> LiveClientBuild {
        LiveClientBuild {
            connection_id: id.to_string(),
            device_platform: platform.to_string(),
            distribution: distribution.map(str::to_string),
            app_build: build,
        }
    }

    #[test]
    fn a_raised_floor_selects_exactly_the_connections_connect_would_refuse() {
        let policy = TenantClientPolicy {
            tenant_id: "t1".to_string(),
            build_policies: BTreeMap::from([
                (
                    "android".to_string(),
                    ClientBuildPolicy {
                        min_build: 200,
                        ..ClientBuildPolicy::default()
                    },
                ),
                (
                    "electron".to_string(),
                    ClientBuildPolicy {
                        min_build: 0,
                        min_build_effective_from_ms: 0,
                        blocked: vec![ClientBuildRange { from: 40, to: 41 }],
                    },
                ),
                (
                    "ios".to_string(),
                    ClientBuildPolicy {
                        min_build: 500,
                        min_build_effective_from_ms: NOW + 1,
                        ..ClientBuildPolicy::default()
                    },
                ),
            ]),
            ..TenantClientPolicy::default()
        };
        let connections = [
            live("old-android", "android", None, 199),
            live("new-android", "android", Some("play"), 200),
            live("unreported-android", "android", None, 0),
            live("blocked-electron", "pc", Some("electron"), 41),
            live("fine-electron", "pc", Some("electron"), 42),
            // 认不出外壳的桌面与鸿蒙没有门禁：CONNECT 放行，在线也不断
            live("bare-desktop", "pc", None, 1),
            live("harmony", "harmonyos", None, 1),
            // 下限还没生效
            live("old-ios", "iphone", None, 1),
        ];
        let selected: Vec<(&str, UpgradeRequired)> =
            connections_below_floor(&policy, &connections, NOW)
                .into_iter()
                .map(|(connection, verdict)| (connection.connection_id.as_str(), verdict))
                .collect();
        assert_eq!(
            selected,
            vec![
                ("old-android", below(200).unwrap()),
                ("unreported-android", below(200).unwrap()),
                ("blocked-electron", blocked(42).unwrap()),
            ]
        );

        // 下限到点生效之后，同一批连接里 iOS 的也要断
        let later = connections_below_floor(&policy, &connections, NOW + 1);
        assert_eq!(later.len(), 4);
        assert_eq!(later[3].0.connection_id, "old-ios");
        assert_eq!(later[3].1, below(500).unwrap());
    }

    #[test]
    fn a_lowered_floor_selects_nobody() {
        let policy = policy("android", ClientBuildPolicy::default());
        let connections = [live("a", "android", None, 0), live("b", "android", None, 7)];
        assert!(connections_below_floor(&policy, &connections, NOW).is_empty());
        assert!(
            connections_below_floor(&TenantClientPolicy::default(), &connections, NOW).is_empty()
        );
    }

    #[test]
    fn reported_build_defaults_to_zero() {
        assert_eq!(reported_build(None), 0);
        assert_eq!(reported_build(Some("")), 0);
        assert_eq!(reported_build(Some("1.2.3")), 0);
        assert_eq!(reported_build(Some("-5")), 0);
        assert_eq!(reported_build(Some(" 20261104 ")), 20_261_104);
    }
}
