//! 按（租户，平台，构建）统计的在线连接数。
//!
//! 计数本身是精确的：每个构建都记，连接断开就减，减到 0 删掉。对外（Prometheus 序列与查询）
//! 每个（租户，平台）只给构建号最大的 [`MAX_REPORTED_BUILDS`] 个，其余合并成一个 `other`
//! 桶，总数不变，`app_build` 标签的基数有上限（设计 §9.3、§12）。

use std::collections::{BTreeMap, HashMap};

/// 每个（租户，平台）单独列出的构建数；更旧的构建合并进 `other`。
pub const MAX_REPORTED_BUILDS: usize = 20;

/// `app_build` 标签里合并桶的取值。
pub const OTHER_BUILDS_LABEL: &str = "other";

/// 一个（租户，平台）的构建分布：构建号从大到小，最多 [`MAX_REPORTED_BUILDS`] 个，
/// 外加更旧构建的合计。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildDistribution {
    /// （构建号, 在线连接数），构建号从大到小。没报构建号的连接记在 0 下。
    pub builds: Vec<(u64, u64)>,
    /// 没有单独列出的更旧构建的在线连接数合计。
    pub other: u64,
}

impl BuildDistribution {
    /// 这个（租户，平台）的在线连接总数。
    pub fn total(&self) -> u64 {
        self.builds
            .iter()
            .map(|(_, connections)| connections)
            .sum::<u64>()
            + self.other
    }
}

/// 一个平台的构建分布，供查询接口返回。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformBuildDistribution {
    pub platform: String,
    pub distribution: BuildDistribution,
}

/// 按（租户，平台）分组的精确构建计数。
#[derive(Debug, Default)]
pub struct ConnectionBuildCounts {
    counts: HashMap<(String, String), BTreeMap<u64, u64>>,
}

impl ConnectionBuildCounts {
    /// 一条连接上线，返回这个（租户，平台）更新后的分布。
    pub fn opened(&mut self, tenant_id: &str, platform: &str, build: u64) -> BuildDistribution {
        let builds = self
            .counts
            .entry((tenant_id.to_string(), platform.to_string()))
            .or_default();
        *builds.entry(build).or_default() += 1;
        distribution(builds)
    }

    /// 一条连接下线，返回这个（租户，平台）更新后的分布。没有记过的连接不减（计数不会变负）。
    pub fn closed(&mut self, tenant_id: &str, platform: &str, build: u64) -> BuildDistribution {
        let key = (tenant_id.to_string(), platform.to_string());
        let Some(builds) = self.counts.get_mut(&key) else {
            return BuildDistribution::default();
        };
        if let Some(connections) = builds.get_mut(&build) {
            *connections = connections.saturating_sub(1);
            if *connections == 0 {
                builds.remove(&build);
            }
        }
        let snapshot = distribution(builds);
        if builds.is_empty() {
            self.counts.remove(&key);
        }
        snapshot
    }

    /// 有在线连接的全部租户各平台的构建分布，租户与平台都按名字排序。
    pub fn snapshot(&self) -> Vec<(String, Vec<PlatformBuildDistribution>)> {
        let mut tenants: Vec<&String> = self.counts.keys().map(|(tenant, _)| tenant).collect();
        tenants.sort();
        tenants.dedup();
        tenants
            .into_iter()
            .map(|tenant| (tenant.clone(), self.tenant(tenant)))
            .collect()
    }

    /// 一个租户各平台的构建分布，平台按名字排序。
    pub fn tenant(&self, tenant_id: &str) -> Vec<PlatformBuildDistribution> {
        let mut platforms: Vec<_> = self
            .counts
            .iter()
            .filter(|((tenant, _), _)| tenant == tenant_id)
            .map(|((_, platform), builds)| PlatformBuildDistribution {
                platform: platform.clone(),
                distribution: distribution(builds),
            })
            .collect();
        platforms.sort_by(|a, b| a.platform.cmp(&b.platform));
        platforms
    }
}

fn distribution(builds: &BTreeMap<u64, u64>) -> BuildDistribution {
    let mut listed = Vec::with_capacity(builds.len().min(MAX_REPORTED_BUILDS));
    let mut other = 0;
    for (index, (build, connections)) in builds.iter().rev().enumerate() {
        if index < MAX_REPORTED_BUILDS {
            listed.push((*build, *connections));
        } else {
            other += connections;
        }
    }
    BuildDistribution {
        builds: listed,
        other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_follow_connections_and_drop_empty_buckets() {
        let mut counts = ConnectionBuildCounts::default();
        counts.opened("t1", "android", 200);
        counts.opened("t1", "android", 200);
        let snapshot = counts.opened("t1", "android", 100);
        assert_eq!(snapshot.builds, vec![(200, 2), (100, 1)]);
        assert_eq!(snapshot.total(), 3);

        let snapshot = counts.closed("t1", "android", 100);
        assert_eq!(snapshot.builds, vec![(200, 2)]);
        counts.closed("t1", "android", 200);
        let snapshot = counts.closed("t1", "android", 200);
        assert_eq!(snapshot, BuildDistribution::default());
        assert!(counts.tenant("t1").is_empty());

        // 没记过的连接下线不会让计数变负
        assert_eq!(counts.closed("t1", "ios", 1), BuildDistribution::default());
    }

    #[test]
    fn only_the_newest_builds_are_listed_and_the_rest_are_summed() {
        let mut counts = ConnectionBuildCounts::default();
        for build in 1..=25u64 {
            counts.opened("t1", "web", build);
        }
        counts.opened("t1", "web", 1);
        let snapshot = counts.opened("t1", "web", 3);
        assert_eq!(snapshot.builds.len(), MAX_REPORTED_BUILDS);
        assert_eq!(snapshot.builds.first(), Some(&(25, 1)));
        assert_eq!(snapshot.builds.last(), Some(&(6, 1)));
        // 1..=5 没列出：1、3 各两条，2、4、5 各一条
        assert_eq!(snapshot.other, 7);
        assert_eq!(snapshot.total(), 27);
    }

    #[test]
    fn the_snapshot_lists_every_tenant_once() {
        let mut counts = ConnectionBuildCounts::default();
        counts.opened("t2", "web", 1);
        counts.opened("t1", "web", 1);
        counts.opened("t1", "ios", 2);
        let snapshot = counts.snapshot();
        assert_eq!(
            snapshot
                .iter()
                .map(|(tenant, platforms)| (tenant.as_str(), platforms.len()))
                .collect::<Vec<_>>(),
            vec![("t1", 2), ("t2", 1)]
        );
    }

    #[test]
    fn tenants_and_platforms_are_kept_apart() {
        let mut counts = ConnectionBuildCounts::default();
        counts.opened("t1", "web", 5);
        counts.opened("t1", "android", 7);
        counts.opened("t2", "web", 9);
        let tenant = counts.tenant("t1");
        assert_eq!(
            tenant,
            vec![
                PlatformBuildDistribution {
                    platform: "android".to_string(),
                    distribution: BuildDistribution {
                        builds: vec![(7, 1)],
                        other: 0,
                    },
                },
                PlatformBuildDistribution {
                    platform: "web".to_string(),
                    distribution: BuildDistribution {
                        builds: vec![(5, 1)],
                        other: 0,
                    },
                },
            ]
        );
    }
}
