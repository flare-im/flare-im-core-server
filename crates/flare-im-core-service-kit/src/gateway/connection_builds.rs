//! 各接入网关的（平台，构建）在线连接数在 Redis 里的约定：网关周期写、capability 汇总读。
//!
//! - `flare:conn-builds:{<tenant>}:<gateway_id>`：哈希，字段 `<platform>:<build>`（更旧构建合计记
//!   `<platform>:other`）→ 连接数。每轮整份替换（MULTI：DEL + HSET + EXPIRE），TTL
//!   [`BUILDS_TTL_SECS`]。
//! - `flare:conn-builds:{<tenant>}:gateways`：有序集合，成员是写过这个租户的网关，分数是最后一次写入
//!   的毫秒时间；读时只取 [`LIVE_WINDOW_MS`] 以内的成员，写时顺手删掉窗口外的。不用 KEYS/SCAN。
//!
//! 网关一轮上报把各租户的 MULTI 块排进同一条流水线，一次往返写完；每个租户仍是自己的事务。
//!
//! 写端和读端现在都用单节点连接（`redis::Client`，不是集群客户端）。键名里花括号包住的租户是
//! Redis Cluster 的 hash tag，同一租户的键落在同一槽，所以将来换集群客户端时每个租户的 MULTI 仍然
//! 成立；跨租户不在一个事务里，本来也不需要。

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::metrics::{MAX_REPORTED_BUILDS, OTHER_BUILDS_LABEL, PlatformBuildDistribution};

/// 网关上报间隔。
pub const REPORT_INTERVAL: Duration = Duration::from_secs(30);
/// 每个网关哈希的 TTL：网关宕机后最多这么久退出统计。
pub const BUILDS_TTL_SECS: u64 = 120;
/// 网关集合里成员的存活窗口（毫秒），与哈希 TTL 一致。
pub const LIVE_WINDOW_MS: i64 = (BUILDS_TTL_SECS as i64) * 1000;
/// 网关集合键本身的 TTL：没有网关再写这个租户时整键回收。
pub const GATEWAYS_TTL_SECS: u64 = 3600;
/// 一次汇总最多读多少个网关。
pub const MAX_GATEWAYS: isize = 1024;

/// 一个网关在一个租户下的构建哈希键。
pub fn builds_key(tenant_id: &str, gateway_id: &str) -> String {
    format!("flare:conn-builds:{{{tenant_id}}}:{gateway_id}")
}

/// 一个租户的网关集合键。
pub fn gateways_key(tenant_id: &str) -> String {
    format!("flare:conn-builds:{{{tenant_id}}}:gateways")
}

/// 哈希字段：`<platform>:<build>`；`build` 为 `None` 表示更旧构建的合计（`<platform>:other`）。
pub fn field(platform: &str, build: Option<u64>) -> String {
    match build {
        Some(build) => format!("{platform}:{build}"),
        None => format!("{platform}:{OTHER_BUILDS_LABEL}"),
    }
}

/// 解析哈希字段；认不出的返回 `None`。
pub fn parse_field(field: &str) -> Option<(&str, Option<u64>)> {
    let (platform, build) = field.rsplit_once(':')?;
    if platform.is_empty() {
        return None;
    }
    if build == OTHER_BUILDS_LABEL {
        return Some((platform, None));
    }
    Some((platform, Some(build.parse().ok()?)))
}

/// 一个网关一个租户的哈希内容。
pub fn hash_fields(platforms: &[PlatformBuildDistribution]) -> Vec<(String, u64)> {
    let mut fields = Vec::new();
    for platform in platforms {
        for (build, connections) in &platform.distribution.builds {
            fields.push((field(&platform.platform, Some(*build)), *connections));
        }
        if platform.distribution.other > 0 {
            fields.push((field(&platform.platform, None), platform.distribution.other));
        }
    }
    fields
}

/// 汇总后的一行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionBuildRow {
    pub platform: String,
    /// `None` = 该平台更旧构建的合计。
    pub app_build: Option<u64>,
    pub connections: u64,
}

/// 把各网关的哈希加总：每个平台重新只留最新的 [`MAX_REPORTED_BUILDS`] 个构建，其余（含各网关
/// 自己的 `other`）并进一行合计。平台升序；平台内构建号降序，合计行在最后。
pub fn merge<'a>(
    hashes: impl IntoIterator<Item = &'a HashMap<String, u64>>,
) -> Vec<ConnectionBuildRow> {
    let mut platforms: BTreeMap<String, (BTreeMap<u64, u64>, u64)> = BTreeMap::new();
    for hash in hashes {
        for (field, connections) in hash {
            let Some((platform, build)) = parse_field(field) else {
                continue;
            };
            let entry = platforms.entry(platform.to_string()).or_default();
            match build {
                Some(build) => *entry.0.entry(build).or_default() += connections,
                None => entry.1 += connections,
            }
        }
    }
    let mut rows = Vec::new();
    for (platform, (builds, mut other)) in platforms {
        for (index, (build, connections)) in builds.into_iter().rev().enumerate() {
            if index < MAX_REPORTED_BUILDS {
                rows.push(ConnectionBuildRow {
                    platform: platform.clone(),
                    app_build: Some(build),
                    connections,
                });
            } else {
                other += connections;
            }
        }
        if other > 0 {
            rows.push(ConnectionBuildRow {
                platform,
                app_build: None,
                connections: other,
            });
        }
    }
    rows
}

/// 整份替换一个网关在一个租户下的构建哈希（空 = 删掉并退出网关集合）。
pub async fn write_gateway_builds(
    connection: &mut impl redis::aio::ConnectionLike,
    tenant_id: &str,
    gateway_id: &str,
    fields: &[(String, u64)],
    now_ms: i64,
) -> redis::RedisResult<()> {
    let mut pipe = redis::pipe();
    queue_gateway_builds(&mut pipe, tenant_id, gateway_id, fields, now_ms);
    pipe.query_async::<()>(connection).await
}

/// 把 [`write_gateway_builds`] 的一个租户排进 `pipe`：自成一个 `MULTI … EXEC` 块，多个租户可以排进
/// 同一条（非 atomic 的）流水线一次发出。所有回复都忽略，任何一条出错整条流水线报错。
pub fn queue_gateway_builds(
    pipe: &mut redis::Pipeline,
    tenant_id: &str,
    gateway_id: &str,
    fields: &[(String, u64)],
    now_ms: i64,
) {
    let key = builds_key(tenant_id, gateway_id);
    let gateways = gateways_key(tenant_id);
    pipe.cmd("MULTI").ignore();
    pipe.cmd("DEL").arg(&key).ignore();
    if fields.is_empty() {
        pipe.cmd("ZREM").arg(&gateways).arg(gateway_id).ignore();
    } else {
        let mut hset = redis::cmd("HSET");
        hset.arg(&key);
        for (field, connections) in fields {
            hset.arg(field).arg(*connections);
        }
        pipe.add_command(hset).ignore();
        pipe.cmd("EXPIRE").arg(&key).arg(BUILDS_TTL_SECS).ignore();
        pipe.cmd("ZADD")
            .arg(&gateways)
            .arg(now_ms)
            .arg(gateway_id)
            .ignore();
        pipe.cmd("EXPIRE")
            .arg(&gateways)
            .arg(GATEWAYS_TTL_SECS)
            .ignore();
    }
    pipe.cmd("ZREMRANGEBYSCORE")
        .arg(&gateways)
        .arg("-inf")
        .arg(format!("({}", now_ms - LIVE_WINDOW_MS))
        .ignore();
    pipe.cmd("EXEC").ignore();
}

/// 汇总一个租户在存活网关上的构建分布；返回（各行，参与汇总的网关数）。
pub async fn read_tenant_builds(
    connection: &mut impl redis::aio::ConnectionLike,
    tenant_id: &str,
    now_ms: i64,
) -> redis::RedisResult<(Vec<ConnectionBuildRow>, usize)> {
    let gateway_ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(gateways_key(tenant_id))
        .arg(now_ms - LIVE_WINDOW_MS)
        .arg("+inf")
        .arg("LIMIT")
        .arg(0)
        .arg(MAX_GATEWAYS)
        .query_async(connection)
        .await?;
    if gateway_ids.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let mut pipe = redis::pipe();
    for gateway_id in &gateway_ids {
        pipe.cmd("HGETALL").arg(builds_key(tenant_id, gateway_id));
    }
    let hashes: Vec<HashMap<String, u64>> = pipe.query_async(connection).await?;
    // 哈希已过期（网关停写）的成员读到空哈希，不计入网关数
    let live: Vec<&HashMap<String, u64>> = hashes.iter().filter(|hash| !hash.is_empty()).collect();
    let gateways = live.len();
    Ok((merge(live), gateways))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::BuildDistribution;

    #[test]
    fn keys_share_the_tenant_hash_tag() {
        assert_eq!(builds_key("t1", "gw-a"), "flare:conn-builds:{t1}:gw-a");
        assert_eq!(gateways_key("t1"), "flare:conn-builds:{t1}:gateways");
    }

    #[test]
    fn fields_round_trip() {
        assert_eq!(field("android", Some(200)), "android:200");
        assert_eq!(field("web", None), "web:other");
        assert_eq!(parse_field("android:200"), Some(("android", Some(200))));
        assert_eq!(parse_field("web:other"), Some(("web", None)));
        assert_eq!(parse_field("web:1.2"), None);
        assert_eq!(parse_field(":5"), None);
        assert_eq!(parse_field("nocolon"), None);
    }

    #[test]
    fn hash_fields_list_builds_and_the_older_total() {
        let fields = hash_fields(&[PlatformBuildDistribution {
            platform: "ios".to_string(),
            distribution: BuildDistribution {
                builds: vec![(30, 2), (29, 1)],
                other: 4,
            },
        }]);
        assert_eq!(
            fields,
            vec![
                ("ios:30".to_string(), 2),
                ("ios:29".to_string(), 1),
                ("ios:other".to_string(), 4),
            ]
        );
    }

    #[test]
    fn merging_sums_gateways_and_retrims_per_platform() {
        let gateway_a: HashMap<String, u64> = (1..=15u64)
            .map(|build| (field("android", Some(build)), 1))
            .chain([(field("android", None), 3), ("web:7".to_string(), 2)])
            .collect();
        let gateway_b: HashMap<String, u64> = (10..=25u64)
            .map(|build| (field("android", Some(build)), 1))
            .chain([("web:7".to_string(), 1), ("garbage".to_string(), 9)])
            .collect();
        let rows = merge([&gateway_a, &gateway_b]);

        let android: Vec<_> = rows
            .iter()
            .filter(|row| row.platform == "android")
            .collect();
        assert_eq!(android.len(), MAX_REPORTED_BUILDS + 1);
        assert_eq!(android[0].app_build, Some(25));
        assert_eq!(android[MAX_REPORTED_BUILDS - 1].app_build, Some(6));
        // 10..=15 两边都有，各 2
        assert!(
            android
                .iter()
                .filter(|row| matches!(row.app_build, Some(10..=15)))
                .all(|row| row.connections == 2)
        );
        // 合计行：构建 1..=5 各 1，加网关 a 自己的 other 3
        assert_eq!(
            android.last().copied(),
            Some(&ConnectionBuildRow {
                platform: "android".to_string(),
                app_build: None,
                connections: 8,
            })
        );
        assert_eq!(
            rows.last(),
            Some(&ConnectionBuildRow {
                platform: "web".to_string(),
                app_build: Some(7),
                connections: 3,
            })
        );
        let total: u64 = rows.iter().map(|row| row.connections).sum();
        // a: 15 + 3 + 2 = 20；b: 16 + 1 = 17（garbage 丢弃）
        assert_eq!(total, 37);
    }
}
