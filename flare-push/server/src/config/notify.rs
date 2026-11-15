//! 防打扰闸门的配置：`[services.push_server.notify]` 与按租户的覆盖。
//!
//! ```toml
//! [services.push_server.notify]
//! enabled = true
//! due_shards = 16
//! due_poll_interval_ms = 250
//! due_batch = 256
//!
//! [services.push_server.notify.tenants."tenant-a"]
//! force_hidden_preview = true
//! all_mention_interval_ms = 1800000
//!
//! [services.push_server.notify.tenants."tenant-a".gate]
//! loud_ladder_group = [60000, 300000, 900000]
//! vendor_cap = 20
//! ```
//!
//! 共享的强类型配置不认识这一段（多余的键被忽略），所以这里自己从配置目录的同一批文件里读：
//! 目录下的 `base.toml`、`shared/*.toml`、`services/*.toml`、`overrides/*.toml`，后读的覆盖先读的；
//! `FLARE_CONFIG_PATH` 指向单个文件时只读那个文件。读不懂的段告警后用缺省值，不阻止启动。

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use flare_notify_rules::{Gate, GateConfig, Ladder};
use serde::Deserialize;

use crate::domain::model::{DEFAULT_ALL_MENTION_INTERVAL_MS, NotifyPolicy, TenantNotifyPolicy};
use crate::infrastructure::notify::keys::{DEFAULT_DUE_SHARDS, DEFAULT_KEY_PREFIX};

/// 闸门参数的租户覆盖。字段名与规则的 `GateConfig` 相同，阶梯写成毫秒数组；没写的字段用缺省值。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateOverride {
    #[serde(default)]
    pub quiet_ladder: Option<Vec<u32>>,
    #[serde(default)]
    pub loud_ladder_direct: Option<Vec<u32>>,
    #[serde(default)]
    pub loud_ladder_group: Option<Vec<u32>>,
    #[serde(default)]
    pub digest_ladder: Option<Vec<u32>>,
    #[serde(default)]
    pub slot_idle_expiry_ms: Option<i64>,
    #[serde(default)]
    pub loud_bucket_capacity: Option<u8>,
    #[serde(default)]
    pub loud_bucket_refill_ms: Option<i64>,
    #[serde(default)]
    pub device_min_loud_gap_ms: Option<i64>,
    #[serde(default)]
    pub digest_enter_conversations: Option<u8>,
    #[serde(default)]
    pub digest_enter_window_ms: Option<i64>,
    #[serde(default)]
    pub digest_exit_window_ms: Option<i64>,
    #[serde(default)]
    pub vendor_cap: Option<u8>,
    #[serde(default)]
    pub vendor_cap_window_ms: Option<i64>,
}

fn ladder(name: &str, steps: Option<&Vec<u32>>, current: Ladder) -> Result<Ladder, String> {
    match steps {
        None => Ok(current),
        Some(steps) => Ladder::new(steps).ok_or_else(|| format!("{name} is not a valid ladder")),
    }
}

impl GateOverride {
    /// 把覆盖叠到 `base` 上，再经 `GateConfig::validate` 检查。
    pub fn apply(&self, base: GateConfig) -> Result<GateConfig, String> {
        let mut config = base;
        config.quiet_ladder = ladder(
            "quiet_ladder",
            self.quiet_ladder.as_ref(),
            config.quiet_ladder,
        )?;
        config.loud_ladder_direct = ladder(
            "loud_ladder_direct",
            self.loud_ladder_direct.as_ref(),
            config.loud_ladder_direct,
        )?;
        config.loud_ladder_group = ladder(
            "loud_ladder_group",
            self.loud_ladder_group.as_ref(),
            config.loud_ladder_group,
        )?;
        config.digest_ladder = ladder(
            "digest_ladder",
            self.digest_ladder.as_ref(),
            config.digest_ladder,
        )?;
        if let Some(value) = self.slot_idle_expiry_ms {
            config.slot_idle_expiry_ms = value;
        }
        if let Some(value) = self.loud_bucket_capacity {
            config.loud_bucket_capacity = value;
        }
        if let Some(value) = self.loud_bucket_refill_ms {
            config.loud_bucket_refill_ms = value;
        }
        if let Some(value) = self.device_min_loud_gap_ms {
            config.device_min_loud_gap_ms = value;
        }
        if let Some(value) = self.digest_enter_conversations {
            config.digest_enter_conversations = value;
        }
        if let Some(value) = self.digest_enter_window_ms {
            config.digest_enter_window_ms = value;
        }
        if let Some(value) = self.digest_exit_window_ms {
            config.digest_exit_window_ms = value;
        }
        if let Some(value) = self.vendor_cap {
            config.vendor_cap = value;
        }
        if let Some(value) = self.vendor_cap_window_ms {
            config.vendor_cap_window_ms = value;
        }
        config.validate().map_err(|error| error.to_string())?;
        Ok(config)
    }
}

/// 一个租户的覆盖。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantNotifyOverride {
    /// 强制隐藏预览：不论用户怎么设置，推送一律用通用文案。
    #[serde(default)]
    pub force_hidden_preview: bool,
    /// 超过成员上限的群里，同一个会话两次全员 @所有人 波次之间的最短间隔（毫秒），缺省 10 分钟。
    #[serde(default)]
    pub all_mention_interval_ms: Option<i64>,
    #[serde(default)]
    pub gate: GateOverride,
}

/// 配置文件里的那一段。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotifySection {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    key_prefix: Option<String>,
    #[serde(default)]
    due_shards: Option<u32>,
    #[serde(default)]
    due_poll_interval_ms: Option<u64>,
    #[serde(default)]
    due_batch: Option<usize>,
    #[serde(default)]
    tenants: HashMap<String, TenantNotifyOverride>,
}

/// 防打扰闸门的设置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyGateSettings {
    /// 关掉时离线任务按消息逐条发出，不经过闸门（与接入闸门之前一样）。
    pub enabled: bool,
    /// 闸门状态所在的推送 Redis（`PUSH_SERVER_GATE_REDIS_URL`，缺省 `[redis.push]`）。
    pub redis_url: String,
    pub key_prefix: String,
    /// 到期队列的分片数。各实例都轮询全部分片，分片只是把一个大有序集合拆小。
    pub due_shards: u32,
    /// 到期循环多久领取一次（毫秒）。决定尾部刷新与延迟推送最多晚多少。
    pub due_poll_interval_ms: u64,
    /// 每个分片每次最多领取多少条。
    pub due_batch: usize,
    pub tenants: HashMap<String, TenantNotifyOverride>,
}

impl Default for NotifyGateSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            redis_url: String::new(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            due_shards: DEFAULT_DUE_SHARDS,
            due_poll_interval_ms: 250,
            due_batch: 256,
            tenants: HashMap::new(),
        }
    }
}

impl NotifyGateSettings {
    /// 从配置目录读出这一段，叠到缺省值上。
    pub fn load(config_path: &str, redis_url: String) -> Self {
        let section = read_section(Path::new(config_path));
        Self::from_section(section, redis_url)
    }

    fn from_section(section: Option<NotifySection>, redis_url: String) -> Self {
        let defaults = Self::default();
        let Some(section) = section else {
            return Self {
                redis_url,
                ..defaults
            };
        };
        Self {
            enabled: section.enabled.unwrap_or(defaults.enabled),
            redis_url,
            key_prefix: section
                .key_prefix
                .filter(|prefix| !prefix.trim().is_empty())
                .unwrap_or(defaults.key_prefix),
            due_shards: section
                .due_shards
                .filter(|n| *n > 0)
                .unwrap_or(defaults.due_shards),
            due_poll_interval_ms: section
                .due_poll_interval_ms
                .filter(|ms| *ms > 0)
                .unwrap_or(defaults.due_poll_interval_ms),
            due_batch: section
                .due_batch
                .filter(|n| *n > 0)
                .unwrap_or(defaults.due_batch),
            tenants: section.tenants,
        }
    }

    /// 各租户的提示策略。覆盖不合法的租户告警后用缺省参数，强制隐藏预览照样生效。
    pub fn policy(&self) -> NotifyPolicy {
        let tenants = self
            .tenants
            .iter()
            .map(|(tenant_id, tenant)| {
                let gate = match tenant.gate.apply(GateConfig::default()) {
                    Ok(config) => Gate::new(config),
                    Err(error) => {
                        tracing::error!(
                            tenant_id = %tenant_id,
                            error = %error,
                            "invalid gate override; the tenant uses the default gate"
                        );
                        Gate::default()
                    }
                };
                let all_mention_interval_ms = match tenant.all_mention_interval_ms {
                    None => DEFAULT_ALL_MENTION_INTERVAL_MS,
                    Some(interval_ms) if interval_ms > 0 => interval_ms,
                    Some(interval_ms) => {
                        tracing::error!(
                            tenant_id = %tenant_id,
                            interval_ms,
                            "invalid all_mention_interval_ms; the tenant uses the default"
                        );
                        DEFAULT_ALL_MENTION_INTERVAL_MS
                    }
                };
                (
                    tenant_id.clone(),
                    TenantNotifyPolicy {
                        gate,
                        force_hidden_preview: tenant.force_hidden_preview,
                        all_mention_interval_ms,
                    },
                )
            })
            .collect();
        NotifyPolicy::new(tenants)
    }
}

/// 配置目录里按合并顺序排好的文件。
fn fragment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = vec![dir.join("base.toml")];
    for sub in ["shared", "services", "overrides"] {
        let Ok(entries) = fs::read_dir(dir.join(sub)) else {
            continue;
        };
        let mut found: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.extension()
                    .and_then(OsStr::to_str)
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
            })
            .collect();
        found.sort();
        files.extend(found);
    }
    files
}

fn merge(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base), toml::Value::Table(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn read_section(path: &Path) -> Option<NotifySection> {
    let files = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        fragment_files(path)
    };
    let mut merged: Option<toml::Value> = None;
    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        let value: toml::Value = match toml::from_str(&text) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(file = %file.display(), error = %error, "config fragment not readable for the notify gate");
                continue;
            }
        };
        let Some(section) = value
            .get("services")
            .and_then(|services| services.get("push_server"))
            .and_then(|service| service.get("notify"))
        else {
            continue;
        };
        match merged.as_mut() {
            Some(merged) => merge(merged, section.clone()),
            None => merged = Some(section.clone()),
        }
    }
    let merged = merged?;
    match merged.try_into::<NotifySection>() {
        Ok(section) => Some(section),
        Err(error) => {
            tracing::error!(error = %error, "invalid [services.push_server.notify]; using defaults");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_apply_by_field_name_and_are_validated() {
        let applied = GateOverride {
            loud_ladder_group: Some(vec![60_000, 300_000]),
            vendor_cap: Some(20),
            ..GateOverride::default()
        }
        .apply(GateConfig::default())
        .unwrap();
        assert_eq!(applied.loud_ladder_group.steps_ms(), &[60_000, 300_000]);
        assert_eq!(applied.vendor_cap, 20);
        assert_eq!(applied.quiet_ladder, GateConfig::default().quiet_ladder);

        let decreasing = GateOverride {
            quiet_ladder: Some(vec![5_000, 1_000]),
            ..GateOverride::default()
        };
        assert!(decreasing.apply(GateConfig::default()).is_err());
        let too_short_expiry = GateOverride {
            slot_idle_expiry_ms: Some(1_000),
            ..GateOverride::default()
        };
        assert!(too_short_expiry.apply(GateConfig::default()).is_err());
    }

    #[test]
    fn the_section_is_read_from_toml_with_tenant_overrides() {
        let value: toml::Value = toml::from_str(
            r#"
            [services.push_server.notify]
            due_shards = 4

            [services.push_server.notify.tenants."t-strict"]
            force_hidden_preview = true
            all_mention_interval_ms = 1800000

            [services.push_server.notify.tenants."t-strict".gate]
            vendor_cap = 10

            [services.push_server.notify.tenants."t-broken"]
            all_mention_interval_ms = 0

            [services.push_server.notify.tenants."t-broken".gate]
            vendor_cap = 0
            "#,
        )
        .unwrap();
        let section: NotifySection = value["services"]["push_server"]["notify"]
            .clone()
            .try_into()
            .unwrap();
        let settings = NotifyGateSettings::from_section(Some(section), "redis://x".into());
        assert_eq!(settings.due_shards, 4);
        assert_eq!(settings.due_poll_interval_ms, 250);
        assert!(settings.enabled);
        let policy = settings.policy();
        assert!(policy.of("t-strict").force_hidden_preview);
        assert_eq!(policy.gate_config("t-strict").vendor_cap, 10);
        assert_eq!(
            policy.gate_config("t-broken"),
            GateConfig::default(),
            "不合法的覆盖退回缺省参数"
        );
        assert_eq!(policy.all_mention_interval_ms("t-strict"), 1_800_000);
        assert_eq!(
            policy.all_mention_interval_ms("t-broken"),
            DEFAULT_ALL_MENTION_INTERVAL_MS
        );
        assert_eq!(
            policy.all_mention_interval_ms("someone-else"),
            DEFAULT_ALL_MENTION_INTERVAL_MS
        );
    }

    #[test]
    fn a_missing_config_directory_means_defaults() {
        let settings = NotifyGateSettings::load("/definitely/not/a/config/dir", "redis://x".into());
        assert_eq!(
            settings,
            NotifyGateSettings {
                redis_url: "redis://x".into(),
                ..NotifyGateSettings::default()
            }
        );
    }

    #[test]
    fn later_fragments_override_earlier_ones() {
        let mut base: toml::Value = toml::from_str("a = 1\n[t]\nx = 1\ny = 2").unwrap();
        let overlay: toml::Value = toml::from_str("[t]\ny = 3\nz = 4").unwrap();
        merge(&mut base, overlay);
        assert_eq!(base["a"].as_integer(), Some(1));
        assert_eq!(base["t"]["x"].as_integer(), Some(1));
        assert_eq!(base["t"]["y"].as_integer(), Some(3));
        assert_eq!(base["t"]["z"].as_integer(), Some(4));
    }
}
