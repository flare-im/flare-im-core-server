//! 执行证据与 (应用, Hook) 熔断的参数：环境变量 > 配置文件（`[hook_executions]`、`[hook_circuit]`）>
//! 默认值。超出范围的值被忽略、用默认值。保留期属于 Capability，不在这里。

use std::path::Path;
use std::time::Duration;

use flare_core_base::config::LayeredConfig;

use super::buffer::{HookExecutionBufferSettings, sample_every};
use crate::hooks::circuit::HookCircuitPolicy;

/// 执行证据的写入参数。
#[derive(Debug, Clone, PartialEq)]
pub struct HookExecutionConfig {
    /// 关掉时不写 `hook_executions`（熔断照常）。
    pub enabled: bool,
    pub buffer_capacity: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
    /// 成功（与熔断短路）的采样比例，0 到 1。
    pub success_sample_ratio: f64,
}

impl Default for HookExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            buffer_capacity: 10_000,
            batch_size: 500,
            flush_interval: Duration::from_secs(1),
            success_sample_ratio: 0.01,
        }
    }
}

impl HookExecutionConfig {
    pub fn buffer_settings(&self) -> HookExecutionBufferSettings {
        HookExecutionBufferSettings {
            capacity: self.buffer_capacity,
            batch_size: self.batch_size.min(self.buffer_capacity),
            sample_every: sample_every(self.success_sample_ratio),
        }
    }
}

/// 两组参数一起读。
#[derive(Debug, Clone, PartialEq)]
pub struct HookRuntimeSettings {
    pub executions: HookExecutionConfig,
    pub circuit: HookCircuitPolicy,
}

impl HookRuntimeSettings {
    pub fn from_sources(config_file: Option<&Path>) -> Self {
        let settings = Self::from_layered(&LayeredConfig::from_optional_toml(config_file));
        let executions = &settings.executions;
        let circuit = &settings.circuit;
        tracing::info!(
            executions_enabled = executions.enabled,
            buffer_capacity = executions.buffer_capacity,
            batch_size = executions.batch_size,
            flush_interval_ms = executions.flush_interval.as_millis() as u64,
            success_sample_ratio = executions.success_sample_ratio,
            success_sample_every = sample_every(executions.success_sample_ratio),
            circuit_enabled = circuit.enabled,
            circuit_failure_threshold = circuit.failure_threshold,
            circuit_failure_window_secs = circuit.failure_window.as_secs(),
            circuit_open_secs = circuit.open_for.as_secs(),
            "hook execution evidence and circuit settings loaded"
        );
        settings
    }

    fn from_layered(layered: &LayeredConfig) -> Self {
        let defaults = HookExecutionConfig::default();
        let circuit_defaults = HookCircuitPolicy::default();
        let executions = HookExecutionConfig {
            enabled: layered
                .resolve_bool("FLARE_HOOK_EXECUTIONS_ENABLED", "hook_executions.enabled")
                .unwrap_or(defaults.enabled),
            buffer_capacity: layered
                .resolve_usize(
                    "FLARE_HOOK_EXECUTIONS_BUFFER_CAPACITY",
                    "hook_executions.buffer_capacity",
                )
                .filter(|n| (100..=1_000_000).contains(n))
                .unwrap_or(defaults.buffer_capacity),
            batch_size: layered
                .resolve_usize(
                    "FLARE_HOOK_EXECUTIONS_BATCH_SIZE",
                    "hook_executions.batch_size",
                )
                .filter(|n| (1..=5_000).contains(n))
                .unwrap_or(defaults.batch_size),
            flush_interval: layered
                .resolve_u64(
                    "FLARE_HOOK_EXECUTIONS_FLUSH_INTERVAL_MS",
                    "hook_executions.flush_interval_ms",
                )
                .filter(|n| (50..=60_000).contains(n))
                .map(Duration::from_millis)
                .unwrap_or(defaults.flush_interval),
            success_sample_ratio: resolve_ratio(
                layered,
                "FLARE_HOOK_EXECUTIONS_SUCCESS_SAMPLE_RATIO",
                "hook_executions.success_sample_ratio",
            )
            .unwrap_or(defaults.success_sample_ratio),
        };
        let circuit = HookCircuitPolicy {
            enabled: layered
                .resolve_bool("FLARE_HOOK_CIRCUIT_ENABLED", "hook_circuit.enabled")
                .unwrap_or(circuit_defaults.enabled),
            failure_threshold: layered
                .resolve_u32(
                    "FLARE_HOOK_CIRCUIT_FAILURE_THRESHOLD",
                    "hook_circuit.failure_threshold",
                )
                .filter(|n| (1..=1_000).contains(n))
                .unwrap_or(circuit_defaults.failure_threshold),
            failure_window: layered
                .resolve_u64(
                    "FLARE_HOOK_CIRCUIT_FAILURE_WINDOW_SECS",
                    "hook_circuit.failure_window_secs",
                )
                .filter(|n| (1..=3_600).contains(n))
                .map(Duration::from_secs)
                .unwrap_or(circuit_defaults.failure_window),
            open_for: layered
                .resolve_u64("FLARE_HOOK_CIRCUIT_OPEN_SECS", "hook_circuit.open_secs")
                .filter(|n| (1..=3_600).contains(n))
                .map(Duration::from_secs)
                .unwrap_or(circuit_defaults.open_for),
        };
        Self {
            executions,
            circuit,
        }
    }
}

/// 0 到 1 的比例：环境变量是文本，配置文件可写浮点、整数（0 或 1）或文本。
fn resolve_ratio(layered: &LayeredConfig, env_key: &str, toml_path: &str) -> Option<f64> {
    let textual = layered
        .resolve_nonempty_string(env_key, toml_path)
        .and_then(|raw| raw.parse::<f64>().ok());
    let value = textual.or_else(|| {
        layered.deserialize_toml_path::<f64>(toml_path).or_else(|| {
            layered
                .deserialize_toml_path::<i64>(toml_path)
                .map(|n| n as f64)
        })
    })?;
    (value.is_finite() && (0.0..=1.0).contains(&value)).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_toml(raw: &str) -> HookRuntimeSettings {
        let dir = std::env::temp_dir().join(format!(
            "flare-hook-settings-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("message-ingest.toml");
        std::fs::write(&path, raw).unwrap();
        let settings =
            HookRuntimeSettings::from_layered(&LayeredConfig::from_optional_toml(Some(&path)));
        std::fs::remove_dir_all(&dir).unwrap();
        settings
    }

    #[test]
    fn defaults_apply_without_a_file() {
        let settings = HookRuntimeSettings::from_layered(&LayeredConfig::default());
        assert_eq!(settings.executions, HookExecutionConfig::default());
        assert_eq!(settings.circuit, HookCircuitPolicy::default());
        assert_eq!(settings.executions.buffer_settings().sample_every, 100);
    }

    #[test]
    fn file_values_are_read_and_out_of_range_values_fall_back_to_defaults() {
        let settings = from_toml(
            r#"
[hook_executions]
enabled = false
buffer_capacity = 2000
batch_size = 100
flush_interval_ms = 250
success_sample_ratio = 0.25

[hook_circuit]
enabled = true
failure_threshold = 3
failure_window_secs = 10
open_secs = 5
"#,
        );
        let executions = &settings.executions;
        assert!(!executions.enabled);
        assert_eq!(executions.buffer_capacity, 2_000);
        assert_eq!(executions.batch_size, 100);
        assert_eq!(executions.flush_interval, Duration::from_millis(250));
        assert_eq!(executions.success_sample_ratio, 0.25);
        assert_eq!(executions.buffer_settings().sample_every, 4);
        assert_eq!(settings.circuit.failure_threshold, 3);
        assert_eq!(settings.circuit.failure_window, Duration::from_secs(10));
        assert_eq!(settings.circuit.open_for, Duration::from_secs(5));

        let settings = from_toml(
            r#"
[hook_executions]
buffer_capacity = 10
success_sample_ratio = 1.5

[hook_circuit]
failure_threshold = 0
open_secs = 99999
"#,
        );
        let defaults = HookExecutionConfig::default();
        assert_eq!(
            settings.executions.buffer_capacity,
            defaults.buffer_capacity
        );
        assert_eq!(
            settings.executions.success_sample_ratio,
            defaults.success_sample_ratio
        );
        let circuit = HookCircuitPolicy::default();
        assert_eq!(
            settings.circuit.failure_threshold,
            circuit.failure_threshold
        );
        assert_eq!(settings.circuit.open_for, circuit.open_for);

        let settings = from_toml("[hook_executions]\nsuccess_sample_ratio = 1\n");
        assert_eq!(settings.executions.success_sample_ratio, 1.0);
        assert_eq!(settings.executions.buffer_settings().sample_every, 1);
        let settings = from_toml("[hook_executions]\nsuccess_sample_ratio = 0\n");
        assert_eq!(settings.executions.buffer_settings().sample_every, 0);
    }
}
