//! 经 `RegisterPluginEndpoint` 登记的插件端点（持久化行）。
//!
//! 插件端点的读模型是进程内的 `PluginRouteBook`；有库时登记写进 `capability_plugin_endpoints`，
//! 各实例从表里收敛。配置发现来的端点（`plugin_discovery_endpoints`）不是这里的记录。

use flare_grpc_proto::capability::RegisteredPluginInstance;

/// `(tenant_id, plugin_id)`：一条登记的主键。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PluginEndpointKey {
    pub tenant_id: String,
    pub plugin_id: String,
}

impl PluginEndpointKey {
    pub fn new(tenant_id: impl Into<String>, plugin_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            plugin_id: plugin_id.into(),
        }
    }
}

/// 一条登记：租户与插件实例描述（与 `ListRegisteredPlugins` 返回的形状相同）。
#[derive(Debug, Clone, PartialEq)]
pub struct PluginEndpoint {
    pub tenant_id: String,
    pub instance: RegisteredPluginInstance,
}

impl PluginEndpoint {
    pub fn key(&self) -> PluginEndpointKey {
        PluginEndpointKey::new(&self.tenant_id, &self.instance.plugin_id)
    }
}

/// `plugin_id`、`capability_id` 与 `declared_operations` 每一项的长度上限，与表的 CHECK 一致。
pub const MAX_PLUGIN_IDENTIFIER_LEN: usize = 128;

/// 插件标识（`plugin_id`、`capability_id`、`declared_operations` 的每一项）：1 到
/// [`MAX_PLUGIN_IDENTIFIER_LEN`] 个 ASCII 字母、数字或 `_ - . :`，与路由表对路径参数的字符集相同。
/// 这些值来自 Admin 接口与插件自报，会原样进日志和错误信息，所以空白、控制字符和其他符号一律拒绝。
pub fn is_plugin_identifier(value: &str) -> bool {
    (1..=MAX_PLUGIN_IDENTIFIER_LEN).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

/// 一条登记能否入库：与 `capability_plugin_endpoints` 的 CHECK 一致，在 RPC 层就拒绝，
/// 免得存储错误变成 INTERNAL；标识另按 [`is_plugin_identifier`] 收紧。返回违反的字段说明，
/// 说明里不带取值。
pub fn validate_plugin_endpoint(endpoint: &PluginEndpoint) -> Result<(), &'static str> {
    fn within(value: &str, min: usize, max: usize) -> bool {
        (min..=max).contains(&value.chars().count())
    }
    let instance = &endpoint.instance;
    if !within(&endpoint.tenant_id, 1, 256) || endpoint.tenant_id == "*" {
        return Err("tenant_id must be one concrete tenant of at most 256 characters");
    }
    if !is_plugin_identifier(&instance.plugin_id) {
        return Err("plugin_id must be 1 to 128 ASCII letters, digits or _ - . :");
    }
    if !is_plugin_identifier(&instance.capability_id) {
        return Err("capability_id must be 1 to 128 ASCII letters, digits or _ - . :");
    }
    if !within(&instance.grpc_authority, 1, 512) {
        return Err("grpc_authority must be 1 to 512 characters");
    }
    if !within(&instance.plugin_version, 0, 64)
        || !within(&instance.api_version, 0, 64)
        || !within(&instance.manifest_sha256, 0, 128)
    {
        return Err(
            "plugin_version and api_version are at most 64, manifest_sha256 128 characters",
        );
    }
    if instance.declared_operations.len() > 256 {
        return Err("at most 256 declared_operations");
    }
    // 声明的每一项都按 capability_id 匹配（见 `PluginRouteBook::serves`），规则相同。
    if !instance
        .declared_operations
        .iter()
        .all(|op| is_plugin_identifier(op))
    {
        return Err(
            "each declared_operations entry must be 1 to 128 ASCII letters, digits or _ - . :",
        );
    }
    // The table allows 16384 bytes of `labels::text`; PostgreSQL prints JSONB with a space after
    // every `:` and `,`, so the compact encoding is held to half of that.
    let labels = serde_json::to_string(&instance.labels).unwrap_or_default();
    if instance.labels.len() > 128 || labels.len() > 8_192 {
        return Err("at most 128 labels, encoding to at most 8192 bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> PluginEndpoint {
        PluginEndpoint {
            tenant_id: "t1".into(),
            instance: RegisteredPluginInstance {
                plugin_id: "p1".into(),
                capability_id: "rtc.call".into(),
                grpc_authority: "http://p1:9".into(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn endpoints_are_bounded_like_the_table() {
        assert!(validate_plugin_endpoint(&endpoint()).is_ok());
        let mut wildcard = endpoint();
        wildcard.tenant_id = "*".into();
        let mut long_plugin = endpoint();
        long_plugin.instance.plugin_id = "p".repeat(129);
        let mut no_authority = endpoint();
        no_authority.instance.grpc_authority.clear();
        let mut operations = endpoint();
        operations.instance.declared_operations = vec!["op".into(); 257];
        let mut labels = endpoint();
        labels.instance.labels.insert("k".into(), "v".repeat(8_192));
        for invalid in [wildcard, long_plugin, no_authority, operations, labels] {
            assert!(validate_plugin_endpoint(&invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn identifiers_are_ascii_words_up_to_128_bytes() {
        for valid in [
            "p",
            "flare-strom-sfu",
            "rtc.media.control",
            "social.moments.delete_moment",
            "ns:op-1",
            "p".repeat(128).as_str(),
        ] {
            assert!(is_plugin_identifier(valid), "{valid:?}");
        }
        for invalid in [
            "",
            "p".repeat(129).as_str(),
            "p1\nINFO forged line",
            "p1\r",
            "p1\u{1b}[31m",
            "p 1",
            "p1/x",
            "p1\"",
            "插件",
            "p1\u{0}",
        ] {
            assert!(!is_plugin_identifier(invalid), "{invalid:?}");
        }
    }

    #[test]
    fn plugin_ids_capability_ids_and_declared_operations_must_be_identifiers() {
        let mut plugin = endpoint();
        plugin.instance.plugin_id = "p1\nINFO forged line".into();
        let mut capability = endpoint();
        capability.instance.capability_id = "rtc.call\r\nWARN x".into();
        let mut operation = endpoint();
        operation.instance.declared_operations = vec!["rtc.call".into(), "rtc.hang up".into()];
        let mut empty_operation = endpoint();
        empty_operation.instance.declared_operations = vec!["rtc.call".into(), String::new()];
        for (invalid, field) in [
            (plugin, "plugin_id"),
            (capability, "capability_id"),
            (operation, "declared_operations"),
            (empty_operation, "declared_operations"),
        ] {
            let error = validate_plugin_endpoint(&invalid).unwrap_err();
            assert!(error.contains(field), "{error}");
            assert!(!error.contains('\n') && !error.contains('\r'), "{error}");
        }
        let mut declared = endpoint();
        declared.instance.declared_operations = vec!["rtc.call".into(), "rtc.hangup".into()];
        assert!(validate_plugin_endpoint(&declared).is_ok());
    }
}
