use std::{collections::HashSet, env, fs, os::unix::fs::PermissionsExt, path::Path};

use crate::error::{Context, Result, ensure, fail};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    version: u32,
    #[serde(default)]
    pub(crate) shared_env: Vec<String>,
    #[serde(default)]
    pub(crate) shared_env_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) required_env: Vec<String>,
    #[serde(default)]
    required_env_unless: Vec<RequiredEnvUnless>,
    #[serde(default)]
    pub(crate) forbidden_env: Vec<String>,
    #[serde(default)]
    pub(crate) restart: Restart,
    pub(crate) services: Vec<Service>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequiredEnvUnless {
    name: String,
    when_env: String,
    equals: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Restart {
    #[serde(default = "default_min_delay")]
    pub(crate) min_delay_ms: u64,
    #[serde(default = "default_max_delay")]
    pub(crate) max_delay_ms: u64,
    #[serde(default = "default_reset_after")]
    pub(crate) reset_after_secs: u64,
    #[serde(default = "default_stop_grace")]
    pub(crate) stop_grace_secs: u64,
}

impl Default for Restart {
    fn default() -> Self {
        Self {
            min_delay_ms: default_min_delay(),
            max_delay_ms: default_max_delay(),
            reset_after_secs: default_reset_after(),
            stop_grace_secs: default_stop_grace(),
        }
    }
}

const fn default_min_delay() -> u64 {
    500
}
const fn default_max_delay() -> u64 {
    30_000
}
const fn default_reset_after() -> u64 {
    60
}
const fn default_stop_grace() -> u64 {
    20
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Service {
    pub(crate) name: String,
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    #[serde(default)]
    pub(crate) inherit_env: Vec<String>,
    #[serde(default)]
    pub(crate) inherit_env_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) required_env: Vec<String>,
    #[serde(default)]
    required_env_unless: Vec<RequiredEnvUnless>,
    #[serde(default)]
    pub(crate) enabled_if_env: Option<String>,
    #[serde(default)]
    pub(crate) environment: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) tcp_health: Option<String>,
}

impl Manifest {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let source = fs::read_to_string(path)
            .with_context(|| format!("read supervisor manifest {}", path.display()))?;
        let manifest: Self = toml::from_str(&source)
            .with_context(|| format!("parse supervisor manifest {}", path.display()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "supervisor manifest version must be 1");
        ensure!(
            !self.services.is_empty(),
            "supervisor manifest has no services"
        );
        ensure!(
            self.restart.min_delay_ms > 0,
            "restart.min_delay_ms must be positive"
        );
        ensure!(
            self.restart.max_delay_ms >= self.restart.min_delay_ms,
            "restart.max_delay_ms must be >= min_delay_ms"
        );
        ensure!(
            self.restart.reset_after_secs > 0,
            "restart.reset_after_secs must be positive"
        );
        ensure!(
            self.restart.stop_grace_secs > 0,
            "restart.stop_grace_secs must be positive"
        );
        for name in self
            .shared_env
            .iter()
            .chain(&self.required_env)
            .chain(&self.forbidden_env)
        {
            validate_env_name(name)?;
        }
        for prefix in &self.shared_env_prefixes {
            validate_env_name(prefix)?;
        }
        validate_required_env_unless(
            "global",
            &self.required_env_unless,
            &self.required_env,
            |name| self.shared_env.iter().any(|shared| shared == name),
        )?;
        for forbidden in &self.forbidden_env {
            ensure!(
                !self.shared_env.iter().any(|shared| shared == forbidden),
                "forbidden environment variable is shared: {forbidden}"
            );
        }
        let mut names = HashSet::new();
        for service in &self.services {
            ensure!(
                valid_service_name(&service.name),
                "invalid service name: {}",
                service.name
            );
            ensure!(
                names.insert(service.name.as_str()),
                "duplicate service: {}",
                service.name
            );
            ensure!(
                Path::new(&service.command).is_absolute(),
                "{} command must be absolute",
                service.name
            );
            for name in service.inherit_env.iter().chain(&service.required_env) {
                validate_env_name(name)?;
            }
            for prefix in &service.inherit_env_prefixes {
                validate_env_name(prefix)?;
            }
            validate_required_env_unless(
                &service.name,
                &service.required_env_unless,
                &service.required_env,
                |name| self.env_allowed(service, name),
            )?;
            for name in service.environment.keys() {
                validate_env_name(name)?;
                ensure!(
                    !self.forbidden_env.iter().any(|forbidden| forbidden == name),
                    "{} injects forbidden environment variable {name}",
                    service.name
                );
            }
            for condition in self
                .required_env_unless
                .iter()
                .chain(&service.required_env_unless)
            {
                ensure!(
                    !service.environment.contains_key(&condition.when_env),
                    "{} overrides conditional mode {}",
                    service.name,
                    condition.when_env
                );
            }
            if let Some(name) = &service.enabled_if_env {
                validate_env_name(name)?;
            }
            if let Some(address) = &service.tcp_health {
                ensure!(
                    address.parse::<std::net::SocketAddr>().is_ok(),
                    "{} tcp_health must be numeric host:port",
                    service.name
                );
            }
            for name in self.required_env.iter().chain(&service.required_env) {
                ensure!(
                    self.env_allowed(service, name) || service.environment.contains_key(name),
                    "{} required env {} is not allowed to child",
                    service.name,
                    name
                );
            }
        }
        Ok(())
    }

    pub(crate) fn select(&self, selector: &str) -> Result<Vec<Service>> {
        self.select_with_env(selector, |name| env::var(name).ok())
    }

    fn select_with_env(
        &self,
        selector: &str,
        env_value: impl Fn(&str) -> Option<String>,
    ) -> Result<Vec<Service>> {
        for forbidden in &self.forbidden_env {
            ensure!(
                !env_is_set(env_value(forbidden).as_deref()),
                "forbidden control-plane credential present in supervisor environment: {forbidden}"
            );
        }
        let names: Vec<&str> = if selector == "all" {
            self.services
                .iter()
                .map(|service| service.name.as_str())
                .collect()
        } else {
            selector.split(',').collect()
        };
        ensure!(!names.is_empty(), "empty service selection");
        let mut seen = HashSet::new();
        let mut selected = Vec::new();
        for name in names {
            ensure!(
                valid_service_name(name),
                "invalid service selection: {name}"
            );
            ensure!(seen.insert(name), "duplicate service selection: {name}");
            let service = self
                .services
                .iter()
                .find(|service| service.name == name)
                .with_context(|| format!("unknown service: {name}"))?;
            if let Some(condition) = &service.enabled_if_env
                && !env_is_set(env_value(condition).as_deref())
            {
                if selector == "all" {
                    continue;
                }
                fail!("{} requires nonempty {}", service.name, condition);
            }
            let require_present = |required: &String| -> Result<()> {
                let present = service
                    .environment
                    .get(required)
                    .map(|value| !value.is_empty())
                    .unwrap_or_else(|| env_is_set(env_value(required).as_deref()));
                ensure!(present, "{} requires nonempty {}", service.name, required);
                Ok(())
            };
            for required in &self.required_env {
                if requirement_exempt(&self.required_env_unless, required, &env_value) {
                    continue;
                }
                require_present(required)?;
            }
            for required in &service.required_env {
                if requirement_exempt(&service.required_env_unless, required, &env_value) {
                    continue;
                }
                require_present(required)?;
            }
            let meta = fs::metadata(&service.command).with_context(|| {
                format!("{} binary unavailable: {}", service.name, service.command)
            })?;
            ensure!(
                meta.is_file() && meta.permissions().mode() & 0o111 != 0,
                "{} command is not executable: {}",
                service.name,
                service.command
            );
            selected.push(service.clone());
        }
        ensure!(!selected.is_empty(), "no services selected");
        Ok(selected)
    }

    pub(crate) fn env_allowed(&self, service: &Service, name: &str) -> bool {
        if self.forbidden_env.iter().any(|forbidden| forbidden == name) {
            return false;
        }
        self.shared_env.iter().any(|allowed| allowed == name)
            || self
                .shared_env_prefixes
                .iter()
                .any(|prefix| name.starts_with(prefix))
            || service.inherit_env.iter().any(|allowed| allowed == name)
            || service
                .inherit_env_prefixes
                .iter()
                .any(|prefix| name.starts_with(prefix))
    }
}

fn requirement_exempt(
    conditions: &[RequiredEnvUnless],
    required: &str,
    env_value: &impl Fn(&str) -> Option<String>,
) -> bool {
    conditions.iter().any(|condition| {
        condition.name == required
            && env_value(&condition.when_env).as_deref() == Some(condition.equals.as_str())
    })
}

fn validate_required_env_unless(
    scope: &str,
    conditions: &[RequiredEnvUnless],
    required_env: &[String],
    mode_allowed: impl Fn(&str) -> bool,
) -> Result<()> {
    let mut conditional_names = HashSet::new();
    for condition in conditions {
        validate_env_name(&condition.name)?;
        validate_env_name(&condition.when_env)?;
        ensure!(
            required_env.contains(&condition.name),
            "{scope} conditional requirement {} is not required in its own scope",
            condition.name
        );
        ensure!(
            mode_allowed(&condition.when_env),
            "{scope} conditional requirement {} checks an env not inherited by the child: {}",
            condition.name,
            condition.when_env
        );
        ensure!(
            !condition.equals.is_empty() && condition.equals.trim() == condition.equals,
            "{scope} conditional requirement {} has an empty or padded value",
            condition.name
        );
        ensure!(
            conditional_names.insert(condition.name.as_str()),
            "{scope} duplicate conditional requirement: {}",
            condition.name
        );
    }
    Ok(())
}

fn env_is_set(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn valid_service_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

fn validate_env_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
            && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'),
        "invalid environment variable name: {name}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Manifest {
        toml::from_str(
            r#"
version = 1
shared_env = ["RUST_LOG"]
[[services]]
name = "api"
command = "/bin/sh"
inherit_env = ["FLARE_INTERNAL_CTX_PRIVATE_KEY"]
required_env = ["FLARE_INTERNAL_CTX_PRIVATE_KEY"]
[[services]]
name = "worker"
command = "/bin/sh"
"#,
        )
        .unwrap()
    }

    fn conditional_fixture() -> Manifest {
        let manifest: Manifest = toml::from_str(
            r#"
version = 1
shared_env = ["FLARE_CONFIG_PATH", "FLARE_REGISTRY_MODE", "FLARE_CONSUL_ENDPOINT"]
required_env = ["FLARE_CONFIG_PATH", "FLARE_CONSUL_ENDPOINT"]
required_env_unless = [
  { name = "FLARE_CONSUL_ENDPOINT", when_env = "FLARE_REGISTRY_MODE", equals = "static" },
]
[[services]]
name = "api"
command = "/bin/sh"
"#,
        )
        .unwrap();
        manifest.validate().unwrap();
        manifest
    }

    fn select_with_mode(
        manifest: &Manifest,
        mode: Option<&str>,
        consul: Option<&str>,
        config_path: Option<&str>,
    ) -> Result<Vec<Service>> {
        manifest.select_with_env("api", |name| match name {
            "FLARE_REGISTRY_MODE" => mode.map(str::to_owned),
            "FLARE_CONSUL_ENDPOINT" => consul.map(str::to_owned),
            "FLARE_CONFIG_PATH" => config_path.map(str::to_owned),
            _ => None,
        })
    }

    fn service_conditional_fixture() -> Manifest {
        let manifest: Manifest = toml::from_str(
            r#"
version = 1
[[services]]
name = "management"
command = "/bin/sh"
[[services]]
name = "data"
command = "/bin/sh"
inherit_env = ["SOCIAL_REGISTRY_MODE", "SOCIAL_CONSUL_ENDPOINT"]
required_env = ["SOCIAL_CONSUL_ENDPOINT"]
required_env_unless = [
  { name = "SOCIAL_CONSUL_ENDPOINT", when_env = "SOCIAL_REGISTRY_MODE", equals = "static" },
]
"#,
        )
        .unwrap();
        manifest.validate().unwrap();
        manifest
    }

    fn select_social_with_mode(
        manifest: &Manifest,
        selector: &str,
        mode: Option<&str>,
        consul: Option<&str>,
    ) -> Result<Vec<Service>> {
        manifest.select_with_env(selector, |name| match name {
            "SOCIAL_REGISTRY_MODE" => mode.map(str::to_owned),
            "SOCIAL_CONSUL_ENDPOINT" => consul.map(str::to_owned),
            _ => None,
        })
    }

    #[test]
    fn explicit_environment_scope_does_not_leak_signing_key() {
        let manifest = fixture();
        manifest.validate().unwrap();
        assert!(manifest.env_allowed(&manifest.services[0], "FLARE_INTERNAL_CTX_PRIVATE_KEY"));
        assert!(!manifest.env_allowed(&manifest.services[1], "FLARE_INTERNAL_CTX_PRIVATE_KEY"));
        assert!(!manifest.env_allowed(&manifest.services[0], "FLARE_CONSOLE_SIGNING_KEY"));
    }

    #[test]
    fn rejects_unknown_fields_and_unsafe_commands() {
        assert!(toml::from_str::<Manifest>("version=1\nunknown=1\n").is_err());
        let mut manifest = fixture();
        manifest.services[0].command = "sh".into();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn missing_required_key_is_not_silently_ignored() {
        let manifest = fixture();
        let name = "FLARE_INTERNAL_CTX_PRIVATE_KEY";
        if !env_is_set(env::var(name).ok().as_deref()) {
            assert!(
                manifest
                    .select("api")
                    .unwrap_err()
                    .to_string()
                    .contains(name)
            );
        }
        assert!(manifest.select("worker").is_ok());
        assert!(manifest.select("worker,worker").is_err());
        assert!(manifest.select("unknown").is_err());
    }

    #[test]
    fn static_mode_alone_exempts_consul_but_not_other_required_env() {
        let manifest = conditional_fixture();
        assert!(select_with_mode(&manifest, Some("static"), None, Some("/etc/flare")).is_ok());
        let error = select_with_mode(&manifest, Some("static"), None, None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("FLARE_CONFIG_PATH"), "{error}");

        let mut manifest = conditional_fixture();
        manifest.services[0]
            .required_env
            .push("FLARE_CONSUL_ENDPOINT".into());
        let error = select_with_mode(&manifest, Some("static"), None, Some("/etc/flare"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("FLARE_CONSUL_ENDPOINT"), "{error}");
    }

    #[test]
    fn configured_unset_or_inexact_mode_still_requires_consul() {
        let manifest = conditional_fixture();
        for mode in [None, Some("configured"), Some("Static"), Some("static ")] {
            let error = select_with_mode(&manifest, mode, None, Some("/etc/flare"))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("FLARE_CONSUL_ENDPOINT"),
                "mode={mode:?}: {error}"
            );
        }
        assert!(
            select_with_mode(
                &manifest,
                Some("configured"),
                Some("http://consul:8500"),
                Some("/etc/flare")
            )
            .is_ok()
        );
    }

    #[test]
    fn conditional_requirement_must_match_a_global_required_env_and_shared_mode() {
        let mut manifest = conditional_fixture();
        manifest.required_env_unless[0].name = "FLARE_CONSUL_ENDPONT".into();
        assert!(manifest.validate().is_err());

        let mut manifest = conditional_fixture();
        manifest
            .shared_env
            .retain(|name| name != "FLARE_REGISTRY_MODE");
        assert!(manifest.validate().is_err());

        let mut manifest = conditional_fixture();
        manifest.required_env_unless[0].equals = " static".into();
        assert!(manifest.validate().is_err());

        let mut manifest = conditional_fixture();
        let duplicate = manifest.required_env_unless[0].clone();
        manifest.required_env_unless.push(duplicate);
        assert!(manifest.validate().is_err());

        let mut manifest = conditional_fixture();
        manifest.services[0]
            .environment
            .insert("FLARE_REGISTRY_MODE".into(), "configured".into());
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn service_conditional_requirement_does_not_affect_management_only_selection() {
        let manifest = service_conditional_fixture();
        assert!(select_social_with_mode(&manifest, "management", None, None).is_ok());
        assert!(select_social_with_mode(&manifest, "all", Some("static"), None).is_ok());
        assert!(select_social_with_mode(&manifest, "data", Some("static"), None).is_ok());
        for mode in [None, Some("configured"), Some("Static"), Some("static ")] {
            let error = select_social_with_mode(&manifest, "data", mode, None)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("SOCIAL_CONSUL_ENDPOINT"),
                "mode={mode:?}: {error}"
            );
        }
        assert!(
            select_social_with_mode(
                &manifest,
                "data",
                Some("configured"),
                Some("http://consul:8500")
            )
            .is_ok()
        );
    }

    #[test]
    fn service_conditional_requirement_must_be_local_inherited_and_not_overridden() {
        let mut manifest = service_conditional_fixture();
        manifest.services[1].required_env.clear();
        assert!(manifest.validate().is_err());

        let mut manifest = service_conditional_fixture();
        manifest.services[1]
            .inherit_env
            .retain(|name| name != "SOCIAL_REGISTRY_MODE");
        assert!(manifest.validate().is_err());

        let mut manifest = service_conditional_fixture();
        manifest.services[1].required_env_unless[0].equals = " static".into();
        assert!(manifest.validate().is_err());

        let mut manifest = service_conditional_fixture();
        let duplicate = manifest.services[1].required_env_unless[0].clone();
        manifest.services[1].required_env_unless.push(duplicate);
        assert!(manifest.validate().is_err());

        let mut manifest = service_conditional_fixture();
        manifest.services[1]
            .environment
            .insert("SOCIAL_REGISTRY_MODE".into(), "configured".into());
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn core_manifest_covers_all_existing_binaries_and_scopes_private_keys() {
        let manifest: Manifest =
            toml::from_str(include_str!("../../../release/supervise-core.toml")).unwrap();
        manifest.validate().unwrap();
        assert_eq!(manifest.services.len(), 16);
        assert!(
            manifest
                .required_env
                .iter()
                .any(|name| name == "FLARE_CONSUL_ENDPOINT")
        );
        assert_eq!(manifest.required_env_unless.len(), 1);
        assert_eq!(
            manifest.required_env_unless[0].name,
            "FLARE_CONSUL_ENDPOINT"
        );
        assert_eq!(
            manifest.required_env_unless[0].when_env,
            "FLARE_REGISTRY_MODE"
        );
        assert_eq!(manifest.required_env_unless[0].equals, "static");
        let signer_users: Vec<_> = manifest
            .services
            .iter()
            .filter(|service| manifest.env_allowed(service, "FLARE_INTERNAL_CTX_PRIVATE_KEY"))
            .map(|service| service.name.as_str())
            .collect();
        assert_eq!(signer_users, ["api-gateway", "signaling-gateway"]);
        let push_proxy = manifest
            .services
            .iter()
            .find(|service| service.name == "push-proxy")
            .unwrap();
        assert!(manifest.env_allowed(push_proxy, "FLARE_INTERNAL_CTX_PUBLIC_KEY"));
        assert!(
            push_proxy
                .required_env
                .iter()
                .any(|name| name == "FLARE_INTERNAL_CTX_PUBLIC_KEY")
        );
        assert!(!manifest.env_allowed(push_proxy, "FLARE_INTERNAL_CTX_PRIVATE_KEY"));
        let gateway = manifest
            .services
            .iter()
            .find(|service| service.name == "signaling-gateway")
            .unwrap();
        assert_eq!(
            gateway.environment.get("PORT").map(String::as_str),
            Some("60051")
        );
        assert_eq!(
            gateway.environment.get("GRPC_PORT").map(String::as_str),
            Some("60060")
        );
        assert!(!manifest.env_allowed(gateway, "PORT"));
        assert!(!manifest.env_allowed(gateway, "GRPC_PORT"));
        assert_eq!(gateway.tcp_health.as_deref(), Some("127.0.0.1:60051"));
        let push_worker = manifest
            .services
            .iter()
            .find(|service| service.name == "push-worker")
            .unwrap();
        let push_server = manifest
            .services
            .iter()
            .find(|service| service.name == "push-server")
            .unwrap();
        assert_eq!(
            push_server.enabled_if_env.as_deref(),
            Some("PUSH_SERVER_SIGNED_PROVENANCE_READY")
        );
        assert_eq!(
            push_worker.enabled_if_env.as_deref(),
            Some("PUSH_WORKER_SIGNED_PROVENANCE_READY")
        );
        assert!(manifest.env_allowed(push_worker, "PUSH_WORKER_APNS_PRIVATE_KEY"));
        assert!(
            !manifest
                .services
                .iter()
                .filter(|service| service.name != "push-worker")
                .any(|service| manifest.env_allowed(service, "PUSH_WORKER_APNS_PRIVATE_KEY"))
        );
        assert!(
            manifest
                .forbidden_env
                .iter()
                .any(|name| name == "FLARE_CONSOLE_SIGNING_KEY")
        );
    }
}
