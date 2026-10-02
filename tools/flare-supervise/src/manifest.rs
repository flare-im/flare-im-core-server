use std::{
    collections::{BTreeMap, HashSet},
    env,
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
};

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

/// Values a child receives only while the parent environment selects one mode,
/// and only for variables the parent does not set at all. A value the parent
/// sets, even an empty one, reaches the child unchanged.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnvironmentDefaults {
    when_env: String,
    equals: String,
    values: BTreeMap<String, String>,
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
    pub(crate) environment: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) environment_defaults: Vec<EnvironmentDefaults>,
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
            self.validate_environment_defaults(service)?;
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

    /// A default must stay overridable by the parent (so it names an inherited
    /// variable), and its mode must be the one the child sees.
    fn validate_environment_defaults(&self, service: &Service) -> Result<()> {
        let mut names = HashSet::new();
        for defaults in &service.environment_defaults {
            validate_env_name(&defaults.when_env)?;
            ensure!(
                self.env_allowed(service, &defaults.when_env),
                "{} environment defaults check an env not inherited by the child: {}",
                service.name,
                defaults.when_env
            );
            ensure!(
                !service.environment.contains_key(&defaults.when_env),
                "{} overrides the mode of its environment defaults: {}",
                service.name,
                defaults.when_env
            );
            ensure!(
                !defaults.equals.is_empty() && defaults.equals.trim() == defaults.equals,
                "{} environment defaults for {} have an empty or padded mode value",
                service.name,
                defaults.when_env
            );
            ensure!(
                !defaults.values.is_empty(),
                "{} environment defaults for {} are empty",
                service.name,
                defaults.when_env
            );
            for (name, value) in &defaults.values {
                validate_env_name(name)?;
                ensure!(
                    !service
                        .environment_defaults
                        .iter()
                        .any(|other| &other.when_env == name),
                    "{} environment default sets a mode its defaults depend on: {name}",
                    service.name
                );
                ensure!(
                    self.env_allowed(service, name),
                    "{} environment default {name} is not inherited by the child, so the parent could not override it",
                    service.name
                );
                ensure!(
                    !service.environment.contains_key(name),
                    "{} sets {name} both as a literal and as an environment default",
                    service.name
                );
                ensure!(
                    !value.is_empty() && value.trim() == value,
                    "{} environment default {name} is empty or padded",
                    service.name
                );
                ensure!(
                    names.insert(name.as_str()),
                    "{} duplicate environment default: {name}",
                    service.name
                );
            }
        }
        Ok(())
    }

    /// The child's environment: the parent variables it may inherit, then the
    /// defaults of the selected mode for variables the parent does not set,
    /// then the service's `environment` literals, which override everything.
    pub(crate) fn child_environment(
        &self,
        service: &Service,
        parent: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Vec<(OsString, OsString)> {
        let mut child: BTreeMap<OsString, OsString> = parent
            .into_iter()
            .filter(|(key, _)| {
                key.to_str()
                    .is_some_and(|name| self.env_allowed(service, name))
            })
            .collect();
        for defaults in &service.environment_defaults {
            let selected = child
                .get(&OsString::from(&defaults.when_env))
                .is_some_and(|mode| mode.to_str() == Some(defaults.equals.as_str()));
            if !selected {
                continue;
            }
            for (name, value) in &defaults.values {
                child
                    .entry(OsString::from(name))
                    .or_insert_with(|| OsString::from(value));
            }
        }
        for (name, value) in &service.environment {
            child.insert(OsString::from(name), OsString::from(value));
        }
        child.into_iter().collect()
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
    fn core_manifest_hands_console_public_key_to_every_admin_verifier() {
        let manifest: Manifest =
            toml::from_str(include_str!("../../../release/supervise-core.toml")).unwrap();
        // Children start from an empty environment and only receive what
        // `env_allowed` admits. Online and Capability verify Control assertions;
        // Online, Capability, Conversation and Storage Reader also verify the
        // admin assertion Admin Gateway forwards (SEC-02, ADM-01). A verifier
        // started without the key answers every admin RPC with UNAUTHENTICATED.
        let key_users: Vec<_> = manifest
            .services
            .iter()
            .filter(|service| manifest.env_allowed(service, "FLARE_CONSOLE_PUBLIC_KEY"))
            .map(|service| service.name.as_str())
            .collect();
        assert_eq!(
            key_users,
            [
                "signaling-online",
                "capability",
                "conversation",
                "storage-reader",
                "admin-gateway"
            ]
        );
        for private in ["FLARE_CONSOLE_SIGNING_KEY", "FLARE_CONSOLE_PRIVATE_KEY"] {
            assert!(
                !manifest
                    .services
                    .iter()
                    .any(|service| manifest.env_allowed(service, private)),
                "{private}"
            );
        }
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
        // Push Server/Worker attest broker records (broker-isolated); Push Proxy only verifies.
        // Admin Gateway signs the Control-verified tenant admin for Media.
        assert_eq!(
            signer_users,
            [
                "push-server",
                "push-worker",
                "api-gateway",
                "signaling-gateway",
                "admin-gateway"
            ]
        );
        let media = manifest
            .services
            .iter()
            .find(|service| service.name == "media")
            .unwrap();
        assert!(
            media
                .required_env
                .iter()
                .any(|name| name == "FLARE_INTERNAL_CTX_PUBLIC_KEY")
        );
        assert!(!manifest.env_allowed(media, "FLARE_INTERNAL_CTX_PRIVATE_KEY"));
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
        let quic_key_users: Vec<_> = manifest
            .services
            .iter()
            .filter(|service| manifest.env_allowed(service, "FLARE_QUIC_TLS_KEY"))
            .map(|service| service.name.as_str())
            .collect();
        assert_eq!(quic_key_users, ["signaling-gateway"]);
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
            Some("PUSH_SERVER_DELIVERY_READY")
        );
        assert_eq!(
            push_worker.enabled_if_env.as_deref(),
            Some("PUSH_WORKER_DELIVERY_READY")
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
        // Every data-plane tenant gate is pinned to strict. `environment` is
        // applied after inherited variables, so an inherited lenient value
        // cannot re-admit unknown or unprojected tenants.
        for (service_name, policy_env) in [
            ("signaling-online", "SIGNALING_ONLINE_TENANT_POLICY"),
            ("message-ingest", "MESSAGE_INGEST_TENANT_POLICY"),
            ("api-gateway", "FLARE_API_GATEWAY_TENANT_POLICY"),
            ("signaling-gateway", "ACCESS_GATEWAY_TENANT_POLICY"),
        ] {
            let service = manifest
                .services
                .iter()
                .find(|service| service.name == service_name)
                .unwrap();
            assert_eq!(
                service.environment.get(policy_env).map(String::as_str),
                Some("strict"),
                "{service_name}"
            );
        }
    }

    fn defaults_fixture(extra: &str) -> std::result::Result<Manifest, String> {
        let manifest: Manifest = toml::from_str(&format!(
            r#"
version = 1
shared_env = ["MODE"]
[[services]]
name = "gateway"
command = "/bin/sh"
inherit_env_prefixes = ["GW_"]
[services.environment]
GW_POLICY = "strict"
{extra}
"#
        ))
        .map_err(|error| error.to_string())?;
        manifest
            .validate()
            .map(|()| manifest)
            .map_err(|error| error.to_string())
    }

    const STATIC_DEFAULTS: &str = r#"
[[services.environment_defaults]]
when_env = "MODE"
equals = "static"
[services.environment_defaults.values]
GW_MEDIA_URL = "http://127.0.0.1:60081"
GW_ONLINE_URL = "http://127.0.0.1:50061"
"#;

    fn child(manifest: &Manifest, parent: &[(&str, &str)]) -> BTreeMap<String, String> {
        manifest
            .child_environment(
                &manifest.services[0],
                parent
                    .iter()
                    .map(|(key, value)| (OsString::from(key), OsString::from(value))),
            )
            .into_iter()
            .map(|(key, value)| (key.into_string().unwrap(), value.into_string().unwrap()))
            .collect()
    }

    #[test]
    fn environment_defaults_apply_only_in_their_mode_and_never_over_the_parent() {
        let manifest = defaults_fixture(STATIC_DEFAULTS).unwrap();
        let static_mode = child(&manifest, &[("MODE", "static"), ("OTHER", "x")]);
        assert_eq!(
            static_mode,
            BTreeMap::from([
                ("MODE".to_string(), "static".to_string()),
                (
                    "GW_MEDIA_URL".to_string(),
                    "http://127.0.0.1:60081".to_string()
                ),
                (
                    "GW_ONLINE_URL".to_string(),
                    "http://127.0.0.1:50061".to_string()
                ),
                ("GW_POLICY".to_string(), "strict".to_string()),
            ])
        );
        // The parent's value wins, an empty one included (it may mean "off").
        let set = child(
            &manifest,
            &[
                ("MODE", "static"),
                ("GW_MEDIA_URL", "http://media.internal:60081"),
                ("GW_ONLINE_URL", ""),
            ],
        );
        assert_eq!(set["GW_MEDIA_URL"], "http://media.internal:60081");
        assert_eq!(set["GW_ONLINE_URL"], "");
        // Another, padded or absent mode leaves discovery to the child.
        for parent in [
            &[("MODE", "configured")][..],
            &[("MODE", " static")][..],
            &[][..],
        ] {
            let other = child(&manifest, parent);
            assert!(!other.contains_key("GW_MEDIA_URL"), "{parent:?}");
            assert!(!other.contains_key("GW_ONLINE_URL"), "{parent:?}");
        }
        // Literals still override whatever the parent passes.
        let literal = child(&manifest, &[("MODE", "static"), ("GW_POLICY", "lenient")]);
        assert_eq!(literal["GW_POLICY"], "strict");
    }

    #[test]
    fn environment_defaults_must_stay_overridable_and_tied_to_the_childs_mode() {
        let rejected = [
            (
                STATIC_DEFAULTS.replace("when_env = \"MODE\"", "when_env = \"SECRET_MODE\""),
                "not inherited by the child: SECRET_MODE",
            ),
            (
                STATIC_DEFAULTS.replace("GW_MEDIA_URL", "MEDIA_URL"),
                "MEDIA_URL is not inherited by the child",
            ),
            (
                STATIC_DEFAULTS.replace("GW_MEDIA_URL", "GW_POLICY"),
                "GW_POLICY both as a literal and as an environment default",
            ),
            (
                STATIC_DEFAULTS.replace("\"http://127.0.0.1:60081\"", "\"\""),
                "GW_MEDIA_URL is empty or padded",
            ),
            (
                STATIC_DEFAULTS.replace("equals = \"static\"", "equals = \" static\""),
                "empty or padded mode value",
            ),
            (
                STATIC_DEFAULTS.replace("GW_ONLINE_URL", "MODE"),
                "sets a mode its defaults depend on: MODE",
            ),
            (
                format!("{STATIC_DEFAULTS}{}", STATIC_DEFAULTS.replace("GW_ONLINE_URL", "GW_OTHER_URL")),
                "duplicate environment default: GW_MEDIA_URL",
            ),
            (
                "[[services.environment_defaults]]\nwhen_env = \"MODE\"\nequals = \"static\"\nvalues = {}\n"
                    .to_string(),
                "are empty",
            ),
        ];
        for (extra, reason) in rejected {
            let error = defaults_fixture(&extra).unwrap_err();
            assert!(error.contains(reason), "{reason}: {error}");
        }
        // The mode itself cannot be pinned by a literal, so the child sees what was checked.
        let pinned = defaults_fixture(&STATIC_DEFAULTS.replace(
            "[[services.environment_defaults]]",
            "MODE = \"static\"\n[[services.environment_defaults]]",
        ))
        .unwrap_err();
        assert!(pinned.contains("overrides the mode"), "{pinned}");
        let unknown = defaults_fixture(&STATIC_DEFAULTS.replace("equals", "matches")).unwrap_err();
        assert!(unknown.contains("unknown field"), "{unknown}");
    }

    #[test]
    fn core_gateways_name_their_in_container_downstreams_in_static_mode() {
        let manifest: Manifest =
            toml::from_str(include_str!("../../../release/supervise-core.toml")).unwrap();
        manifest.validate().unwrap();
        // Each downstream variable suffix and the manifest service that listens for it.
        let downstreams = [
            ("MEDIA", "media"),
            ("MESSAGE_INGEST", "message-ingest"),
            ("MESSAGE_ORCHESTRATOR", "orchestrator"),
            ("CONVERSATION", "conversation"),
            ("ONLINE", "signaling-online"),
            ("STORAGE_READER", "storage-reader"),
            ("CAPABILITY", "capability"),
        ];
        let listener = |name: &str| {
            let service = manifest.services.iter().find(|s| s.name == name).unwrap();
            format!("http://{}", service.tcp_health.as_deref().unwrap())
        };
        for (gateway, prefix, count) in [
            ("api-gateway", "FLARE_API_GATEWAY", 6),
            ("admin-gateway", "FLARE_ADMIN_GATEWAY", 7),
        ] {
            let service = manifest
                .services
                .iter()
                .find(|s| s.name == gateway)
                .unwrap();
            let parent = |mode: &str| {
                manifest
                    .child_environment(
                        service,
                        [(OsString::from("FLARE_REGISTRY_MODE"), OsString::from(mode))],
                    )
                    .into_iter()
                    .map(|(key, value)| (key.into_string().unwrap(), value.into_string().unwrap()))
                    .filter(|(key, _)| key.ends_with("_SERVICE_URL"))
                    .collect::<BTreeMap<_, _>>()
            };
            let expected: BTreeMap<_, _> = downstreams
                .iter()
                .take(count)
                .map(|(suffix, listener_service)| {
                    (
                        format!("{prefix}_GRPC_{suffix}_SERVICE_URL"),
                        listener(listener_service),
                    )
                })
                .collect();
            assert_eq!(parent("static"), expected, "{gateway}");
            // Configured mode keeps discovery.
            assert!(parent("configured").is_empty(), "{gateway}");
        }
    }

    #[test]
    fn conversation_reaches_the_in_container_storage_reader_in_static_mode() {
        let manifest: Manifest =
            toml::from_str(include_str!("../../../release/supervise-core.toml")).unwrap();
        manifest.validate().unwrap();
        let service = |name: &str| manifest.services.iter().find(|s| s.name == name).unwrap();
        let reader = format!(
            "http://{}",
            service("storage-reader").tcp_health.as_deref().unwrap()
        );
        let url = |parent: Vec<(OsString, OsString)>| {
            manifest
                .child_environment(service("conversation"), parent)
                .into_iter()
                .find(|(key, _)| key == "CONVERSATION_STORAGE_READER_SERVICE_URL")
                .map(|(_, value)| value.into_string().unwrap())
        };
        let mode = |value: &str| (OsString::from("FLARE_REGISTRY_MODE"), OsString::from(value));
        assert_eq!(url(vec![mode("static")]), Some(reader));
        // Configured mode keeps discovery; a value the parent sets still wins.
        assert_eq!(url(vec![mode("configured")]), None);
        let mine = (
            OsString::from("CONVERSATION_STORAGE_READER_SERVICE_URL"),
            OsString::from("http://10.0.0.7:60083"),
        );
        assert_eq!(
            url(vec![mode("static"), mine]),
            Some("http://10.0.0.7:60083".to_string())
        );
    }
}
