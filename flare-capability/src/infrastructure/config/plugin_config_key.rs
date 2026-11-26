//! Envelope encryption of plugin configuration secrets at rest: AES-256-GCM under a key from
//! `FLARE_PLUGIN_CONFIG_KEY`.
//!
//! The variable is `<kid>:<base64 of 32 bytes>`, comma separated when rotating: the first key seals,
//! all of them open. A value the manifest marks secret ([`PluginManifest::secret_configuration_paths`])
//! is stored as an envelope object in place, so the configuration stays one JSON document. The
//! authenticated data names the application, the binding and the place in the document: an envelope
//! copied to another binding or key does not open.
//!
//! Unset is an empty keyring. Storing a secret value then fails, and Capability refuses to start
//! while the database holds envelopes ([`PluginConfigCipher::check_sealed`]). Nothing here prints a
//! key or a value.
//!
//! [`PluginManifest::secret_configuration_paths`]: flare_im_core_capability_core::plugin::PluginManifest::secret_configuration_paths
use aes_gcm::aead::{Aead, OsRng, Payload};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit, Nonce};
use base64::{Engine, engine::general_purpose::STANDARD};
use flare_im_core_capability_core::plugin::configuration::SecretPath;
use flare_server_core::error::{FlareError, Result};
use serde_json::{Map, Value, json};

use crate::domain::plugin::{AppPluginState, PluginRegistryError, RegistryResult};

pub const PLUGIN_CONFIG_KEY_ENV: &str = "FLARE_PLUGIN_CONFIG_KEY";
/// Marks an envelope; the value is its format.
const ENVELOPE_MARK: &str = "$flareSecret";
const ENVELOPE_FORMAT: &str = "aes-256-gcm/v1";
const AAD_DOMAIN: &str = "flare.plugin.config.v1";
const MAX_KEYS: usize = 8;
const MAX_KID_BYTES: usize = 64;
#[cfg(test)]
#[path = "plugin_config_key_event_pg_tests.rs"]
mod event_pg_tests;

struct ConfigKey {
    kid: String,
    cipher: Aes256Gcm,
}

/// The keyring. The default holds no key.
#[derive(Default)]
pub struct PluginConfigCipher {
    keys: Vec<ConfigKey>,
}

impl std::fmt::Debug for PluginConfigCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginConfigCipher")
            .field("keys", &self.keys.len())
            .finish()
    }
}

fn invalid_key() -> FlareError {
    FlareError::system("invalid FLARE_PLUGIN_CONFIG_KEY")
}

fn is_envelope(value: &Value) -> bool {
    value.get(ENVELOPE_MARK).is_some()
}

fn context(state: &AppPluginState, pointer: &str) -> String {
    format!(
        "{AAD_DOMAIN}\n{}\n{}\n{pointer}",
        state.app_id, state.binding_id
    )
}

impl PluginConfigCipher {
    pub fn from_env() -> Result<Self> {
        match std::env::var(PLUGIN_CONFIG_KEY_ENV) {
            Ok(raw) if raw.trim().is_empty() => Ok(Self::default()),
            Ok(raw) => Self::parse(&raw),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(_) => Err(invalid_key()),
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        let mut keys: Vec<ConfigKey> = Vec::new();
        for entry in raw.split(',') {
            let (kid, material) = entry.trim().split_once(':').ok_or_else(invalid_key)?;
            let valid_kid = !kid.is_empty()
                && kid.len() <= MAX_KID_BYTES
                && kid
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'));
            if !valid_kid || keys.iter().any(|key| key.kid == kid) {
                return Err(invalid_key());
            }
            let material = STANDARD.decode(material).map_err(|_| invalid_key())?;
            if material.len() != 32 {
                return Err(invalid_key());
            }
            let cipher = Aes256Gcm::new_from_slice(&material).map_err(|_| invalid_key())?;
            keys.push(ConfigKey {
                kid: kid.to_owned(),
                cipher,
            });
        }
        if keys.len() > MAX_KEYS {
            return Err(invalid_key());
        }
        Ok(Self { keys })
    }

    /// A database holding envelopes needs a keyring: starting without one would only fail later,
    /// one plugin at a time.
    pub async fn check_sealed(&self, pool: &sqlx::PgPool) -> Result<()> {
        if !self.keys.is_empty() {
            return Ok(());
        }
        let sealed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM plugin_app_states \
             WHERE strpos(configuration::text,$1)>0 \
             OR strpos(COALESCE(runtime_configuration::text,''),$1)>0)",
        )
        .bind(ENVELOPE_MARK)
        .fetch_one(pool)
        .await
        .map_err(|_| FlareError::system("plugin configuration store unavailable"))?;
        if sealed {
            return Err(FlareError::system(
                "stored plugin configuration holds secrets: FLARE_PLUGIN_CONFIG_KEY is required",
            ));
        }
        Ok(())
    }

    /// Replaces the secret values of both configurations with envelopes.
    pub fn seal_state(
        &self,
        state: &mut AppPluginState,
        paths: &[SecretPath],
    ) -> RegistryResult<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut configuration = std::mem::take(&mut state.configuration);
        let mut acknowledged = state.runtime_configuration.take();
        for path in paths {
            self.seal_at(&mut configuration, path, String::new(), state)?;
            if let Some(acknowledged) = &mut acknowledged {
                self.seal_at(acknowledged, path, String::new(), state)?;
            }
        }
        state.configuration = configuration;
        state.runtime_configuration = acknowledged;
        Ok(())
    }

    fn seal_at(
        &self,
        value: &mut Value,
        path: &[String],
        pointer: String,
        state: &AppPluginState,
    ) -> RegistryResult<()> {
        match (value, path) {
            (Value::Array(items), _) => {
                for (index, item) in items.iter_mut().enumerate() {
                    self.seal_at(item, path, format!("{pointer}/{index}"), state)?;
                }
                Ok(())
            }
            (value, []) => {
                if value.is_null() || value.as_str() == Some("") || is_envelope(value) {
                    return Ok(());
                }
                *value = self.seal_value(value, &context(state, &pointer))?;
                Ok(())
            }
            (Value::Object(object), [key, rest @ ..]) => match object.get_mut(key) {
                Some(next) => self.seal_at(next, rest, format!("{pointer}/{key}"), state),
                None => Ok(()),
            },
            _ => Ok(()),
        }
    }

    fn seal_value(&self, value: &Value, context: &str) -> RegistryResult<Value> {
        let key = self.keys.first().ok_or_else(|| {
            PluginRegistryError::Invalid(
                "secret plugin configuration needs FLARE_PLUGIN_CONFIG_KEY".into(),
            )
        })?;
        let plain = serde_json::to_vec(value).map_err(|_| PluginRegistryError::Unavailable)?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let sealed = key
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plain,
                    aad: context.as_bytes(),
                },
            )
            .map_err(|_| PluginRegistryError::Unavailable)?;
        Ok(json!({
            ENVELOPE_MARK: ENVELOPE_FORMAT,
            "kid": key.kid,
            "nonce": STANDARD.encode(nonce),
            "ciphertext": STANDARD.encode(sealed),
        }))
    }

    /// Replaces every envelope of both configurations with its value. An envelope that does not
    /// open is `Unavailable`: a missing key, or a stored row that was tampered with.
    pub fn open_state(&self, state: &mut AppPluginState) -> RegistryResult<()> {
        let mut configuration = std::mem::take(&mut state.configuration);
        let mut acknowledged = state.runtime_configuration.take();
        let opened = self
            .open_at(&mut configuration, String::new(), state)
            .and_then(|()| match &mut acknowledged {
                Some(value) => self.open_at(value, String::new(), state),
                None => Ok(()),
            });
        state.configuration = configuration;
        state.runtime_configuration = acknowledged;
        opened
    }

    fn open_at(
        &self,
        value: &mut Value,
        pointer: String,
        state: &AppPluginState,
    ) -> RegistryResult<()> {
        if is_envelope(value) {
            let plain = value
                .as_object()
                .ok_or(PluginRegistryError::Unavailable)
                .and_then(|envelope| self.open_value(envelope, &context(state, &pointer)))?;
            *value = plain;
            return Ok(());
        }
        match value {
            Value::Array(items) => {
                for (index, item) in items.iter_mut().enumerate() {
                    self.open_at(item, format!("{pointer}/{index}"), state)?;
                }
            }
            Value::Object(object) => {
                for (key, item) in object.iter_mut() {
                    self.open_at(item, format!("{pointer}/{key}"), state)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn open_value(&self, envelope: &Map<String, Value>, context: &str) -> RegistryResult<Value> {
        let text = |name: &str| {
            envelope
                .get(name)
                .and_then(Value::as_str)
                .ok_or(PluginRegistryError::Unavailable)
        };
        if text(ENVELOPE_MARK)? != ENVELOPE_FORMAT {
            return Err(PluginRegistryError::Unavailable);
        }
        let kid = text("kid")?;
        let key = self
            .keys
            .iter()
            .find(|key| key.kid == kid)
            .ok_or(PluginRegistryError::Unavailable)?;
        let decode = |name: &str| {
            STANDARD
                .decode(text(name)?)
                .map_err(|_| PluginRegistryError::Unavailable)
        };
        let nonce: [u8; 12] = decode("nonce")?
            .try_into()
            .map_err(|_| PluginRegistryError::Unavailable)?;
        let plain = key
            .cipher
            .decrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: &decode("ciphertext")?,
                    aad: context.as_bytes(),
                },
            )
            .map_err(|_| PluginRegistryError::Unavailable)?;
        serde_json::from_slice(&plain).map_err(|_| PluginRegistryError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::plugin::DesiredAppState;

    fn cipher(raw: &str) -> PluginConfigCipher {
        PluginConfigCipher::parse(raw).unwrap()
    }

    fn key(kid: &str, byte: u8) -> String {
        format!("{kid}:{}", STANDARD.encode([byte; 32]))
    }

    fn state(configuration: Value, binding: &str) -> AppPluginState {
        DesiredAppState {
            app_id: "201".into(),
            binding_id: binding.into(),
            plugin_id: "p".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: "sha256:x".into(),
            revision: 1,
            enabled: false,
            configured: true,
            configuration,
            secrets_from: None,
        }
        .applied_to(None)
        .unwrap()
    }

    fn paths() -> Vec<SecretPath> {
        vec![
            vec!["apiKey".to_owned()],
            vec!["accounts".to_owned(), "token".to_owned()],
        ]
    }

    #[test]
    fn a_secret_is_stored_sealed_and_comes_back_whole() {
        let keys = cipher(&key("k1", 1));
        let clear = json!({"apiKey": "s3cret", "limit": 5, "accounts": [{"token": "t", "id": 1}]});
        let mut row = state(clear.clone(), "b1");
        row.runtime_configuration = Some(clear.clone());
        keys.seal_state(&mut row, &paths()).unwrap();
        let text =
            serde_json::to_string(&(&row.configuration, &row.runtime_configuration)).unwrap();
        assert!(!text.contains("s3cret"));
        assert_eq!(row.configuration["limit"], json!(5));
        assert_eq!(row.configuration["accounts"][0]["id"], json!(1));
        keys.open_state(&mut row).unwrap();
        assert_eq!(row.configuration, clear);
        assert_eq!(row.runtime_configuration, Some(clear));
    }

    #[test]
    fn an_envelope_opens_only_where_it_was_sealed() {
        let keys = cipher(&key("k1", 1));
        let mut row = state(json!({"apiKey": "s3cret"}), "b1");
        keys.seal_state(&mut row, &paths()).unwrap();
        let mut elsewhere = state(row.configuration.clone(), "b2");
        assert!(keys.open_state(&mut elsewhere).is_err());
        let mut moved = state(json!({"limit": row.configuration["apiKey"]}), "b1");
        assert!(keys.open_state(&mut moved).is_err());
    }

    #[test]
    fn a_secret_cannot_be_stored_without_a_key() {
        let keys = PluginConfigCipher::default();
        let mut row = state(json!({"apiKey": "s3cret"}), "b1");
        assert!(matches!(
            keys.seal_state(&mut row, &paths()),
            Err(PluginRegistryError::Invalid(_))
        ));
        // Nothing to protect needs no key.
        let mut empty = state(json!({"apiKey": "", "limit": 1}), "b1");
        keys.seal_state(&mut empty, &paths()).unwrap();
    }

    #[test]
    fn rotation_seals_with_the_first_key_and_opens_with_any() {
        let old = cipher(&key("k1", 1));
        let mut row = state(json!({"apiKey": "s3cret"}), "b1");
        old.seal_state(&mut row, &paths()).unwrap();
        let rotated = cipher(&format!("{},{}", key("k2", 2), key("k1", 1)));
        rotated.open_state(&mut row).unwrap();
        rotated.seal_state(&mut row, &paths()).unwrap();
        assert_eq!(row.configuration["apiKey"]["kid"], json!("k2"));
        assert!(old.open_state(&mut row).is_err());
    }

    #[test]
    fn a_key_must_be_named_and_of_the_right_length() {
        for raw in ["k1", ":AAAA", "k1:AAAA", "k 1:AAAA"] {
            assert!(PluginConfigCipher::parse(raw).is_err());
        }
        let twice = format!("{},{}", key("k1", 1), key("k1", 2));
        assert!(PluginConfigCipher::parse(&twice).is_err());
    }

    #[test]
    fn the_debug_form_shows_no_key_material() {
        let keys = cipher(&key("k1", 1));
        assert!(!format!("{keys:?}").contains("k1"));
    }
}
