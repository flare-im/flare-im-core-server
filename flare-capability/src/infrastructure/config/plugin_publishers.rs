//! Publisher keys the registry trusts (`FLARE_PLUGIN_PUBLISHER_KEYS`), read once at startup: a JSON
//! array of `{"key_id","publisher_id","public_key_base64"}`. Unset is an empty store, which refuses
//! every package.
use base64::{Engine, engine::general_purpose::STANDARD};
use flare_im_core_capability_core::plugin::signature::PublisherTrustStore;
use flare_server_core::error::{FlareError, Result};
use serde::Deserialize;

const ENV: &str = "FLARE_PLUGIN_PUBLISHER_KEYS";
const MAX_RAW_BYTES: usize = 32768;
const MAX_KEYS: usize = 128;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublisherKey {
    key_id: String,
    publisher_id: String,
    public_key_base64: String,
}

fn invalid() -> FlareError {
    FlareError::system("invalid FLARE_PLUGIN_PUBLISHER_KEYS")
}

fn parse(raw: &str) -> Result<PublisherTrustStore> {
    if raw.len() > MAX_RAW_BYTES {
        return Err(invalid());
    }
    let keys: Vec<PublisherKey> = serde_json::from_str(raw).map_err(|_| invalid())?;
    if keys.len() > MAX_KEYS {
        return Err(invalid());
    }
    let mut trust = PublisherTrustStore::default();
    for key in keys {
        let bytes: [u8; 32] = STANDARD
            .decode(&key.public_key_base64)
            .map_err(|_| invalid())?
            .try_into()
            .map_err(|_| invalid())?;
        trust
            .insert(key.key_id, key.publisher_id, bytes)
            .map_err(|_| invalid())?;
    }
    Ok(trust)
}

pub fn publisher_trust_from_env() -> Result<PublisherTrustStore> {
    match std::env::var(ENV) {
        Ok(raw) => parse(&raw),
        Err(std::env::VarError::NotPresent) => Ok(PublisherTrustStore::default()),
        Err(_) => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_what_is_not_a_key_list() {
        assert!(parse("[]").is_ok());
        assert!(parse("{}").is_err());
        assert!(
            parse(r#"[{"key_id":"k","publisher_id":"p","public_key_base64":"short"}]"#).is_err()
        );
    }
}
