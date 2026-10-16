//! Push binding fence keys. The authority is singleton Redis/Dragonfly, not Redis Cluster.
use sha2::{Digest, Sha256};
pub const PUSH_BINDING_MAX_COMMAND_MS: i64 = 600_000;
pub const PUSH_BINDING_TOMBSTONE_SECS: i64 = 86_400;
pub fn binding_id_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn binding_hash(values: &[&str]) -> String {
    let mut hash = Sha256::new();
    for value in values {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
pub fn binding_id(secret: &str) -> Option<String> {
    binding_id_valid(secret).then(|| format!("{:x}", Sha256::digest(secret.as_bytes())))
}
pub fn push_binding_slot(prefix: &str, app: &str, provider: &str, device: &str) -> String {
    format!(
        "{}:binding-slot:{}",
        prefix.trim_end_matches(':'),
        binding_hash(&[app, provider, device])
    )
}
pub fn push_binding_command(prefix: &str, id: &str) -> String {
    format!("{}:binding-command:{id}", prefix.trim_end_matches(':'))
}
