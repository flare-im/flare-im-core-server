/// Returns the Redis sorted-set key that indexes pending WAL message IDs.
pub fn wal_pending_index_key(wal_hash_key: &str) -> String {
    format!("{wal_hash_key}:pending")
}

#[cfg(test)]
mod tests {
    use super::wal_pending_index_key;

    #[test]
    fn derives_pending_index_key_from_hash_key() {
        assert_eq!(
            wal_pending_index_key("flare:message:wal"),
            "flare:message:wal:pending"
        );
    }
}
