//! Service principals of the business integration that Core trusts with operator authority.
//!
//! A business system that runs on top of Core reaches it through one or more integration
//! services, each signing its calls as a `Service` actor (`x-flare-ctx`). Core names none of them:
//! the deployment lists them in `FLARE_TRUSTED_INTEGRATION_PRINCIPALS` (comma separated, default
//! empty). A listed principal may operate on any message of its tenant (recall, edit, delete for a
//! user), purge a conversation's history, revoke a user's identity sessions and send as the
//! platform. Empty: no service has that authority; `System` actors keep theirs.

use std::sync::Arc;

pub const TRUSTED_INTEGRATION_PRINCIPALS_ENV: &str = "FLARE_TRUSTED_INTEGRATION_PRINCIPALS";

/// The trusted integration principals, read once at wiring and shared by clone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrustedIntegrationPrincipals(Arc<[String]>);

impl TrustedIntegrationPrincipals {
    /// Parse a comma separated list; blanks are ignored.
    pub fn parse(list: &str) -> Self {
        Self(comma_list(list).into())
    }

    /// From `FLARE_TRUSTED_INTEGRATION_PRINCIPALS`; unset means none.
    pub fn from_env() -> Self {
        Self::parse(&std::env::var(TRUSTED_INTEGRATION_PRINCIPALS_ENV).unwrap_or_default())
    }

    /// Whether `principal` (a signed `Service` actor id) is trusted.
    pub fn contains(&self, principal: &str) -> bool {
        !principal.is_empty() && self.0.iter().any(|p| p == principal)
    }
}

/// The trimmed, non-empty items of a comma separated list.
pub fn comma_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listed_principals_are_trusted_and_nothing_else() {
        let trusted = TrustedIntegrationPrincipals::parse(" svc-a, ,svc-b ");
        assert!(trusted.contains("svc-a"));
        assert!(trusted.contains("svc-b"));
        assert!(!trusted.contains("svc-c"));
        assert!(!trusted.contains(""));
        assert!(!TrustedIntegrationPrincipals::default().contains("svc-a"));
    }
}
