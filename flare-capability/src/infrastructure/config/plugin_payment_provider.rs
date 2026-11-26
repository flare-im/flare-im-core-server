//! `plugin.paymentProvider`: the transient operation of the plugin that holds this deployment's
//! payment credentials (`FLARE_PLUGIN_PAYMENT_PROVIDER`, for example
//! `flare.wallet.payments.ticket`).
//!
//! A host's payment confirmation hands the credential it collects to this operation only, never to
//! the plugin that asked for the payment; the plugin gets a one-time ticket instead. Unset (the
//! default) means no deployment-named provider: no client can confirm a payment. The name is the
//! platform's, never a manifest's, so publishing a plugin cannot make it the provider.

use flare_im_core_capability_core::plugin::ui::{ClientPlugin, UiPaymentProvider};

pub const PLUGIN_PAYMENT_PROVIDER_ENV: &str = "FLARE_PLUGIN_PAYMENT_PROVIDER";

/// The configured provider operation, when it reads as an operation id.
pub fn plugin_payment_provider() -> Option<String> {
    let value = std::env::var(PLUGIN_PAYMENT_PROVIDER_ENV).ok()?;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if !valid_operation(value) {
        tracing::warn!(
            "{PLUGIN_PAYMENT_PROVIDER_ENV} is not an operation id: no payment provider is named"
        );
        return None;
    }
    Some(value.to_owned())
}

fn valid_operation(value: &str) -> bool {
    value.len() <= 128
        && value.contains('.')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

/// The provider a viewer's catalog names: the plugin of that catalog whose authorized transient
/// operations include `operation`. `None` when the viewer cannot reach it (not installed, disabled,
/// the operation not approved or stopped).
pub fn catalog_payment_provider(
    operation: Option<&str>,
    plugins: &[ClientPlugin],
) -> Option<UiPaymentProvider> {
    let operation = operation?;
    plugins
        .iter()
        .find(|plugin| {
            operation
                .strip_prefix(plugin.plugin_id.as_str())
                .is_some_and(|rest| rest.starts_with('.'))
                && plugin.transient_operations.contains(operation)
        })
        .map(|plugin| UiPaymentProvider {
            plugin_id: plugin.plugin_id.clone(),
            operation: operation.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn plugin(plugin_id: &str, operations: &[&str], transient: &[&str]) -> ClientPlugin {
        serde_json::from_value(json!({
            "pluginId": plugin_id, "version": "1.0.0",
            "manifestDigest": format!("sha256:{}", "a".repeat(64)), "grantRevision": 1,
            "extensions": {"platforms": ["web"], "contributions": []},
            "operations": operations, "transientOperations": transient,
        }))
        .unwrap()
    }

    #[test]
    fn the_provider_is_the_configured_transient_operation_of_a_reachable_plugin() {
        let plugins = vec![
            plugin(
                "example.shop",
                &["example.shop.pay", "example.pay.ticket"],
                &["example.shop.pay"],
            ),
            plugin(
                "example.pay",
                &["example.pay.ticket", "example.pay.order"],
                &["example.pay.ticket"],
            ),
        ];
        assert_eq!(
            catalog_payment_provider(Some("example.pay.ticket"), &plugins),
            Some(UiPaymentProvider {
                plugin_id: "example.pay".into(),
                operation: "example.pay.ticket".into(),
            })
        );
        // Not configured, not transient, not authorized for this viewer, or another plugin's.
        assert_eq!(catalog_payment_provider(None, &plugins), None);
        assert_eq!(
            catalog_payment_provider(Some("example.pay.order"), &plugins),
            None
        );
        assert_eq!(
            catalog_payment_provider(Some("example.pay.ticket"), &plugins[..1]),
            None,
            "a plugin cannot name itself the provider by listing the operation"
        );
        assert_eq!(
            catalog_payment_provider(Some("example.payx.ticket"), &plugins),
            None
        );
    }

    #[test]
    fn only_an_operation_id_names_a_provider() {
        assert!(valid_operation("flare.wallet.payments.ticket"));
        assert!(!valid_operation("wallet"));
        assert!(!valid_operation("flare.wallet payments"));
        assert!(!valid_operation(&format!("a.{}", "b".repeat(130))));
    }
}
