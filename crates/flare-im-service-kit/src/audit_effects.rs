//! Management effect events (v4 R3-2), the Core twin of `flare-social-common::audit`.
//!
//! The gateway admin partition publishes `flare.audit.request.v1` / `flare.audit.result.v1`
//! around every dispatch; the service that performed a management write publishes one
//! `flare.audit.effect.v1` after it succeeded, and the platform aggregates the three by
//! `request_id` into one audit record. Same stream and subject family as the Social side:
//! stream [`STREAM`], subject `flare.audit.v1.<app>`.
//!
//! Only effects of a Staff context are recorded: end users and services are not operators.
//! A failed publish is only logged, because the effect has already happened and the audit
//! event is asynchronous, not a precondition of the write. [`NATS_URL_ENV`] unset disables the
//! sink with one warning (local development); set but unreachable at startup is a startup error.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flare_server_core::context::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::mq::nats::NatsProducer;
use flare_server_core::mq::nats::config::{NatsProducerConfig, NatsStreamSpec};
use flare_server_core::mq::producer::Producer;
use flare_server_core::{SignedActorType, VerifiedContext};
use serde::Serialize;

pub const STREAM: &str = "flare_audit";
pub const SUBJECT_PREFIX: &str = "flare.audit.v1";
pub const EFFECT_EVENT: &str = "flare.audit.effect.v1";
pub const NATS_URL_ENV: &str = "FLARE_AUDIT_NATS_URL";

#[derive(Serialize)]
pub struct EffectEvent<'a> {
    pub event: &'static str,
    pub request_id: &'a str,
    pub at_ms: i64,
    /// The application (tenant) the effect happened in.
    pub scope: &'a str,
    /// The service that produced the effect.
    pub source: &'static str,
    pub actor_id: &'a str,
    pub actor_type: &'static str,
    pub permission: Option<&'a str>,
    pub aggregate: &'static str,
    pub aggregate_id: &'a str,
    pub action: &'static str,
    pub outcome: &'static str,
}

struct AuditNats {
    url: String,
}

impl NatsProducerConfig for AuditNats {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn timeout_ms(&self) -> u64 {
        2_000
    }
    fn retries(&self) -> u32 {
        2
    }
    fn retry_backoff_ms(&self) -> u64 {
        100
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        vec![
            NatsStreamSpec::new(STREAM, vec![format!("{SUBJECT_PREFIX}.>")])
                .with_max_age(Duration::from_secs(30 * 24 * 3600))
                .with_duplicate_window(Duration::from_secs(10 * 60)),
        ]
    }
}

pub struct AuditEffects {
    source: &'static str,
    producer: Option<Arc<NatsProducer>>,
}

impl AuditEffects {
    /// Connects when [`NATS_URL_ENV`] is set; otherwise a disabled sink with a warning.
    pub async fn from_env(source: &'static str) -> Result<Self> {
        match std::env::var("FLARE_AUDIT_NATS_URL") {
            Ok(url) if !url.trim().is_empty() => Self::connect(source, url.trim()).await,
            Ok(_) | Err(std::env::VarError::NotPresent) => {
                tracing::warn!(
                    source,
                    "{NATS_URL_ENV} not set: management effects are not audited"
                );
                Ok(Self::disabled(source))
            }
            Err(_) => Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                format!("{NATS_URL_ENV} is not valid UTF-8"),
            )
            .build_error()),
        }
    }

    pub async fn connect(source: &'static str, url: &str) -> Result<Self> {
        let producer = NatsProducer::new(&AuditNats {
            url: url.to_owned(),
        })
        .await
        .map_err(|error| {
            ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                format!("audit stream unavailable: {error}"),
            )
            .build_error()
        })?;
        Ok(Self {
            source,
            producer: Some(Arc::new(producer)),
        })
    }

    pub fn disabled(source: &'static str) -> Self {
        Self {
            source,
            producer: None,
        }
    }

    pub fn enabled(&self) -> bool {
        self.producer.is_some()
    }

    /// Publishes the effect of a successful management write when the verified actor is
    /// Staff. The request id, application and permission come from the signed claims; the
    /// message id `{request_id}:effect:{source}:{aggregate_id}` deduplicates a retried publish.
    pub async fn record(
        &self,
        ctx: &Ctx,
        aggregate: &'static str,
        aggregate_id: &str,
        action: &'static str,
    ) {
        let Some(producer) = &self.producer else {
            return;
        };
        let Some(claims) = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
        else {
            return;
        };
        if claims.actor_type != SignedActorType::Staff {
            return;
        }
        let scope = if claims.tenant_id.is_empty() {
            "platform"
        } else {
            claims.tenant_id.as_str()
        };
        let event = EffectEvent {
            event: EFFECT_EVENT,
            request_id: &claims.request_id,
            at_ms: now_ms(),
            scope,
            source: self.source,
            actor_id: &claims.actor_id,
            actor_type: "staff",
            permission: claims.permission.as_deref(),
            aggregate,
            aggregate_id,
            action,
            outcome: "ok",
        };
        let Ok(payload) = serde_json::to_vec(&event) else {
            return;
        };
        let subject = format!("{SUBJECT_PREFIX}.{scope}");
        let mut headers = HashMap::new();
        headers.insert(
            "x-message-id".to_owned(),
            format!(
                "{}:effect:{}:{aggregate_id}",
                claims.request_id, self.source
            ),
        );
        if let Err(error) = producer
            .send(
                ctx,
                &subject,
                Some(&claims.request_id),
                payload,
                Some(headers),
            )
            .await
        {
            tracing::error!(error = %error, subject, aggregate, action, "audit effect publish failed");
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_effect_event_has_the_social_shape() {
        let event = EffectEvent {
            event: EFFECT_EVENT,
            request_id: "req-1",
            at_ms: 1,
            scope: "tenant-a",
            source: "admin-gateway",
            actor_id: "employee-1",
            actor_type: "staff",
            permission: Some("core:users:kick"),
            aggregate: "user_session",
            aggregate_id: "u1",
            action: "kick",
            outcome: "ok",
        };
        let json: serde_json::Value = serde_json::to_value(&event).unwrap();
        let mut keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "action",
                "actor_id",
                "actor_type",
                "aggregate",
                "aggregate_id",
                "at_ms",
                "event",
                "outcome",
                "permission",
                "request_id",
                "scope",
                "source",
            ]
        );
        assert_eq!(json["event"], "flare.audit.effect.v1");
        assert_eq!(
            AuditNats { url: String::new() }.stream_specs()[0].name,
            STREAM
        );
        assert!(!AuditEffects::disabled("admin-gateway").enabled());
    }
}
