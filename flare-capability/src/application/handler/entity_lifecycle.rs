//! Delivery of the plugin entity lifecycle outbox (plugin platform v2 §3).
//!
//! A row says the card of a plugin entity went away (`recalled`, `post_failed`). The worker claims
//! due rows with a lease and, for each, calls the operation the card's contribution declares as
//! `entityLifecycle.unbound` with a platform system ticket ([`PluginSystemCallClaims`]: its own
//! signing domain, bound to the tenant grant, the operation, the entity and the reason; never a
//! user or staff ticket) and the input `{entity, reason, serverMsgId?}`. The request id is stable
//! per row (`lifecycle-<id>`), so a redelivery is the same command to the plugin.
//!
//! - A `post_failed` row whose entity has a binding after all (Storage Writer bound the card when
//!   it stored it, or a retried call posted it): `skipped`, nothing to roll back. This is checked
//!   before every `post_failed` delivery, confirmed or not; a row queued while the post's outcome
//!   was unknown is due only after the verification delay.
//! - The plugin answered (accepted, or refused with its own code): the row is `delivered`.
//! - The contribution declares no `unbound` operation (or the installation did not approve it):
//!   `skipped`.
//! - The plugin is not enabled, its projection is unavailable, the bindings cannot be read, the
//!   plugin host answered `Unavailable` (its projection is not current: no tenant state, another
//!   grant revision or configuration digest, the tenant stopped, the lease lapsed, a version
//!   switch, an unknown kid during key rotation; the callback did not run), the call timed out or
//!   failed in transport: retried with exponential backoff, and `abandoned` after [`RETENTION`].
//! - The plugin host refused the system ticket itself: `Unauthenticated` (it does not verify under
//!   the keys the deployment holds, it is malformed or expired, or the header is missing or
//!   repeated) or `PermissionDenied` (the ticket does not match the call: operation, request id,
//!   not the declared unbound operation, entity or reason differ from the input, tenant metadata,
//!   a user id). The callback never ran and a retry cannot change it: a Core signing defect. The
//!   row is `abandoned` with an error log, never recorded as `delivered`.
//!
//! The emergency switch (`ensure_not_suspended`) does not stop this worker: a stopped plugin
//! still gets `unbound` once it is enabled and reachable, so a red packet is refunded and a poll
//! cancelled instead of being held. A plugin that is disabled in the projection is retried until
//! it is enabled again or the row is abandoned.
use std::sync::Arc;
use std::time::Duration;

use flare_im_capability_core::plugin::invocation::{PluginCallSigner, PluginSystemCallClaims};
use flare_im_capability_core::plugin::manifest::{UNBOUND_REASON_POST_FAILED, UiContributionKind};
use serde_json::json;

use super::remote_dispatch::{PluginAnswer, PluginCall, call_plugin_answer};
use crate::domain::repository::PluginAuthorizationReader;
use crate::domain::repository::entity_lifecycle::{
    EntityLifecycleOutbox, LifecycleOutcome, LifecycleTask,
};
use crate::domain::repository::plugin_state::PluginEntityBindingStore;

/// A row not delivered within this window is abandoned (logged as an error).
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
/// Rows claimed per round.
const BATCH: u32 = 32;
/// How long a claimed row stays invisible to other workers: longer than a round of [`BATCH`]
/// calls at the plugin call timeout, so a slow round is not delivered twice.
const LEASE: Duration = Duration::from_secs(300);
/// Pause between rounds that found nothing to do.
const IDLE: Duration = Duration::from_secs(2);
/// First retry delay; doubles per attempt up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// What became of one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    Done(LifecycleOutcome, Option<String>),
    Retry(Duration, String),
}

pub fn backoff(attempts: i32) -> Duration {
    let exponent = attempts.clamp(1, 20) as u32 - 1;
    FIRST_BACKOFF
        .saturating_mul(2u32.saturating_pow(exponent))
        .min(MAX_BACKOFF)
}

pub struct EntityLifecycleWorker {
    outbox: Arc<dyn EntityLifecycleOutbox>,
    authorization: Arc<dyn PluginAuthorizationReader>,
    signer: Arc<PluginCallSigner>,
    call_timeout: Duration,
    /// Checked before a `post_failed` is delivered; without it such rows wait.
    bindings: Option<Arc<dyn PluginEntityBindingStore>>,
}

impl EntityLifecycleWorker {
    pub fn new(
        outbox: Arc<dyn EntityLifecycleOutbox>,
        authorization: Arc<dyn PluginAuthorizationReader>,
        signer: Arc<PluginCallSigner>,
        call_timeout: Duration,
    ) -> Self {
        Self {
            outbox,
            authorization,
            signer,
            call_timeout,
            bindings: None,
        }
    }

    /// The bindings a `post_failed` is checked against before it is delivered.
    pub fn with_bindings(mut self, bindings: Option<Arc<dyn PluginEntityBindingStore>>) -> Self {
        self.bindings = bindings;
        self
    }

    /// Deliver until `shutdown` resolves.
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let handled = self.deliver_due().await;
            if handled < BATCH as usize {
                tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(IDLE) => {}
                }
            }
        }
    }

    /// One round: claim due rows and end or reschedule each. Returns the rows claimed.
    pub async fn deliver_due(&self) -> usize {
        let tasks = match self.outbox.claim_due(BATCH, LEASE).await {
            Ok(tasks) => tasks,
            Err(error) => {
                tracing::warn!(%error, "plugin entity lifecycle outbox unavailable");
                return 0;
            }
        };
        let claimed = tasks.len();
        for task in tasks {
            let disposition = self.deliver(&task).await;
            let stored = match &disposition {
                Disposition::Done(outcome, note) => {
                    if *outcome == LifecycleOutcome::Abandoned {
                        tracing::error!(tenant_id = %task.tenant_id, plugin_id = %task.plugin_id, entity = %task.entity, reason = %task.reason, attempts = task.attempts, note = ?note, "plugin entity lifecycle notification abandoned");
                    }
                    self.outbox
                        .complete(task.id, *outcome, note.as_deref())
                        .await
                }
                Disposition::Retry(delay, error) => {
                    tracing::debug!(tenant_id = %task.tenant_id, plugin_id = %task.plugin_id, entity = %task.entity, attempts = task.attempts, %error, "plugin entity lifecycle notification retried later");
                    self.outbox.retry_after(task.id, *delay, error).await
                }
            };
            if let Err(error) = stored {
                tracing::warn!(%error, id = task.id, "plugin entity lifecycle row not updated; its lease hands it back");
            }
        }
        claimed
    }

    /// Deliver one row (see the module docs).
    pub async fn deliver(&self, task: &LifecycleTask) -> Disposition {
        let now = chrono::Utc::now().timestamp_millis();
        let retry = |error: &str| {
            if now.saturating_sub(task.created_at_ms) >= RETENTION.as_millis() as i64 {
                Disposition::Done(LifecycleOutcome::Abandoned, Some(error.to_owned()))
            } else {
                Disposition::Retry(backoff(task.attempts), error.to_owned())
            }
        };
        if task.reason == UNBOUND_REASON_POST_FAILED {
            let Some(bindings) = &self.bindings else {
                return retry("plugin entity bindings are not configured");
            };
            match bindings
                .bindings(&task.tenant_id, &task.plugin_id, &task.entity)
                .await
            {
                Ok(rows) if !rows.is_empty() => {
                    return Disposition::Done(
                        LifecycleOutcome::Skipped,
                        Some("the card was stored after all".into()),
                    );
                }
                Ok(_) => {}
                Err(_) => return retry("plugin entity bindings unavailable"),
            }
        }
        let snapshot = match self.authorization.plugin_snapshot(&task.tenant_id).await {
            Ok(Some(snapshot)) if snapshot.validate(now).is_ok() => snapshot,
            Ok(_) => return retry("no valid plugin projection for the tenant"),
            Err(_) => return retry("plugin projection unavailable"),
        };
        let Some(projected) = snapshot.plugins.iter().find(|p| {
            p.grant.plugin_id == task.plugin_id
                && p.grant.enabled
                && !p.grant.revoked
                && now >= p.grant.valid_from_ms
                && now < p.grant.valid_until_ms
        }) else {
            return retry("plugin not enabled for the tenant");
        };
        let Ok(manifest) = projected.manifest() else {
            return retry("projected plugin manifest unreadable");
        };
        let Some(operation) = manifest
            .client_extensions
            .contributions
            .iter()
            .find(|c| c.kind == UiContributionKind::MessageContent && c.id == task.contribution_id)
            .and_then(|c| c.entity_lifecycle.as_ref())
            .map(|lifecycle| lifecycle.unbound.clone())
        else {
            return Disposition::Done(
                LifecycleOutcome::Skipped,
                Some("the contribution declares no entityLifecycle.unbound".into()),
            );
        };
        let Some(declared) = manifest.operation(&operation) else {
            return Disposition::Done(
                LifecycleOutcome::Skipped,
                Some("the unbound operation is not declared".into()),
            );
        };
        if !projected.grant.approved_operations.contains(&operation) {
            return Disposition::Done(
                LifecycleOutcome::Skipped,
                Some("the installation did not approve the unbound operation".into()),
            );
        }
        let timeout = self
            .call_timeout
            .min(Duration::from_millis(u64::from(declared.timeout_ms)));
        let request_id = format!("lifecycle-{}", task.id);
        let claims = PluginSystemCallClaims {
            tenant_id: task.tenant_id.clone(),
            plugin_id: projected.grant.plugin_id.clone(),
            plugin_version: projected.grant.plugin_version.clone(),
            manifest_digest: projected.grant.manifest_digest.clone(),
            deployment_id: projected.deployment.deployment_id.clone(),
            grant_id: projected.grant.grant_id.clone(),
            grant_revision: projected.grant.revision,
            configuration_digest: projected.configuration_digest.clone(),
            operation: operation.clone(),
            entity: task.entity.clone(),
            reason: task.reason.clone(),
            request_id: request_id.clone(),
            issued_at_ms: now,
            expires_at_ms: now
                .saturating_add(timeout.as_millis().min(120_000) as i64)
                .min(projected.grant.valid_until_ms),
        };
        let ticket = match self.signer.sign_system(&claims, now) {
            Ok(ticket) => ticket,
            Err(_) => {
                return Disposition::Done(
                    LifecycleOutcome::Abandoned,
                    Some("system ticket refused by the protocol".into()),
                );
            }
        };
        let mut input = json!({ "entity": task.entity, "reason": task.reason });
        if let Some(server_msg_id) = &task.server_msg_id {
            input["serverMsgId"] = json!(server_msg_id);
        }
        let outcome = tokio::time::timeout(
            timeout,
            call_plugin_answer(
                PluginCall {
                    grpc_authority: &projected.deployment.grpc_authority,
                    plugin_id: &projected.grant.plugin_id,
                    operation: &operation,
                    tenant_id: &task.tenant_id,
                    user_id: None,
                    request_id: &request_id,
                    trace_id: "",
                    payload: &input,
                    ticket: &ticket,
                },
                timeout,
            ),
        )
        .await;
        match outcome {
            Ok(Ok(answer @ PluginAnswer::Refused { code, .. }))
                if answer.is_credential_refusal() =>
            {
                tracing::error!(
                    tenant_id = %task.tenant_id,
                    plugin_id = %task.plugin_id,
                    entity = %task.entity,
                    reason = %task.reason,
                    code = ?code,
                    "plugin host refused the lifecycle system ticket; the callback did not run (Core signing defect)"
                );
                Disposition::Done(
                    LifecycleOutcome::Abandoned,
                    Some(format!(
                        "plugin host refused the system ticket ({code:?}); the callback did not run"
                    )),
                )
            }
            Ok(Ok(PluginAnswer::Answered(result))) if result.success => {
                Disposition::Done(LifecycleOutcome::Delivered, None)
            }
            Ok(Ok(PluginAnswer::Answered(result))) => Disposition::Done(
                LifecycleOutcome::Delivered,
                Some(format!(
                    "plugin answered {}",
                    result
                        .data
                        .get("errorCode")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("PLUGIN_REJECTED")
                )),
            ),
            Ok(Ok(PluginAnswer::Refused { code, reason })) => Disposition::Done(
                LifecycleOutcome::Delivered,
                Some(format!(
                    "plugin refused {code:?} {}",
                    super::remote_dispatch::plugin_error_code(&reason)
                )),
            ),
            Ok(Err(error)) => retry(&error.to_string()),
            Err(_) => retry("plugin lifecycle call deadline exceeded"),
        }
    }
}

#[cfg(test)]
#[path = "entity_lifecycle_tests.rs"]
mod tests;
