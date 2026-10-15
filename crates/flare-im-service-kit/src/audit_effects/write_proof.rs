//! Durable provenance, not an authorization token. Minted only after verified machine
//! admission and bound to the finalized queue message. No expiry: delayed queue delivery
//! must remain auditable. The message ledger is the replay/idempotency boundary.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use ed25519_dalek::{
    Signature, Signer, SigningKey, Verifier, VerifyingKey,
    pkcs8::{DecodePrivateKey, DecodePublicKey},
};
use flare_proto::common::{Message, message_content::Content};
use flare_server_core::{VerifiedContext, context::Ctx};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

pub const HEADER: &str = "x-flare-write-proof";
pub const PRIVATE_ENV: &str = "FLARE_WRITE_PROOF_PRIVATE_KEY";
pub const PUBLIC_ENV: &str = "FLARE_WRITE_PROOF_PUBLIC_KEY";
const DOMAIN: &[u8] = b"flare.writer.machine.commit.v1\0";
type Result<T> = std::result::Result<T, &'static str>;
pub struct ProofSigner(SigningKey);
pub struct ProofVerifier(VerifyingKey);
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    app: String,
    request_id: String,
    actor: String,
    key: String,
    scope: String,
    message_id: String,
    conversation: String,
    digest: String,
}
/// Private fields: unsigned headers and message attributes cannot construct this marker.
#[derive(Clone)]
pub struct VerifiedWrite(Receipt);
#[derive(Clone)]
pub struct RecoveredWrite {
    verified: VerifiedWrite,
    token: String,
}

fn digest(message: &Message) -> Result<String> {
    use prost::Message as _;
    // This proof accepts SystemContent only. All maps in that concrete protobuf
    // shape are encoded separately in sorted order, never HashMap wire order.
    let mut message = message.clone();
    let attributes = std::mem::take(&mut message.attributes)
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let extensions = std::mem::take(&mut message.extensions)
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let retention_attributes = message.retention_policy.as_mut().map(|p| {
        std::mem::take(&mut p.attributes)
            .into_iter()
            .collect::<BTreeMap<_, _>>()
    });
    let Some(Content::System(content)) = message.content.as_mut().and_then(|c| c.content.as_mut())
    else {
        return Err("system write proof required");
    };
    let system_attributes = std::mem::take(&mut content.attributes)
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let bytes = serde_json::to_vec(&(
        B64.encode(message.encode_to_vec()),
        attributes,
        extensions,
        system_attributes,
        retention_attributes,
    ))
    .map_err(|_| "invalid proof message")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}
fn env_value(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(s) if !s.trim().is_empty() => Ok(Some(s)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        _ => Err("invalid write proof key configuration"),
    }
}
impl ProofSigner {
    pub fn from_pem(pem: &str) -> Result<Self> {
        SigningKey::from_pkcs8_pem(pem)
            .map(Self)
            .map_err(|_| "invalid write proof private key")
    }
    pub fn from_env() -> Result<Option<Self>> {
        env_value(PRIVATE_ENV)?
            .map(|p| Self::from_pem(&p))
            .transpose()
    }
    pub fn stamp(&self, ctx: &Ctx, message: &Message) -> Result<String> {
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .filter(|c| crate::platform_send::is_platform_system_sender(c))
            .ok_or("verified machine required for write proof")?;
        if !requires_proof(message)
            || message.sender_id != claims.actor_id
            || message.server_id.is_empty()
        {
            return Err("write proof message mismatch");
        }
        let receipt = Receipt {
            app: claims.tenant_id.clone(),
            request_id: claims.request_id.clone(),
            actor: claims.actor_id.clone(),
            key: claims.credential_id.clone().ok_or("machine key missing")?,
            scope: crate::platform_send::SYSTEM_MESSAGE_SCOPE.into(),
            message_id: message.server_id.clone(),
            conversation: message.conversation_id.clone(),
            digest: digest(message)?,
        };
        let body = serde_json::to_vec(&receipt).map_err(|_| "write proof serialization failed")?;
        let signed = [DOMAIN, body.as_slice()].concat();
        Ok(format!(
            "{}.{}",
            B64.encode(&body),
            B64.encode(self.0.sign(&signed).to_bytes())
        ))
    }
}
pub fn requires_proof(message: &Message) -> bool {
    matches!(message.content.as_ref().and_then(|c|c.content.as_ref()),Some(Content::System(s)) if s.event_kind.starts_with("platform."))
}
impl ProofVerifier {
    pub fn recover(&self, token: &str, app: &str, message: &Message) -> Result<RecoveredWrite> {
        Ok(RecoveredWrite {
            verified: self.verify(token, app, message)?,
            token: token.into(),
        })
    }
    pub fn from_pem(pem: &str) -> Result<Self> {
        VerifyingKey::from_public_key_pem(pem)
            .map(Self)
            .map_err(|_| "invalid write proof public key")
    }
    pub fn from_env() -> Result<Option<Self>> {
        env_value(PUBLIC_ENV)?
            .map(|p| Self::from_pem(&p))
            .transpose()
    }
    pub fn verify(&self, token: &str, app: &str, message: &Message) -> Result<VerifiedWrite> {
        if token.len() > 8192 {
            return Err("write proof too large");
        }
        let (body, signature) = token.split_once('.').ok_or("invalid write proof")?;
        let body = B64.decode(body).map_err(|_| "invalid write proof")?;
        let signature =
            Signature::from_slice(&B64.decode(signature).map_err(|_| "invalid write proof")?)
                .map_err(|_| "invalid write proof")?;
        self.0
            .verify(&[DOMAIN, body.as_slice()].concat(), &signature)
            .map_err(|_| "invalid write proof signature")?;
        let r: Receipt = serde_json::from_slice(&body).map_err(|_| "invalid write proof body")?;
        if r.app != app
            || app.is_empty()
            || app == "*"
            || r.actor != message.sender_id
            || r.message_id != message.server_id
            || r.conversation != message.conversation_id
            || r.digest != digest(message)?
            || r.scope != crate::platform_send::SYSTEM_MESSAGE_SCOPE
            || !requires_proof(message)
        {
            return Err("write proof binding mismatch");
        }
        Ok(VerifiedWrite(r))
    }
}
impl VerifiedWrite {
    pub fn group_key(&self) -> &str {
        &self.0.message_id
    }
    pub async fn record(
        &self,
        tx: &mut sqlx::PgConnection,
        app: &str,
        id: &str,
        conversation: &str,
    ) -> sqlx::Result<()> {
        let r = &self.0;
        if r.app != app || r.message_id != id || r.conversation != conversation {
            return Err(sqlx::Error::Protocol(
                "machine audit binding mismatch".into(),
            ));
        }
        let payload = serde_json::json!({"event":super::EFFECT_EVENT,"request_id":r.request_id,"at_ms":super::now_ms(),"scope":app,"source":"storage-writer","actor_id":r.actor,"actor_type":"service","permission":r.scope,"credential_id":r.key,"aggregate":"message","aggregate_id":id,"action":"archive_commit","outcome":"committed"});
        sqlx::query(
            "INSERT INTO core_audit_outbox(event_id,app_id,payload) VALUES($1::uuid,$2,$3::jsonb)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(app)
        .bind(payload)
        .execute(tx)
        .await?;
        Ok(())
    }
}
pub fn queue_headers(
    ctx: &Ctx,
    message: &Message,
    signer: Option<&ProofSigner>,
    mut headers: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    headers.remove(HEADER);
    if requires_proof(message) {
        if let Some(recovered) = ctx.get_data::<RecoveredWrite>() {
            let r = &recovered.verified.0;
            if ctx.tenant_id() != Some(r.app.as_str()) || r.digest != digest(message)? {
                return Err("recovered write binding mismatch");
            }
            headers.insert(HEADER.into(), recovered.token.clone());
            return Ok(headers);
        }
        headers.insert(
            HEADER.into(),
            signer
                .ok_or("machine write proof signer unavailable")?
                .stamp(ctx, message)?,
        );
    }
    Ok(headers)
}
