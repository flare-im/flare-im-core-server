//! Shared transport bounds; never return provider bodies, bearer tokens, or request URLs.
use flare_server_core::{ErrorCode, FlareError};
use std::time::Duration;

pub fn client() -> Result<reqwest::Client, FlareError> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()
        .map_err(|_| failure("push HTTP configuration rejected"))
}
pub fn failure(message: &'static str) -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, message)
}
/// `Retry-After` of a vendor answer (seconds; read before the body is consumed).
pub fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    crate::domain::vendor_delivery::parse_retry_after(
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
    )
}
pub async fn body(mut response: reqwest::Response) -> Result<String, FlareError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| failure("push response unavailable"))?
    {
        if bytes.len().saturating_add(chunk.len()) > 16 * 1024 {
            return Err(failure("push response exceeds limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|_| failure("push response is not UTF-8"))
}
