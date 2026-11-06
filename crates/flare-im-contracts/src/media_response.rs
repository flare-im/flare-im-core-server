//! Bounded HTTP response options shared by object-store signing and local media delivery.
use std::collections::HashMap;

use flare_server_core::error::{ErrorCode, Result};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MediaResponseHeaders {
    pub content_disposition: Option<String>,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
}

impl MediaResponseHeaders {
    /// Only these two response overrides are client configurable. A download's
    /// Content-Disposition is always constructed from authoritative metadata.
    pub fn from_request(headers: &HashMap<String, String>) -> Result<Self> {
        let mut result = Self::default();
        for (name, value) in headers {
            if value.is_empty()
                || value.len() > 1024
                || value.bytes().any(|byte| !(0x20..=0x7e).contains(&byte))
            {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "invalid media response header value"
                ));
            }
            if name.eq_ignore_ascii_case("cache-control")
                && !matches!(value.as_str(), "no-store" | "private, no-store")
            {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "media cache override must remain private and uncacheable"
                ));
            }
            let slot = match name.to_ascii_lowercase().as_str() {
                "content-type" => &mut result.content_type,
                "cache-control" => &mut result.cache_control,
                _ => {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::InvalidParameter,
                        "unsupported media response header"
                    ));
                }
            };
            if slot.replace(value.clone()).is_some() {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "duplicate media response header"
                ));
            }
        }
        Ok(result)
    }

    /// A response override may not turn stored bytes into another executable
    /// content type. MIME remains the server's authoritative upload metadata.
    pub fn validate_content_type(&self, authoritative: &str) -> Result<()> {
        if self
            .content_type
            .as_ref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(authoritative))
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "media content type override differs from stored metadata"
            ));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.content_disposition.is_none()
            && self.content_type.is_none()
            && self.cache_control.is_none()
    }

    pub fn with_download(mut self, file_name: &str) -> Result<Self> {
        self.content_disposition = Some(attachment_content_disposition(file_name)?);
        Ok(self)
    }
}

/// RFC 6266 ASCII fallback plus RFC 5987 UTF-8 filename. Paths and controls
/// never become response-header syntax or filesystem paths on the recipient.
pub fn attachment_content_disposition(file_name: &str) -> Result<String> {
    if file_name.len() > 1024 || file_name.chars().any(char::is_control) {
        return Err(flare_server_core::flare_err!(
            ErrorCode::InvalidParameter,
            "invalid media download filename"
        ));
    }
    let name = file_name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim();
    let name = if name.is_empty() || matches!(name, "." | "..") {
        "download"
    } else {
        name
    };
    let fallback: String = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || " ._-()".contains(character) {
                character
            } else {
                '_'
            }
        })
        .collect();
    let mut encoded = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").expect("writing a String cannot fail");
        }
    }
    Ok(format!(
        "attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_filename_has_a_safe_fallback_and_exact_utf8_name() {
        assert_eq!(
            attachment_content_disposition("report.txt").unwrap(),
            "attachment; filename=\"report.txt\"; filename*=UTF-8''report.txt"
        );
        assert_eq!(
            attachment_content_disposition("../资料 \"137\".txt").unwrap(),
            "attachment; filename=\"__ _137_.txt\"; filename*=UTF-8''%E8%B5%84%E6%96%99%20%22137%22.txt"
        );
        assert_eq!(
            attachment_content_disposition("C:\\private\\photo.png").unwrap(),
            "attachment; filename=\"photo.png\"; filename*=UTF-8''photo.png"
        );
        for invalid in [
            "photo.png\r\nSet-Cookie: injected=1",
            "bad\0name",
            "tab\tname",
        ] {
            assert!(attachment_content_disposition(invalid).is_err());
        }
    }

    #[test]
    fn response_header_allowlist_rejects_injection_and_client_filenames() {
        let safe = MediaResponseHeaders::from_request(&HashMap::from([
            ("Content-Type".into(), "image/png".into()),
            ("cache-control".into(), "private, no-store".into()),
        ]))
        .unwrap();
        assert_eq!(safe.content_type.as_deref(), Some("image/png"));
        assert_eq!(safe.cache_control.as_deref(), Some("private, no-store"));
        assert!(safe.validate_content_type("image/png").is_ok());
        assert!(safe.validate_content_type("text/plain").is_err());
        for (name, value) in [
            ("Set-Cookie", "x=1"),
            ("Content-Disposition", "attachment; filename=forged"),
            ("Content-Type", "image/png\r\nX-Evil: 1"),
            ("Cache-Control", "public, max-age=31536000"),
        ] {
            assert!(
                MediaResponseHeaders::from_request(&HashMap::from([(name.into(), value.into())]))
                    .is_err()
            );
        }
        assert!(
            MediaResponseHeaders::from_request(&HashMap::from([
                ("Content-Type".into(), "image/png".into()),
                ("content-type".into(), "text/html".into()),
            ]))
            .is_err()
        );
    }
}
