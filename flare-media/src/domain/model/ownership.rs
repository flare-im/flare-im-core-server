use serde::{Deserialize, Serialize};

/// Persisted ownership is independent from the actor that performed the upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaOwnerKind {
    #[default]
    User,
    Application,
}

impl MediaOwnerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Application => "application",
        }
    }
}

impl std::str::FromStr for MediaOwnerKind {
    type Err = ();
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "user" => Ok(Self::User),
            "application" => Ok(Self::Application),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaPurpose {
    #[default]
    General,
    Avatar,
    Image,
}

impl MediaPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::Avatar => "avatar",
            Self::Image => "image",
        }
    }

    pub fn is_application_image(self) -> bool {
        matches!(self, Self::Avatar | Self::Image)
    }
}

impl std::str::FromStr for MediaPurpose {
    type Err = ();
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "general" => Ok(Self::General),
            "avatar" => Ok(Self::Avatar),
            "image" => Ok(Self::Image),
            _ => Err(()),
        }
    }
}

/// Upload session access stays with the authenticated creator, even when the
/// completed asset belongs to its application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadActorKind {
    #[default]
    User,
    Staff,
}

#[derive(Debug, Clone)]
pub struct AdminImageUploadPolicy {
    pub max_bytes: u64,
    pub max_pixels: u64,
    pub max_dimension: u32,
    pub allowed_mime_types: Vec<String>,
}

impl Default for AdminImageUploadPolicy {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_pixels: 16_777_216,
            max_dimension: 8192,
            allowed_mime_types: ["image/png", "image/jpeg", "image/webp", "image/gif"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        }
    }
}

impl AdminImageUploadPolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_bytes == 0
            || self.max_bytes > i64::MAX as u64
            || self.max_pixels == 0
            || self.max_dimension == 0
            || self.allowed_mime_types.is_empty()
            || self.allowed_mime_types.iter().any(|mime| {
                !matches!(
                    mime.as_str(),
                    "image/png" | "image/jpeg" | "image/webp" | "image/gif"
                )
            })
        {
            return Err("invalid administrative image upload policy");
        }
        Ok(())
    }
}

/// Only server-validated user avatar bytes are written under this anonymous GET prefix.
pub const PUBLIC_AVATAR_OBJECT_PREFIX: &str = "avatars";
pub const PUBLIC_AVATAR_METADATA_KEY: &str = "public_avatar";
