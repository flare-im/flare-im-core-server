//! 能力目录读模型（与 `ListCapabilities` / 静态注册表对齐）。

use crate::domain::capability::CapabilityDescriptor;

pub fn list_registered_capabilities() -> Vec<CapabilityDescriptor> {
    // AV/媒体后端能力由插件私有协定驱动，不在核心能力目录对外暴露。
    Vec::new()
}
