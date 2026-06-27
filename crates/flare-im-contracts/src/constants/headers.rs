//! Internal MQ headers shared across IM Core services.
//!
//! These headers are routing hints between services. Stable client-visible
//! delivery semantics must still be expressed in protobuf typed fields.

pub const HEADER_DELIVERY_MODE: &str = "x-flare-delivery-mode";
pub const HEADER_INLINE_EVENTS_TRUNCATED: &str = "x-flare-inline-events-truncated";
pub const DELIVERY_MODE_PING: &str = "ping";
pub const DELIVERY_MODE_PING_WITH_INLINE: &str = "ping_with_inline";
