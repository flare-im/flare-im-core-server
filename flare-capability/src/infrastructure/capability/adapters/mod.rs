//! 能力扩展可选适配器（Guard / Resolver / RTC 远端实现等）。

pub mod unwired;

pub use unwired::{
    UnwiredFriendshipGuard, UnwiredMuteGuard, UnwiredRecipientResolver, UnwiredRtcCapability,
};
