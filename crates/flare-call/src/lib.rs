//! Business-neutral call lifecycle module.
//!
//! `flare-call` owns the call session aggregate and CQRS command handlers.
//! Transport gateways only route call signals, and RTC capability/plugin crates
//! only allocate or control media resources.

pub mod application;
pub mod domain;
