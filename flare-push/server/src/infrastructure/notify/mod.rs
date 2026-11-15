//! 防打扰闸门的存储适配：推送 Redis 里的闸门状态、它不可用时的进程内近似，
//! 以及 Online 写下的设备信号。

pub mod device_signals;
pub mod keys;
pub mod memory_gate_store;
pub mod redis_gate_store;

pub use device_signals::RedisDeviceSignals;
pub use keys::GateKeys;
pub use memory_gate_store::MemoryGateStore;
pub use redis_gate_store::RedisGateStore;
