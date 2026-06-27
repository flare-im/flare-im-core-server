//! Conversation sequence allocation for Flare IM write paths.

mod leased_allocator;
mod sequence_allocator;

pub use leased_allocator::{
    LeaseOutcome, LeasedSegmentAllocator, MonotonicClock, RedisSeqLeaseBackend, SeqAllocation,
    SeqLeaseBackend, SystemClock,
};
pub use sequence_allocator::SequenceAllocator;
