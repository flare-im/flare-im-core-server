//! Conversation sequence allocation for Flare IM write paths.

mod floor_provider;
mod sequence_allocator;

pub use sequence_allocator::SequenceAllocator;

pub use floor_provider::SeqFloorProvider;
