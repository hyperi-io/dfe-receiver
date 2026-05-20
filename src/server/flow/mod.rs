//! Generic UDP flow listener (NetFlow + sFlow auto-dispatched).
//!
//! More modules will be added in subsequent tasks.

pub mod config;
pub mod decoder;
pub mod dispatch;
pub mod envelope;
pub mod handler;
pub mod kernel_drops;
pub mod listener;
pub mod metrics;
pub mod rate_limit;
pub mod schema;

#[cfg(test)]
pub mod proptest_inputs;

pub use handler::FlowHandler;
