//! MCP tool registry, `tools/list`, and `tools/call` dispatch.

mod args;
mod descriptors;
mod dispatch;
mod handlers;

pub use descriptors::{INSTRUCTIONS, list};
pub use dispatch::call;
