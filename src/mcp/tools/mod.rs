//! MCP tool registry, `tools/list`, and `tools/call` dispatch.

mod args;
mod dispatch;
mod handlers;
mod registry;

pub use dispatch::call;
pub use registry::{INSTRUCTIONS, list};
