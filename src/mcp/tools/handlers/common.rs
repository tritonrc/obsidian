use serde_json::Value;

use crate::mcp::tools::dispatch::tool_err;
use crate::store::SharedState;

/// All service names reporting any signal (logs, metrics, traces), sorted unique.
pub(super) fn known_services(state: &SharedState) -> Vec<String> {
    use rustc_hash::FxHashSet;

    let mut set: FxHashSet<String> = FxHashSet::default();
    set.extend(state.log_store.read().get_label_values("service"));
    set.extend(state.metric_store.read().get_label_values("service"));
    set.extend(state.trace_store.read().service_names());
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort_unstable();
    v
}

/// Require a non-empty `service` that actually reports telemetry. On failure
/// returns a model-visible `isError` result that echoes the bad value and lists
/// valid services (per the §5 self-correction contract).
///
/// Presence and type are already settled by the tool's argument type; only what
/// the store knows is checked here.
pub(super) fn require_known_service(
    state: &SharedState,
    id: &Option<Value>,
    service: &str,
) -> Result<(), Value> {
    if service.is_empty() {
        return Err(tool_err(id.clone(), "`service` must not be empty".into()));
    }
    let known = known_services(state);
    if known.iter().any(|s| s == service) {
        Ok(())
    } else {
        let valid = if known.is_empty() {
            "none yet".to_string()
        } else {
            known.join(", ")
        };
        Err(tool_err(
            id.clone(),
            format!("unknown service \"{service}\". Known services: {valid}"),
        ))
    }
}

/// Escape a value for safe interpolation inside a double-quoted LogQL/TraceQL
/// string literal, so caller-supplied filters can't break the generated query.
pub(super) fn escape_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
