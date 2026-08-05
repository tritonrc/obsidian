//! Machinery behind [`super`]: the traits, macros, and validator that turn one
//! field list into a schema, a deserializer, and a caller-facing error.
//!
//! Nothing here knows about a specific tool. Add a tool by declaring its
//! arguments in [`super`], not by touching this file.

use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// One tool's arguments: the struct its handler reads, plus the `inputSchema`
/// advertised for it. Both come from the same `tool_args!` declaration, so the
/// schema cannot describe a property the handler has no field for.
pub(in crate::mcp::tools) trait ToolArgs: DeserializeOwned {
    /// The tool name, for caller-facing messages.
    const TOOL: &'static str;

    /// The advertised `inputSchema`, generated from the declared fields.
    fn schema() -> Value;
}

/// A type usable as a tool argument, and the JSON Schema describing it.
pub(in crate::mcp::tools) trait ArgType {
    /// Whether the argument may be omitted. Only `Option<T>` may.
    const OPTIONAL: bool = false;

    /// The schema for this argument's value.
    fn arg_schema() -> Value;
}

impl ArgType for String {
    fn arg_schema() -> Value {
        json!({ "type": "string" })
    }
}

/// Every integer argument is a count or a checkpoint token, so the advertised
/// lower bound is the one `u64` can hold — and the one [`problem`] enforces.
impl ArgType for u64 {
    fn arg_schema() -> Value {
        json!({ "type": "integer", "minimum": 0 })
    }
}

/// An optional argument has the same value schema as a required one; it is only
/// absent from `required`.
impl<T: ArgType> ArgType for Option<T> {
    const OPTIONAL: bool = true;

    fn arg_schema() -> Value {
        T::arg_schema()
    }
}

/// Declare a tool's arguments once: the field list becomes the handler's struct,
/// the advertised `inputSchema`, and the validator's allowlist.
///
/// A field's written type decides everything about it — `Option<T>` is optional,
/// anything else is `required`, and `T`'s [`ArgType`] supplies the value schema.
/// There is no second place to update, which is the point.
///
/// Expands against `ArgType`, `ToolArgs`, and `serde` being in scope at the use
/// site (they are in [`super`], the only intended one).
macro_rules! tool_args {
    (
        $(#[$doc:meta])*
        struct $name:ident for $tool:literal {
            $( $field:ident : $ty:ty ),* $(,)?
        }
    ) => {
        $(#[$doc])*
        #[derive(Debug, ::serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub(in crate::mcp::tools) struct $name {
            $( pub $field: $ty, )*
        }

        impl ToolArgs for $name {
            const TOOL: &'static str = $tool;

            fn schema() -> ::serde_json::Value {
                let fields: Vec<(&'static str, ::serde_json::Value, bool)> = vec![
                    $((
                        stringify!($field),
                        <$ty as ArgType>::arg_schema(),
                        <$ty as ArgType>::OPTIONAL,
                    ),)*
                ];
                let required: Vec<::serde_json::Value> = fields
                    .iter()
                    .filter(|(_, _, optional)| !optional)
                    .map(|(name, ..)| ::serde_json::json!(name))
                    .collect();
                let properties: ::serde_json::Map<String, ::serde_json::Value> = fields
                    .into_iter()
                    .map(|(name, schema, _)| (name.to_owned(), schema))
                    .collect();
                let mut schema = ::serde_json::json!({
                    "type": "object",
                    "properties": ::serde_json::Value::Object(properties),
                    "additionalProperties": false,
                });
                if !required.is_empty() {
                    schema["required"] = ::serde_json::Value::Array(required);
                }
                schema
            }
        }
    };
}

/// Declare a closed set of string values once: the enum a handler matches on,
/// its wire spelling, and the `enum` the schema advertises.
///
/// Keeping the literals in one place is what lets the validator reject an
/// out-of-range value instead of a handler quietly falling through to a default.
macro_rules! str_enum {
    (
        $(#[$doc:meta])*
        enum $name:ident { $( $variant:ident = $text:literal ),* $(,)? }
    ) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, ::serde::Deserialize)]
        pub(in crate::mcp::tools) enum $name {
            $( #[serde(rename = $text)] $variant, )*
        }

        impl $name {
            /// The wire spelling — the same literal the schema advertises.
            pub(in crate::mcp::tools) fn as_str(self) -> &'static str {
                match self { $( Self::$variant => $text, )* }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ArgType for $name {
            fn arg_schema() -> ::serde_json::Value {
                ::serde_json::json!({ "type": "string", "enum": [ $($text),* ] })
            }
        }
    };
}

pub(super) use {str_enum, tool_args};

/// Validate `args` against `T`'s advertised schema, then deserialize it.
///
/// Both steps run because they answer different questions. [`problem`] produces
/// the message: serde reports `invalid type: integer 123, expected a string`
/// without naming the field it was reading, and a message that does not name the
/// argument cannot be acted on by the caller. Deserialization is the authority on
/// what the handler actually sees. A value that passes the first and fails the
/// second is a bug in this file, not caller error — hence the generic fallback
/// message and the round-trip tests in [`super`].
pub(in crate::mcp::tools) fn parse<T: ToolArgs>(args: &Value) -> Result<T, String> {
    if let Some(problem) = problem(T::TOOL, &T::schema(), args) {
        return Err(problem);
    }
    serde_json::from_value(args.clone())
        .map_err(|e| format!("{} could not read these arguments: {e}", T::TOOL))
}

/// Does `value` satisfy a JSON Schema `type` keyword? Unknown type names and
/// type unions (which no `inputSchema` uses) are not constrained.
///
/// `integer` means what `u64` can hold: a non-negative integer literal. Every
/// integer argument here is a count or a checkpoint token, advertised
/// `minimum: 0` so a client validating against `tools/list` agrees, and the
/// fractional form `50.0` — formally a JSON Schema integer — is refused rather
/// than accepted and then dropped on deserialization. Narrower than the keyword,
/// but a validator that admits values the handlers cannot read reopens the
/// silent-drop bug this guard exists to prevent.
fn type_matches(expected: &str, value: &Value) -> bool {
    match expected {
        "string" => value.is_string(),
        "integer" => value.is_u64(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        _ => true,
    }
}

/// How a declared type is described to the caller, so the message says what to
/// write rather than only what was wrong.
fn type_expectation(expected: &str) -> &str {
    match expected {
        "integer" => "a non-negative integer",
        "string" => "a string",
        "number" => "a number",
        "boolean" => "a boolean",
        "object" => "an object",
        "array" => "an array",
        other => other,
    }
}

/// The JSON Schema type name for a value, for error messages.
fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Why `tool` cannot accept these arguments, phrased for the caller, or `None`
/// if every key is declared, every value fits its declared schema, and every
/// required argument is present.
///
/// Each of these was once a silent drop: an undeclared key, a wrong-typed value,
/// and an out-of-`enum` value all deserialize to "absent", and an absent filter
/// is invisible in the result — which still reports a count that reads exactly
/// like a match.
///
/// `args` must be an object; a non-object is malformed request structure,
/// rejected by the caller before this point.
fn problem(tool: &str, schema: &Value, args: &Value) -> Option<String> {
    let props = schema.get("properties")?.as_object()?;
    let given = args.as_object()?;

    for (key, value) in given {
        let Some(spec) = props.get(key) else {
            let accepts = if props.is_empty() {
                format!("{tool} takes no arguments")
            } else {
                format!("{tool} accepts: {}", names(props, &[]))
            };
            return Some(format!(
                "unknown argument `{key}` — {accepts}. Undeclared keys are not filters: they are \
                 never applied, so the result would have looked filtered when it was not. For \
                 span/resource attributes use a raw query string."
            ));
        };
        if let Some(expected) = spec.get("type").and_then(|t| t.as_str())
            && !type_matches(expected, value)
        {
            return Some(format!(
                "argument `{key}` must be {}, got {} ({value}). A value the handler cannot read \
                 is read as absent, so its filter would never have been applied.",
                type_expectation(expected),
                type_name(value)
            ));
        }
        if let Some(allowed) = spec.get("enum").and_then(|e| e.as_array())
            && !allowed.contains(value)
        {
            let valid: Vec<String> = allowed.iter().map(|v| v.to_string()).collect();
            return Some(format!(
                "invalid `{key}`: {value}. Valid values: {}",
                valid.join(", ")
            ));
        }
    }

    // Required arguments are checked last: a key the caller did write wrong is
    // more actionable than one they left out.
    let required: Vec<&str> = schema
        .get("required")
        .and_then(|r| r.as_array())
        .into_iter()
        .flatten()
        .filter_map(|k| k.as_str())
        .collect();
    if let Some(missing) = required.iter().find(|key| !given.contains_key(**key)) {
        // Naming the required set separately from the optional one answers the
        // next question too: what else this call could have carried.
        let mut message = format!(
            "missing required argument `{missing}` — {tool} requires: {}",
            required.join(", ")
        );
        let optional = names(props, &required);
        if !optional.is_empty() {
            message.push_str(&format!("; optional: {optional}"));
        }
        return Some(message);
    }
    None
}

/// Declared property names, sorted and minus `exclude`, for a "what does this
/// tool take" hint.
fn names(props: &serde_json::Map<String, Value>, exclude: &[&str]) -> String {
    let mut names: Vec<&str> = props
        .keys()
        .map(String::as_str)
        .filter(|name| !exclude.contains(name))
        .collect();
    names.sort_unstable();
    names.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    tool_args! {
        /// A stand-in tool, so these tests exercise the machinery rather than
        /// whichever real tool happens to have the right field shape today.
        struct SampleArgs for "sample" {
            name: String,
            limit: Option<u64>,
            mode: Option<SampleMode>,
        }
    }

    str_enum! {
        enum SampleMode { Fast = "fast", Slow = "slow" }
    }

    #[test]
    fn schema_is_generated_from_the_field_types() {
        assert_eq!(
            SampleArgs::schema(),
            json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 0 },
                    "mode": { "type": "string", "enum": ["fast", "slow"] }
                },
                "additionalProperties": false,
                "required": ["name"]
            })
        );
    }

    #[test]
    fn a_tool_with_no_arguments_advertises_a_closed_empty_object() {
        tool_args! {
            struct NoArgs for "no_args" {}
        }
        assert_eq!(
            NoArgs::schema(),
            json!({ "type": "object", "properties": {}, "additionalProperties": false })
        );
    }

    #[test]
    fn declared_arguments_parse_into_the_struct() {
        let parsed: SampleArgs =
            parse(&json!({ "name": "api", "limit": 5, "mode": "slow" })).unwrap();
        assert_eq!(parsed.name, "api");
        assert_eq!(parsed.limit, Some(5));
        assert_eq!(parsed.mode, Some(SampleMode::Slow));
    }

    #[test]
    fn omitted_optional_arguments_parse_as_absent() {
        let parsed: SampleArgs = parse(&json!({ "name": "api" })).unwrap();
        assert_eq!(parsed.limit, None);
        assert_eq!(parsed.mode, None);
    }

    #[test]
    fn a_missing_required_argument_is_named() {
        let problem = parse::<SampleArgs>(&json!({ "limit": 5 })).unwrap_err();
        assert!(
            problem.contains("`name`"),
            "must name the argument: {problem}"
        );
        assert!(
            problem.contains("requires: name"),
            "must say what is required, not what is merely accepted: {problem}"
        );
        assert!(
            problem.contains("optional: limit, mode"),
            "must separate the optional arguments from the required ones: {problem}"
        );
    }

    #[test]
    fn an_undeclared_key_is_named_alongside_what_the_tool_accepts() {
        let problem =
            parse::<SampleArgs>(&json!({ "name": "api", "environment": "prod" })).unwrap_err();
        assert!(problem.contains("environment"), "{problem}");
        assert!(
            problem.contains("limit"),
            "must list declared keys: {problem}"
        );
    }

    #[test]
    fn a_wrong_typed_value_is_named_with_the_type_it_needs() {
        let problem = parse::<SampleArgs>(&json!({ "name": 123 })).unwrap_err();
        assert!(problem.contains("`name`"), "{problem}");
        assert!(problem.contains("a string"), "{problem}");
    }

    #[test]
    fn an_out_of_enum_value_lists_the_valid_ones() {
        let problem = parse::<SampleArgs>(&json!({ "name": "api", "mode": "medium" })).unwrap_err();
        assert!(problem.contains("`mode`"), "{problem}");
        assert!(
            problem.contains("fast"),
            "must list valid values: {problem}"
        );
    }

    /// `50.0` is a JSON Schema integer but not a `u64`; accepting it here would
    /// hand deserialization a value it drops.
    #[test]
    fn the_fractional_form_of_an_integer_is_refused() {
        let problem = parse::<SampleArgs>(&json!({ "name": "api", "limit": 50.0 })).unwrap_err();
        assert!(problem.contains("non-negative integer"), "{problem}");
    }

    #[test]
    fn a_negative_integer_is_refused() {
        let problem = parse::<SampleArgs>(&json!({ "name": "api", "limit": -5 })).unwrap_err();
        assert!(problem.contains("`limit`"), "{problem}");
    }

    /// An explicit `null` for an optional filter is the same trap as omitting a
    /// key the tool never reads: it looks set and is not applied.
    #[test]
    fn an_explicit_null_is_refused_rather_than_read_as_absent() {
        let problem = parse::<SampleArgs>(&json!({ "name": "api", "mode": null })).unwrap_err();
        assert!(problem.contains("`mode`"), "{problem}");
    }

    #[test]
    fn a_closed_enum_round_trips_through_its_wire_spelling() {
        assert_eq!(SampleMode::Fast.as_str(), "fast");
        assert_eq!(SampleMode::Slow.to_string(), "slow");
    }
}
