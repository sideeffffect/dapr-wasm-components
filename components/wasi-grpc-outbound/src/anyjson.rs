//! JSON ↔ `google.protobuf.Any` conversions.
//!
//! Several Dapr gRPC messages carry JSON-ish payloads as `Any` (job data,
//! conversation parameters). The WIT contract keeps them as JSON strings;
//! we pack them as `google.protobuf.Value` — the canonical proto encoding
//! of a JSON document — and unpack tolerantly.

use prost::Message;

const VALUE_TYPE_URL: &str = "type.googleapis.com/google.protobuf.Value";

fn json_to_value(json: &serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match json {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(*b),
        serde_json::Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => Kind::StringValue(s.clone()),
        serde_json::Value::Array(items) => Kind::ListValue(prost_types::ListValue {
            values: items.iter().map(json_to_value).collect(),
        }),
        serde_json::Value::Object(entries) => Kind::StructValue(prost_types::Struct {
            fields: entries
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

fn value_to_json(value: &prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind;
    match &value.kind {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Kind::NumberValue(n)) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Kind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.iter().map(value_to_json).collect())
        }
        Some(Kind::StructValue(object)) => serde_json::Value::Object(
            object
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

/// Parse a JSON object (text) into a `google.protobuf.Struct` — used for
/// conversation tool parameters and the response-format schema, which the
/// proto carries as `Struct`.
pub fn json_to_struct(json_text: &str) -> Result<prost_types::Struct, crate::types::Error> {
    let parsed: serde_json::Value = serde_json::from_str(json_text).map_err(|e| {
        crate::types::Error::InvalidArgument(format!("expected a JSON object: {e}"))
    })?;
    match json_to_value(&parsed).kind {
        Some(prost_types::value::Kind::StructValue(object)) => Ok(object),
        _ => Err(crate::types::Error::InvalidArgument(
            "expected a JSON object".to_string(),
        )),
    }
}

/// Pack a JSON document (text) as `Any(google.protobuf.Value)`.
pub fn pack_json(json_text: &str) -> Result<prost_types::Any, crate::types::Error> {
    let parsed: serde_json::Value = serde_json::from_str(json_text).map_err(|e| {
        crate::types::Error::InvalidArgument(format!("payload is not valid JSON: {e}"))
    })?;
    Ok(prost_types::Any {
        type_url: VALUE_TYPE_URL.to_string(),
        value: json_to_value(&parsed).encode_to_vec(),
    })
}

/// Unpack an `Any` back to JSON text: `google.protobuf.Value` payloads are
/// decoded properly, `BytesValue`/`StringValue` wrappers (daprd packs e.g.
/// actor reminder data as `BytesValue`) are unwrapped to their payload,
/// anything else is returned as its raw bytes (lossy).
pub fn unpack_json(any: &prost_types::Any) -> String {
    if any.type_url.ends_with("google.protobuf.Value") {
        if let Ok(value) = prost_types::Value::decode(any.value.as_slice()) {
            return value_to_json(&value).to_string();
        }
    }
    if any.type_url.ends_with("google.protobuf.BytesValue") {
        if let Ok(wrapper) = wrappers::BytesValue::decode(any.value.as_slice()) {
            return String::from_utf8_lossy(&wrapper.value).into_owned();
        }
    }
    if any.type_url.ends_with("google.protobuf.StringValue") {
        if let Ok(wrapper) = wrappers::StringValue::decode(any.value.as_slice()) {
            return wrapper.value;
        }
    }
    String::from_utf8_lossy(&any.value).into_owned()
}

/// The `google.protobuf.*Value` wrapper messages (each a single field,
/// tag 1). `prost-types` does not ship them.
pub mod wrappers {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct DoubleValue {
        #[prost(double, tag = "1")]
        pub value: f64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct FloatValue {
        #[prost(float, tag = "1")]
        pub value: f32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Int64Value {
        #[prost(int64, tag = "1")]
        pub value: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct UInt64Value {
        #[prost(uint64, tag = "1")]
        pub value: u64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Int32Value {
        #[prost(int32, tag = "1")]
        pub value: i32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct UInt32Value {
        #[prost(uint32, tag = "1")]
        pub value: u32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct BoolValue {
        #[prost(bool, tag = "1")]
        pub value: bool,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct StringValue {
        #[prost(string, tag = "1")]
        pub value: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct BytesValue {
        #[prost(bytes = "vec", tag = "1")]
        pub value: Vec<u8>,
    }
}

/// Pack a protojson-style wrapper object —
/// `{"@type": "type.googleapis.com/google.protobuf.Int64Value", "value": "100"}`
/// — as the corresponding `Any`. This is the encoding the Dapr docs use for
/// typed conversation parameters. Returns `None` when `object` carries no
/// `@type` (the caller falls back to `pack_json`), an error for `@type`s
/// this provider cannot encode.
pub fn pack_protojson_wrapper(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<Result<prost_types::Any, crate::types::Error>> {
    use crate::types::Error;

    let type_url = object.get("@type")?.as_str()?.to_string();
    let value = object
        .get("value")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    fn parse<T: std::str::FromStr>(value: &serde_json::Value, what: &str) -> Result<T, Error> {
        // protojson allows 64-bit ints (and more) as JSON strings.
        match value {
            serde_json::Value::String(text) => text.parse().ok(),
            other => other.to_string().parse().ok(),
        }
        .ok_or_else(|| Error::InvalidArgument(format!("@type {what}: invalid value {value}")))
    }

    let suffix = type_url.rsplit('/').next().unwrap_or(&type_url);
    let encoded = match suffix {
        "google.protobuf.DoubleValue" => {
            parse(&value, suffix).map(|value| wrappers::DoubleValue { value }.encode_to_vec())
        }
        "google.protobuf.FloatValue" => {
            parse(&value, suffix).map(|value| wrappers::FloatValue { value }.encode_to_vec())
        }
        "google.protobuf.Int64Value" => {
            parse(&value, suffix).map(|value| wrappers::Int64Value { value }.encode_to_vec())
        }
        "google.protobuf.UInt64Value" => {
            parse(&value, suffix).map(|value| wrappers::UInt64Value { value }.encode_to_vec())
        }
        "google.protobuf.Int32Value" => {
            parse(&value, suffix).map(|value| wrappers::Int32Value { value }.encode_to_vec())
        }
        "google.protobuf.UInt32Value" => {
            parse(&value, suffix).map(|value| wrappers::UInt32Value { value }.encode_to_vec())
        }
        "google.protobuf.BoolValue" => value
            .as_bool()
            .ok_or_else(|| {
                crate::types::Error::InvalidArgument(format!("@type {suffix}: invalid value"))
            })
            .map(|value| wrappers::BoolValue { value }.encode_to_vec()),
        "google.protobuf.StringValue" => value
            .as_str()
            .ok_or_else(|| {
                crate::types::Error::InvalidArgument(format!("@type {suffix}: invalid value"))
            })
            .map(|text| {
                wrappers::StringValue {
                    value: text.to_string(),
                }
                .encode_to_vec()
            }),
        "google.protobuf.Value" => Ok(json_to_value(&value).encode_to_vec()),
        other => Err(crate::types::Error::InvalidArgument(format!(
            "@type {other} is not supported here"
        ))),
    };
    Some(encoded.map(|value| prost_types::Any { type_url, value }))
}
