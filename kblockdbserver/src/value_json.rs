//! JSON <-> `kblockdblib::Value` conversion.
//!
//! `kblockdblib::Value` itself derives no `serde` traits -- `kblockdblib` is deliberately
//! dependency-free, and JSON is a concern of this server, not of the
//! storage engine. `ValueJson` is the wire representation instead, a
//! tagged union that round-trips exactly the four `kblockdblib::Value` variants:
//!
//! ```json
//! {"type": "str", "value": "stone"}
//! {"type": "f64", "value": 2.6}
//! {"type": "i64", "value": 7}
//! {"type": "bool", "value": true}
//! ```

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ValueJson {
    Str { value: String },
    F64 { value: f64 },
    I64 { value: i64 },
    Bool { value: bool },
}

impl From<kblockdblib::Value> for ValueJson {
    fn from(v: kblockdblib::Value) -> Self {
        match v {
            kblockdblib::Value::Str(value) => ValueJson::Str { value },
            kblockdblib::Value::F64(value) => ValueJson::F64 { value },
            kblockdblib::Value::I64(value) => ValueJson::I64 { value },
            kblockdblib::Value::Bool(value) => ValueJson::Bool { value },
        }
    }
}

impl From<ValueJson> for kblockdblib::Value {
    fn from(v: ValueJson) -> Self {
        match v {
            ValueJson::Str { value } => kblockdblib::Value::Str(value),
            ValueJson::F64 { value } => kblockdblib::Value::F64(value),
            ValueJson::I64 { value } => kblockdblib::Value::I64(value),
            ValueJson::Bool { value } => kblockdblib::Value::Bool(value),
        }
    }
}

/// A `kblockdblib::ValueType` on the wire: the same four lowercase tags
/// `ValueJson`'s `type` field uses (`"str"`, `"f64"`, `"i64"`, `"bool"`),
/// but standalone -- a column declares its type without carrying a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ValueTypeJson {
    Str,
    F64,
    I64,
    Bool,
}

impl From<kblockdblib::ValueType> for ValueTypeJson {
    fn from(t: kblockdblib::ValueType) -> Self {
        match t {
            kblockdblib::ValueType::Str => ValueTypeJson::Str,
            kblockdblib::ValueType::F64 => ValueTypeJson::F64,
            kblockdblib::ValueType::I64 => ValueTypeJson::I64,
            kblockdblib::ValueType::Bool => ValueTypeJson::Bool,
        }
    }
}

impl From<ValueTypeJson> for kblockdblib::ValueType {
    fn from(t: ValueTypeJson) -> Self {
        match t {
            ValueTypeJson::Str => kblockdblib::ValueType::Str,
            ValueTypeJson::F64 => kblockdblib::ValueType::F64,
            ValueTypeJson::I64 => kblockdblib::ValueType::I64,
            ValueTypeJson::Bool => kblockdblib::ValueType::Bool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_kblockdblib_value_and_json() {
        for (json, kblockdblib_value) in [
            (
                r#"{"type":"str","value":"stone"}"#,
                kblockdblib::Value::Str("stone".into()),
            ),
            (
                r#"{"type":"f64","value":2.6}"#,
                kblockdblib::Value::F64(2.6),
            ),
            (r#"{"type":"i64","value":7}"#, kblockdblib::Value::I64(7)),
            (
                r#"{"type":"bool","value":true}"#,
                kblockdblib::Value::Bool(true),
            ),
        ] {
            let parsed: ValueJson = serde_json::from_str(json).unwrap();
            assert_eq!(kblockdblib::Value::from(parsed.clone()), kblockdblib_value);
            assert_eq!(ValueJson::from(kblockdblib_value), parsed);
        }
    }

    #[test]
    fn rejects_an_unknown_type_tag() {
        let err = serde_json::from_str::<ValueJson>(r#"{"type":"complex","value":true}"#);
        assert!(err.is_err());
    }
}
