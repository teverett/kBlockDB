//! JSON <-> `kdb::Value` conversion.
//!
//! `kdb::Value` itself derives no `serde` traits -- `kdb` is deliberately
//! dependency-free, and JSON is a concern of this server, not of the
//! storage engine. `ValueJson` is the wire representation instead, a
//! tagged union that round-trips exactly the three `kdb::Value` variants:
//!
//! ```json
//! {"type": "str", "value": "stone"}
//! {"type": "f64", "value": 2.6}
//! {"type": "i64", "value": 7}
//! ```

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ValueJson {
    Str { value: String },
    F64 { value: f64 },
    I64 { value: i64 },
}

impl From<kdb::Value> for ValueJson {
    fn from(v: kdb::Value) -> Self {
        match v {
            kdb::Value::Str(value) => ValueJson::Str { value },
            kdb::Value::F64(value) => ValueJson::F64 { value },
            kdb::Value::I64(value) => ValueJson::I64 { value },
        }
    }
}

impl From<ValueJson> for kdb::Value {
    fn from(v: ValueJson) -> Self {
        match v {
            ValueJson::Str { value } => kdb::Value::Str(value),
            ValueJson::F64 { value } => kdb::Value::F64(value),
            ValueJson::I64 { value } => kdb::Value::I64(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_kdb_value_and_json() {
        for (json, kdb_value) in [
            (
                r#"{"type":"str","value":"stone"}"#,
                kdb::Value::Str("stone".into()),
            ),
            (r#"{"type":"f64","value":2.6}"#, kdb::Value::F64(2.6)),
            (r#"{"type":"i64","value":7}"#, kdb::Value::I64(7)),
        ] {
            let parsed: ValueJson = serde_json::from_str(json).unwrap();
            assert_eq!(kdb::Value::from(parsed.clone()), kdb_value);
            assert_eq!(ValueJson::from(kdb_value), parsed);
        }
    }

    #[test]
    fn rejects_an_unknown_type_tag() {
        let err = serde_json::from_str::<ValueJson>(r#"{"type":"bool","value":true}"#);
        assert!(err.is_err());
    }
}
