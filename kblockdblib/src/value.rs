/// A single cell attribute's value. Every cell is logically a
/// `HashMap<String, Value>`, but we never materialize that map directly on
/// disk or in memory -- see `chunk.rs` for the columnar layout that actually
/// stores this data.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    F64(f64),
    I64(i64),
    Bool(bool),
}

impl Value {
    pub const TAG_STR: u8 = 0;
    pub const TAG_F64: u8 = 1;
    pub const TAG_I64: u8 = 2;
    pub const TAG_BOOL: u8 = 3;

    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Str(_) => ValueType::Str,
            Value::F64(_) => ValueType::F64,
            Value::I64(_) => ValueType::I64,
            Value::Bool(_) => ValueType::Bool,
        }
    }
}

/// Which of `Value`'s variants a value is, without the value itself -- what
/// `Schema` records per key the first time it's ever set, and rejects a
/// later `set` for changing (see `Schema`'s doc comment on why a key's type
/// is fixed for the life of the world, not just a chunk).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Str,
    F64,
    I64,
    Bool,
}

impl ValueType {
    /// Short, stable, on-disk string form for `schema.txt` -- never
    /// changes even if `Value`'s `Debug` output someday does, since
    /// existing `schema.txt` files must stay parseable.
    pub fn as_str(self) -> &'static str {
        match self {
            ValueType::Str => "str",
            ValueType::F64 => "f64",
            ValueType::I64 => "i64",
            ValueType::Bool => "bool",
        }
    }

    /// The inverse of `as_str`, for reading `schema.txt` back. `None` for
    /// anything else -- a corrupt or (from a newer version) unrecognized
    /// type tag.
    pub fn parse(s: &str) -> Option<ValueType> {
        match s {
            "str" => Some(ValueType::Str),
            "f64" => Some(ValueType::F64),
            "i64" => Some(ValueType::I64),
            "bool" => Some(ValueType::Bool),
            _ => None,
        }
    }
}
