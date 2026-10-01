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

    /// The one-byte tag a `Value` of this type carries on the wire and in
    /// a chunk file -- the same `Value::TAG_*` constants, so a type can be
    /// sent on its own (a column declaration, say) without a value
    /// attached and still agree with every encoded value of that type.
    pub const fn tag(self) -> u8 {
        match self {
            ValueType::Str => Value::TAG_STR,
            ValueType::F64 => Value::TAG_F64,
            ValueType::I64 => Value::TAG_I64,
            ValueType::Bool => Value::TAG_BOOL,
        }
    }

    /// The inverse of `tag`. `None` for an unrecognized tag byte.
    pub fn from_tag(tag: u8) -> Option<ValueType> {
        match tag {
            Value::TAG_STR => Some(ValueType::Str),
            Value::TAG_F64 => Some(ValueType::F64),
            Value::TAG_I64 => Some(ValueType::I64),
            Value::TAG_BOOL => Some(ValueType::Bool),
            _ => None,
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
