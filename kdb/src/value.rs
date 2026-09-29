/// A single cell attribute's value. Every cell is logically a
/// `HashMap<String, Value>`, but we never materialize that map directly on
/// disk or in memory -- see `chunk.rs` for the columnar layout that actually
/// stores this data.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    F64(f64),
    I64(i64),
}

impl Value {
    pub const TAG_STR: u8 = 0;
    pub const TAG_F64: u8 = 1;
    pub const TAG_I64: u8 = 2;
}
