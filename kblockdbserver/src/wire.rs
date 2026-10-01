//! kBlockDB's binary protocol: a peer to the REST API, not a replacement
//! for it -- same `AppState`/`World`, same accounts, same semantics, just
//! without HTTP/JSON's per-call overhead (see `binary_server.rs` and the
//! README's "Binary protocol" section for why this exists and when it's
//! worth reaching for).
//!
//! This module is deliberately self-contained (no dependency on anything
//! else in this crate, like `AppState`/`ApiError`) and published via
//! `kblockdbserver`'s `[lib]` target, so a client -- `kblockdbperf`'s
//! `binary_client.rs`, currently the only one -- can `use
//! kblockdbserver::wire` and share this exact encode/decode logic instead
//! of reimplementing the wire format from this doc comment and risking
//! drift from what the server actually speaks.
//!
//! **Framing.** Every message, either direction, over a persistent TCP
//! connection, is a length-prefixed frame:
//!
//! ```text
//! [u32 LE payload_len] [payload_len bytes: payload]
//! ```
//!
//! `payload_len` is capped at [`MAX_FRAME_LEN`] -- a client that sends a
//! larger one gets the connection closed on it, rather than this server
//! trying to allocate however many gigabytes an attacker felt like
//! claiming. Because framing only depends on this length prefix, never on
//! understanding the payload, a malformed *payload* (an unknown opcode, a
//! truncated field, ...) never desyncs the stream -- the server can always
//! find the start of the *next* frame regardless of whether it understood
//! this one, so one bad request doesn't have to end the connection.
//!
//! **Requests** (client -> server), as `payload`:
//!
//! ```text
//! [u8 opcode] <opcode-specific fields>
//!
//! 0x00 Hello:  [u8 username_len][username bytes]
//!              [u8 password_len][password bytes]
//! 0x01 Get:    <coord> <key>
//! 0x02 Set:    <coord> <key> <value>
//! 0x03 Remove: <coord> <key>
//! 0x04 Health: (no fields)
//! 0x05 Stats:  (no fields)
//! 0x06 GetRegion:    <coord origin> <coord extent> <key>
//! 0x07 SetRegion:    <coord origin> <coord extent> <key>
//!                    [u32 LE value_count][value_count * <value>]
//! 0x08 RemoveRegion: <coord origin> <coord extent> <key>
//! 0x09 Query:        [u32 LE query_len][query bytes]
//! 0x0a ListColumns:  (no fields)
//! 0x0b AddColumn:    <key> [u8 value type tag]
//! 0x0c RemoveColumn: <key>
//! ```
//!
//! `<coord>` is `[u8 axes][axes * i32 LE]` (signed -- kBlockDB's coordinate
//! space is zero-centered, see `kblockdblib::World`'s doc comment on its
//! axis bounds); `<key>` is `[u16 LE key_len][key bytes]`; `<value>` is
//! `[u8 type_tag]` (see [`kblockdblib::Value`]'s `TAG_*` constants) followed
//! by `[8 bytes LE]` for F64/I64, `[u32 LE len][len bytes]` for Str, or
//! `[1 byte]` (0 or 1) for Bool.
//!
//! **Responses** (server -> client), as `payload`. Every status has one
//! fixed shape regardless of which request it's answering -- the client
//! always knows what it just asked, so there's no ambiguity, but decoding
//! never has to branch on context either:
//!
//! ```text
//! [u8 status] <status-specific fields>
//!
//! 0x00 HelloOk:      [u8 axes][u32 LE world_dim][u8 read_only (0/1)]
//! 0x01 Ok:           (Set/Remove succeeded)
//! 0x02 Value:        <value> <meta>
//! 0x03 NotFound:     (Get found nothing)
//! 0x04 BadRequest:   <message>
//! 0x05 Unauthorized: <message>
//! 0x06 Forbidden:    <message>
//! 0x07 Internal:     <message>
//! 0x08 Health:       [u8 axes][u32 LE world_dim][u32 LE chunk_dim]
//!                    [u64 LE timestamp_seconds] <hostname as a message>
//! 0x09 Stats:        [u64 LE total_chunks][u64 LE total_bytes][u64 LE total_blocks]
//! 0x0a RegionValues: [u32 LE count][count * ([u8 present] [<value> if present])]
//! 0x0b Query:        [u8 kind] <query-specific fields>
//! 0x0c Columns:      [u32 LE count][count * (<key> [u8 value type tag])]
//! 0x0d Conflict:     <message>
//! ```
//!
//! `<message>` is `[u16 LE len][len bytes utf8]`. `<meta>` is
//! `[u64 LE created_at_ms][u64 LE modified_at_ms][u64 LE version]` --
//! see [`kblockdblib::CellMeta`], which this mirrors field-for-field.
//!
//! One request, one response, strictly in order -- this minimal version
//! doesn't pipeline multiple in-flight requests on one connection (a
//! client that wants more throughput than one connection's round-trip
//! latency allows should open more connections, the same way it would
//! against the REST API).

// Re-exported (not just imported) so a client depending only on this
// crate's `wire` module -- like `kblockdbperf`, which deliberately
// doesn't depend on `kblockdblib` directly (it treats `kblockdbserver` as
// a black box) -- can name `Value` without adding that dependency itself.
pub use kblockdblib::{Value, ValueType};
use std::fmt;

/// No frame's payload may claim to be larger than this -- see this
/// module's doc comment. 64 MiB comfortably fits any realistic single
/// cell value while still bounding how much a misbehaving or malicious
/// peer can make this server try to allocate from one length prefix.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug, PartialEq)]
pub enum Request {
    Hello {
        username: String,
        password: String,
    },
    Get {
        coord: Vec<i32>,
        key: String,
    },
    Set {
        coord: Vec<i32>,
        key: String,
        value: Value,
    },
    Remove {
        coord: Vec<i32>,
        key: String,
    },
    Health,
    Stats,
    GetRegion {
        origin: Vec<i32>,
        extent: Vec<i32>,
        key: String,
    },
    SetRegion {
        origin: Vec<i32>,
        extent: Vec<i32>,
        key: String,
        values: Vec<Value>,
    },
    RemoveRegion {
        origin: Vec<i32>,
        extent: Vec<i32>,
        key: String,
    },
    Query {
        query: String,
    },
    ListColumns,
    AddColumn {
        key: String,
        value_type: ValueType,
    },
    RemoveColumn {
        key: String,
    },
}

/// One column in the world's schema, as carried by `Response::Columns`.
#[derive(Debug, PartialEq)]
pub struct Column {
    pub key: String,
    pub value_type: ValueType,
}

#[derive(Debug, PartialEq)]
pub struct QueryValue {
    pub key: String,
    pub value: Value,
    pub created_at_ms: u64,
    pub modified_at_ms: u64,
    pub version: u64,
}

#[derive(Debug, PartialEq)]
pub struct QueryRow {
    pub coord: Vec<i32>,
    pub values: Vec<QueryValue>,
}

#[derive(Debug, PartialEq)]
pub enum QueryResult {
    Rows(Vec<QueryRow>),
    Affected(u64),
}

#[derive(Debug, PartialEq)]
pub enum Response {
    HelloOk {
        axes: u8,
        world_dim: u32,
        read_only: bool,
    },
    Ok,
    Value {
        value: Value,
        created_at_ms: u64,
        modified_at_ms: u64,
        version: u64,
    },
    NotFound,
    BadRequest(String),
    Unauthorized(String),
    Forbidden(String),
    Internal(String),
    Health {
        axes: u8,
        world_dim: u32,
        chunk_dim: u32,
        timestamp: u64,
        hostname: String,
    },
    Stats {
        total_chunks: u64,
        total_bytes: u64,
        total_blocks: u64,
    },
    RegionValues(Vec<Option<Value>>),
    Query(QueryResult),
    Columns(Vec<Column>),
    Conflict(String),
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidUtf8,
    UnknownOpcode(u8),
    #[allow(dead_code)] // only decode_response constructs this -- client-side only, see below
    UnknownStatus(u8),
    UnknownValueTag(u8),
    FrameTooLarge(u32),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::UnexpectedEof => write!(f, "truncated frame"),
            DecodeError::InvalidUtf8 => write!(f, "invalid utf-8"),
            DecodeError::UnknownOpcode(b) => write!(f, "unknown opcode 0x{b:02x}"),
            DecodeError::UnknownStatus(b) => write!(f, "unknown status 0x{b:02x}"),
            DecodeError::UnknownValueTag(b) => write!(f, "unknown value type tag 0x{b:02x}"),
            DecodeError::FrameTooLarge(n) => {
                write!(
                    f,
                    "frame of {n} bytes exceeds the {MAX_FRAME_LEN}-byte limit"
                )
            }
        }
    }
}

impl std::error::Error for DecodeError {}

// --- Encoding ---
//
// Every `encode_*` builds just the `payload` bytes -- framing (the u32
// length prefix) is `write_frame`'s job, not this module's, so encoding
// itself never needs to know the eventual total length up front.
//
// `kblockdbserver` only ever encodes *responses* and decodes *requests* --
// `encode_request`/`decode_response` (and the helpers only they use) are
// the other, client-side half of this protocol, kept here so the wire
// format is defined once, symmetrically, in one place rather than
// reimplemented by whatever eventually speaks this protocol as a client.
// Nothing in this crate's own binary calls them yet, hence `allow
// (dead_code)` below -- this module's own round-trip tests are what
// exercise them today.

#[allow(dead_code)]
pub fn encode_request(req: &Request) -> Vec<u8> {
    let mut buf = Vec::new();
    match req {
        Request::Hello { username, password } => {
            buf.push(0x00);
            put_short_string(&mut buf, username);
            put_short_string(&mut buf, password);
        }
        Request::Get { coord, key } => {
            buf.push(0x01);
            put_coord(&mut buf, coord);
            put_key(&mut buf, key);
        }
        Request::Set { coord, key, value } => {
            buf.push(0x02);
            put_coord(&mut buf, coord);
            put_key(&mut buf, key);
            put_value(&mut buf, value);
        }
        Request::Remove { coord, key } => {
            buf.push(0x03);
            put_coord(&mut buf, coord);
            put_key(&mut buf, key);
        }
        Request::Health => buf.push(0x04),
        Request::Stats => buf.push(0x05),
        Request::GetRegion {
            origin,
            extent,
            key,
        } => {
            buf.push(0x06);
            put_coord(&mut buf, origin);
            put_coord(&mut buf, extent);
            put_key(&mut buf, key);
        }
        Request::SetRegion {
            origin,
            extent,
            key,
            values,
        } => {
            buf.push(0x07);
            put_coord(&mut buf, origin);
            put_coord(&mut buf, extent);
            put_key(&mut buf, key);
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for value in values {
                put_value(&mut buf, value);
            }
        }
        Request::RemoveRegion {
            origin,
            extent,
            key,
        } => {
            buf.push(0x08);
            put_coord(&mut buf, origin);
            put_coord(&mut buf, extent);
            put_key(&mut buf, key);
        }
        Request::Query { query } => {
            buf.push(0x09);
            put_long_string(&mut buf, query);
        }
        Request::ListColumns => buf.push(0x0a),
        Request::AddColumn { key, value_type } => {
            buf.push(0x0b);
            put_key(&mut buf, key);
            buf.push(value_type.tag());
        }
        Request::RemoveColumn { key } => {
            buf.push(0x0c);
            put_key(&mut buf, key);
        }
    }
    buf
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
    let mut buf = Vec::new();
    match resp {
        Response::HelloOk {
            axes,
            world_dim,
            read_only,
        } => {
            buf.push(0x00);
            buf.push(*axes);
            buf.extend_from_slice(&world_dim.to_le_bytes());
            buf.push(u8::from(*read_only));
        }
        Response::Ok => buf.push(0x01),
        Response::Value {
            value,
            created_at_ms,
            modified_at_ms,
            version,
        } => {
            buf.push(0x02);
            put_value(&mut buf, value);
            put_meta(&mut buf, *created_at_ms, *modified_at_ms, *version);
        }
        Response::NotFound => buf.push(0x03),
        Response::BadRequest(m) => {
            buf.push(0x04);
            put_message(&mut buf, m);
        }
        Response::Unauthorized(m) => {
            buf.push(0x05);
            put_message(&mut buf, m);
        }
        Response::Forbidden(m) => {
            buf.push(0x06);
            put_message(&mut buf, m);
        }
        Response::Internal(m) => {
            buf.push(0x07);
            put_message(&mut buf, m);
        }
        Response::Health {
            axes,
            world_dim,
            chunk_dim,
            timestamp,
            hostname,
        } => {
            buf.push(0x08);
            buf.push(*axes);
            buf.extend_from_slice(&world_dim.to_le_bytes());
            buf.extend_from_slice(&chunk_dim.to_le_bytes());
            buf.extend_from_slice(&timestamp.to_le_bytes());
            put_message(&mut buf, hostname);
        }
        Response::Stats {
            total_chunks,
            total_bytes,
            total_blocks,
        } => {
            buf.push(0x09);
            buf.extend_from_slice(&total_chunks.to_le_bytes());
            buf.extend_from_slice(&total_bytes.to_le_bytes());
            buf.extend_from_slice(&total_blocks.to_le_bytes());
        }
        Response::RegionValues(values) => {
            buf.push(0x0a);
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for value in values {
                match value {
                    Some(value) => {
                        buf.push(1);
                        put_value(&mut buf, value);
                    }
                    None => buf.push(0),
                }
            }
        }
        Response::Query(result) => {
            buf.push(0x0b);
            match result {
                QueryResult::Rows(rows) => {
                    buf.push(0);
                    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                    for row in rows {
                        put_coord(&mut buf, &row.coord);
                        buf.extend_from_slice(&(row.values.len() as u32).to_le_bytes());
                        for value in &row.values {
                            put_key(&mut buf, &value.key);
                            put_value(&mut buf, &value.value);
                            put_meta(
                                &mut buf,
                                value.created_at_ms,
                                value.modified_at_ms,
                                value.version,
                            );
                        }
                    }
                }
                QueryResult::Affected(count) => {
                    buf.push(1);
                    buf.extend_from_slice(&count.to_le_bytes());
                }
            }
        }
        Response::Columns(columns) => {
            buf.push(0x0c);
            buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());
            for column in columns {
                put_key(&mut buf, &column.key);
                buf.push(column.value_type.tag());
            }
        }
        Response::Conflict(m) => {
            buf.push(0x0d);
            put_message(&mut buf, m);
        }
    }
    buf
}

#[allow(dead_code)] // client-side only, see encode_request above
fn put_coord(buf: &mut Vec<u8>, coord: &[i32]) {
    buf.push(coord.len() as u8);
    for &c in coord {
        buf.extend_from_slice(&c.to_le_bytes());
    }
}

fn put_key(buf: &mut Vec<u8>, key: &str) {
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
}

#[allow(dead_code)] // client-side only, see encode_request above
fn put_short_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(s.len() as u8);
    buf.extend_from_slice(s.as_bytes());
}

#[allow(dead_code)] // client-side only, see encode_request above
fn put_long_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn put_message(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

/// `<meta>`: `[u64 LE created_at_ms][u64 LE modified_at_ms][u64 LE version]`.
fn put_meta(buf: &mut Vec<u8>, created_at_ms: u64, modified_at_ms: u64, version: u64) {
    buf.extend_from_slice(&created_at_ms.to_le_bytes());
    buf.extend_from_slice(&modified_at_ms.to_le_bytes());
    buf.extend_from_slice(&version.to_le_bytes());
}

fn put_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Str(s) => {
            buf.push(Value::TAG_STR);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::F64(x) => {
            buf.push(Value::TAG_F64);
            buf.extend_from_slice(&x.to_le_bytes());
        }
        Value::I64(x) => {
            buf.push(Value::TAG_I64);
            buf.extend_from_slice(&x.to_le_bytes());
        }
        Value::Bool(b) => {
            buf.push(Value::TAG_BOOL);
            buf.push(u8::from(*b));
        }
    }
}

// --- Decoding ---

/// A cursor over an in-memory frame payload -- every `decode_*` reads
/// through one of these instead of hand-tracking an index, so a truncated
/// field anywhere is always just `Err(UnexpectedEof)`, never an out-of-
/// bounds panic.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::UnexpectedEof)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(DecodeError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn string(&mut self, len: usize) -> Result<String, DecodeError> {
        String::from_utf8(self.bytes(len)?.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }

    fn short_string(&mut self) -> Result<String, DecodeError> {
        let len = self.u8()? as usize;
        self.string(len)
    }

    fn key(&mut self) -> Result<String, DecodeError> {
        let len = self.u16()? as usize;
        self.string(len)
    }

    fn long_string(&mut self) -> Result<String, DecodeError> {
        let len = self.u32()? as usize;
        self.string(len)
    }

    #[allow(dead_code)] // only decode_response uses this -- client-side only, see below
    fn message(&mut self) -> Result<String, DecodeError> {
        self.key() // identical shape -- u16 len prefix, utf8 bytes
    }

    fn coord(&mut self) -> Result<Vec<i32>, DecodeError> {
        let axes = self.u8()? as usize;
        (0..axes).map(|_| self.i32()).collect()
    }

    fn value(&mut self) -> Result<Value, DecodeError> {
        match self.u8()? {
            t if t == Value::TAG_STR => {
                let len = self.u32()? as usize;
                Ok(Value::Str(self.string(len)?))
            }
            t if t == Value::TAG_F64 => Ok(Value::F64(self.f64()?)),
            t if t == Value::TAG_I64 => Ok(Value::I64(self.i64()?)),
            t if t == Value::TAG_BOOL => Ok(Value::Bool(self.u8()? != 0)),
            other => Err(DecodeError::UnknownValueTag(other)),
        }
    }

    fn value_type(&mut self) -> Result<ValueType, DecodeError> {
        let tag = self.u8()?;
        ValueType::from_tag(tag).ok_or(DecodeError::UnknownValueTag(tag))
    }

    /// `<meta>`: `[u64 LE created_at_ms][u64 LE modified_at_ms][u64 LE version]`.
    fn meta(&mut self) -> Result<(u64, u64, u64), DecodeError> {
        Ok((self.u64()?, self.u64()?, self.u64()?))
    }
}

pub fn decode_request(payload: &[u8]) -> Result<Request, DecodeError> {
    let mut r = Reader::new(payload);
    match r.u8()? {
        0x00 => Ok(Request::Hello {
            username: r.short_string()?,
            password: r.short_string()?,
        }),
        0x01 => Ok(Request::Get {
            coord: r.coord()?,
            key: r.key()?,
        }),
        0x02 => {
            let coord = r.coord()?;
            let key = r.key()?;
            let value = r.value()?;
            Ok(Request::Set { coord, key, value })
        }
        0x03 => Ok(Request::Remove {
            coord: r.coord()?,
            key: r.key()?,
        }),
        0x04 => Ok(Request::Health),
        0x05 => Ok(Request::Stats),
        0x06 => Ok(Request::GetRegion {
            origin: r.coord()?,
            extent: r.coord()?,
            key: r.key()?,
        }),
        0x07 => {
            let origin = r.coord()?;
            let extent = r.coord()?;
            let key = r.key()?;
            let count = r.u32()? as usize;
            let values = (0..count)
                .map(|_| r.value())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Request::SetRegion {
                origin,
                extent,
                key,
                values,
            })
        }
        0x08 => Ok(Request::RemoveRegion {
            origin: r.coord()?,
            extent: r.coord()?,
            key: r.key()?,
        }),
        0x0a => Ok(Request::ListColumns),
        0x0b => {
            let key = r.key()?;
            let value_type = r.value_type()?;
            Ok(Request::AddColumn { key, value_type })
        }
        0x0c => Ok(Request::RemoveColumn { key: r.key()? }),
        0x09 => Ok(Request::Query {
            query: r.long_string()?,
        }),
        other => Err(DecodeError::UnknownOpcode(other)),
    }
}

// Client-side only, see the note above `encode_request`.
#[allow(dead_code)]
pub fn decode_response(payload: &[u8]) -> Result<Response, DecodeError> {
    let mut r = Reader::new(payload);
    match r.u8()? {
        0x00 => Ok(Response::HelloOk {
            axes: r.u8()?,
            world_dim: r.u32()?,
            read_only: r.u8()? != 0,
        }),
        0x01 => Ok(Response::Ok),
        0x02 => {
            let value = r.value()?;
            let (created_at_ms, modified_at_ms, version) = r.meta()?;
            Ok(Response::Value {
                value,
                created_at_ms,
                modified_at_ms,
                version,
            })
        }
        0x03 => Ok(Response::NotFound),
        0x04 => Ok(Response::BadRequest(r.message()?)),
        0x05 => Ok(Response::Unauthorized(r.message()?)),
        0x06 => Ok(Response::Forbidden(r.message()?)),
        0x07 => Ok(Response::Internal(r.message()?)),
        0x08 => Ok(Response::Health {
            axes: r.u8()?,
            world_dim: r.u32()?,
            chunk_dim: r.u32()?,
            timestamp: r.u64()?,
            hostname: r.message()?,
        }),
        0x09 => Ok(Response::Stats {
            total_chunks: r.u64()?,
            total_bytes: r.u64()?,
            total_blocks: r.u64()?,
        }),
        0x0a => {
            let count = r.u32()? as usize;
            let values = (0..count)
                .map(|_| match r.u8()? {
                    0 => Ok(None),
                    _ => Ok(Some(r.value()?)),
                })
                .collect::<Result<Vec<_>, DecodeError>>()?;
            Ok(Response::RegionValues(values))
        }
        0x0b => match r.u8()? {
            0 => {
                let row_count = r.u32()? as usize;
                let rows = (0..row_count)
                    .map(|_| {
                        let coord = r.coord()?;
                        let value_count = r.u32()? as usize;
                        let values = (0..value_count)
                            .map(|_| {
                                let key = r.key()?;
                                let value = r.value()?;
                                let (created_at_ms, modified_at_ms, version) = r.meta()?;
                                Ok(QueryValue {
                                    key,
                                    value,
                                    created_at_ms,
                                    modified_at_ms,
                                    version,
                                })
                            })
                            .collect::<Result<Vec<_>, DecodeError>>()?;
                        Ok(QueryRow { coord, values })
                    })
                    .collect::<Result<Vec<_>, DecodeError>>()?;
                Ok(Response::Query(QueryResult::Rows(rows)))
            }
            1 => Ok(Response::Query(QueryResult::Affected(r.u64()?))),
            other => Err(DecodeError::UnknownStatus(other)),
        },
        0x0c => {
            let count = r.u32()? as usize;
            let columns = (0..count)
                .map(|_| {
                    let key = r.key()?;
                    let value_type = r.value_type()?;
                    Ok(Column { key, value_type })
                })
                .collect::<Result<Vec<_>, DecodeError>>()?;
            Ok(Response::Columns(columns))
        }
        0x0d => Ok(Response::Conflict(r.message()?)),
        other => Err(DecodeError::UnknownStatus(other)),
    }
}

// --- Frame I/O ---

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Reads one length-prefixed frame's payload off `r`. `Ok(None)` means the
/// peer closed the connection cleanly, right at a frame boundary (the
/// ordinary way a connection ends) -- distinct from an `Err`, which means
/// it closed (or errored) *mid*-frame, or claimed a payload larger than
/// [`MAX_FRAME_LEN`].
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_bytes = [0u8; 4];
    // A plain `read_exact` can't tell "0 bytes then EOF" (a clean
    // disconnect) apart from "1-3 bytes then EOF" (a truncated length
    // prefix) -- both come back as the same `UnexpectedEof`, regardless of
    // how much was actually read first. Reading in a loop with plain
    // `read` (not `read_exact`) keeps track of that ourselves.
    let mut filled = 0;
    while filled < len_bytes.len() {
        let n = r.read(&mut len_bytes[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None); // clean disconnect, right at a frame boundary
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-frame, while reading the length prefix",
            ));
        }
        filled += n;
    }
    let len = u32::from_le_bytes(len_bytes);
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            DecodeError::FrameTooLarge(len),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

/// Writes one length-prefixed frame carrying `payload` to `w`.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, payload: &[u8]) -> std::io::Result<()> {
    if payload.len() as u64 > MAX_FRAME_LEN as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "payload of {} bytes exceeds the {MAX_FRAME_LEN}-byte frame limit",
                payload.len()
            ),
        ));
    }
    w.write_all(&(payload.len() as u32).to_le_bytes()).await?;
    w.write_all(payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(req: Request) {
        let encoded = encode_request(&req);
        assert_eq!(decode_request(&encoded).unwrap(), req);
    }

    fn roundtrip_response(resp: Response) {
        let encoded = encode_response(&resp);
        assert_eq!(decode_response(&encoded).unwrap(), resp);
    }

    #[test]
    fn hello_request_round_trips() {
        roundtrip_request(Request::Hello {
            username: "admin".into(),
            password: "hunter2".into(),
        });
        roundtrip_request(Request::Hello {
            username: String::new(),
            password: String::new(),
        });
    }

    #[test]
    fn get_request_round_trips() {
        roundtrip_request(Request::Get {
            coord: vec![1, 2, 3],
            key: "material".into(),
        });
        // Zero axes is a legal (if useless) coordinate shape -- decoding
        // shouldn't special-case it.
        roundtrip_request(Request::Get {
            coord: vec![],
            key: "material".into(),
        });
    }

    #[test]
    fn set_request_round_trips_every_value_type() {
        for value in [
            Value::Str("stone".into()),
            Value::F64(2.5),
            Value::I64(-7),
            Value::Str(String::new()),
            Value::Bool(true),
            Value::Bool(false),
        ] {
            roundtrip_request(Request::Set {
                coord: vec![9_999, 0, 42],
                key: "k".into(),
                value,
            });
        }
    }

    #[test]
    fn remove_request_round_trips() {
        roundtrip_request(Request::Remove {
            coord: vec![1, 2, 3],
            key: "material".into(),
        });
    }

    #[test]
    fn every_extended_request_round_trips() {
        for request in [
            Request::Health,
            Request::Stats,
            Request::GetRegion {
                origin: vec![0, 1],
                extent: vec![2, 3],
                key: "material".into(),
            },
            Request::SetRegion {
                origin: vec![0, 1],
                extent: vec![2, 1],
                key: "material".into(),
                values: vec![Value::Str("stone".into()), Value::Str("air".into())],
            },
            Request::RemoveRegion {
                origin: vec![0, 1],
                extent: vec![2, 3],
                key: "material".into(),
            },
            Request::Query {
                query: "SELECT *".into(),
            },
        ] {
            roundtrip_request(request);
        }
    }

    #[test]
    fn negative_coordinates_round_trip() {
        roundtrip_request(Request::Get {
            coord: vec![-1, -2, -3],
            key: "material".into(),
        });
        roundtrip_request(Request::Set {
            coord: vec![i32::MIN, i32::MAX, 0],
            key: "k".into(),
            value: Value::I64(1),
        });
    }

    #[test]
    fn hello_ok_response_round_trips() {
        roundtrip_response(Response::HelloOk {
            axes: 3,
            world_dim: 10_000,
            read_only: false,
        });
        roundtrip_response(Response::HelloOk {
            axes: 4,
            world_dim: 1,
            read_only: true,
        });
    }

    #[test]
    fn every_extended_response_round_trips() {
        roundtrip_response(Response::Health {
            axes: 3,
            world_dim: 10_000,
            chunk_dim: 32,
            timestamp: 123,
            hostname: "db-1.example.com".into(),
        });
        // An empty hostname is still a well-formed frame -- the server
        // substitutes "unknown" rather than sending one, but decoding
        // must not depend on that.
        roundtrip_response(Response::Health {
            axes: 1,
            world_dim: 1,
            chunk_dim: 1,
            timestamp: 0,
            hostname: String::new(),
        });
        roundtrip_response(Response::Stats {
            total_chunks: 2,
            total_bytes: 3,
            total_blocks: 4,
        });
        roundtrip_response(Response::RegionValues(vec![
            Some(Value::I64(7)),
            None,
            Some(Value::Bool(true)),
        ]));
        roundtrip_response(Response::Query(QueryResult::Rows(vec![QueryRow {
            coord: vec![1, 2, 3],
            values: vec![QueryValue {
                key: "material".into(),
                value: Value::Str("stone".into()),
                created_at_ms: 10,
                modified_at_ms: 11,
                version: 1,
            }],
        }])));
        roundtrip_response(Response::Query(QueryResult::Affected(9)));
    }

    #[test]
    fn ok_and_not_found_responses_round_trip() {
        roundtrip_response(Response::Ok);
        roundtrip_response(Response::NotFound);
    }

    #[test]
    fn value_response_round_trips_every_type() {
        for value in [
            Value::Str("air".into()),
            Value::F64(1.5),
            Value::I64(0),
            Value::Bool(true),
        ] {
            roundtrip_response(Response::Value {
                value,
                created_at_ms: 1000,
                modified_at_ms: 2000,
                version: 3,
            });
        }
    }

    #[test]
    fn error_responses_round_trip_with_their_message() {
        roundtrip_response(Response::BadRequest("bad coord".into()));
        roundtrip_response(Response::Unauthorized("nope".into()));
        roundtrip_response(Response::Forbidden("read-only".into()));
        roundtrip_response(Response::Internal("disk on fire".into()));
    }

    #[test]
    fn every_column_request_round_trips() {
        roundtrip_request(Request::ListColumns);
        roundtrip_request(Request::RemoveColumn {
            key: "material".into(),
        });
        for value_type in [
            ValueType::Str,
            ValueType::F64,
            ValueType::I64,
            ValueType::Bool,
        ] {
            roundtrip_request(Request::AddColumn {
                key: "material".into(),
                value_type,
            });
        }
    }

    #[test]
    fn column_responses_round_trip() {
        roundtrip_response(Response::Columns(vec![]));
        roundtrip_response(Response::Columns(vec![
            Column {
                key: "hardness".into(),
                value_type: ValueType::F64,
            },
            Column {
                key: "material".into(),
                value_type: ValueType::Str,
            },
            Column {
                key: "visible".into(),
                value_type: ValueType::Bool,
            },
        ]));
        roundtrip_response(Response::Conflict(
            "column 'material' already exists".into(),
        ));
    }

    #[test]
    fn decode_request_rejects_an_unknown_column_type_tag() {
        // AddColumn("k") with a type tag no `ValueType` uses.
        let payload = [0x0b, 0x01, 0x00, b'k', 0xFE];
        assert_eq!(
            decode_request(&payload),
            Err(DecodeError::UnknownValueTag(0xFE))
        );
    }

    #[test]
    fn decode_request_rejects_an_unknown_opcode() {
        assert_eq!(
            decode_request(&[0xFF]),
            Err(DecodeError::UnknownOpcode(0xFF))
        );
    }

    #[test]
    fn decode_response_rejects_an_unknown_status() {
        assert_eq!(
            decode_response(&[0xFF]),
            Err(DecodeError::UnknownStatus(0xFF))
        );
    }

    #[test]
    fn decode_rejects_a_truncated_frame() {
        // A Get opcode promising a coordinate, but with nothing after it.
        assert_eq!(decode_request(&[0x01]), Err(DecodeError::UnexpectedEof));
        // Cut off partway through an i32 coordinate component.
        assert_eq!(
            decode_request(&[0x01, 0x01, 0x00, 0x00]),
            Err(DecodeError::UnexpectedEof)
        );
    }

    #[test]
    fn decode_rejects_an_unknown_value_tag() {
        // Set with a coord/key that parse fine, then a bogus value tag.
        let mut payload = encode_request(&Request::Set {
            coord: vec![1],
            key: "k".into(),
            value: Value::I64(0),
        });
        let value_tag_pos = payload.len() - 9; // 1 tag byte + 8 value bytes for I64
        payload[value_tag_pos] = 0xFF;
        assert_eq!(
            decode_request(&payload),
            Err(DecodeError::UnknownValueTag(0xFF))
        );
    }

    #[test]
    fn decode_rejects_invalid_utf8_in_a_key() {
        let mut payload = encode_request(&Request::Get {
            coord: vec![],
            key: "k".into(),
        });
        let last = payload.len() - 1;
        payload[last] = 0xFF; // not valid utf-8 on its own
        assert_eq!(decode_request(&payload), Err(DecodeError::InvalidUtf8));
    }

    #[tokio::test]
    async fn write_frame_then_read_frame_round_trips_a_payload() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"hello").await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let got = read_frame(&mut cursor).await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"hello"[..]));
    }

    #[tokio::test]
    async fn read_frame_on_an_empty_stream_is_a_clean_close() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        assert_eq!(read_frame(&mut cursor).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_frame_on_a_truncated_length_prefix_is_an_error() {
        let mut cursor = std::io::Cursor::new(vec![0x01, 0x00]); // only 2 of 4 length bytes
        assert!(read_frame(&mut cursor).await.is_err());
    }

    #[tokio::test]
    async fn read_frame_rejects_a_length_prefix_over_the_limit() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_LEN + 1).to_le_bytes());
        let mut cursor = std::io::Cursor::new(buf);
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn write_frame_then_read_frame_round_trips_an_encoded_request() {
        let req = Request::Set {
            coord: vec![1, 2, 3],
            key: "material".into(),
            value: Value::Str("stone".into()),
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &encode_request(&req)).await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let payload = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(decode_request(&payload).unwrap(), req);
    }
}
