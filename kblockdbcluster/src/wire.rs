//! The peer-to-peer replication protocol's wire format: a server-to-server
//! protocol distinct from `kblockdbserver`'s end-user binary protocol
//! (different auth model, a push/streaming shape instead of
//! request/response, and no reason to make external clients care about
//! internal-only replication traffic) -- see docs/clustering.md at the
//! repository root for the feature as a whole.
//!
//! **Framing** is the same length-prefixed shape as `kblockdbserver`'s own
//! binary protocol (`[u32 LE payload_len][payload_len bytes: payload]`,
//! capped at [`MAX_FRAME_LEN`]), kept as its own copy here rather than a
//! shared dependency: this crate must not depend on `kblockdbserver` (the
//! embedding direction goes the other way -- see this crate's doc
//! comment), and there's nothing peer-specific about "a length-prefixed
//! frame" to make sharing it worth a dependency cycle.
//!
//! **Versioning** mirrors the binary protocol's `Hello`/`HelloOk` idiom
//! with its own separate [`PEER_PROTOCOL_VERSION`]: the connecting side's
//! `Hello` carries its version, cluster secret, and the port its own
//! peer listener is on (so the accepting side can dial back -- every
//! known peer is replicated to in both directions, see `peers.rs`); the
//! accepting side
//! checks the secret first (wrong secret is indistinguishable on the wire
//! from a version mismatch -- both just get `HelloRejected`, so a
//! would-be attacker learns nothing about *why* a guess failed) and
//! otherwise replies `HelloOk`.
//!
//! **Message shape.** Unlike a request/response protocol, this one is
//! push-only: after `Hello`/`HelloOk`, the connecting side just streams
//! `ChangeBatch` frames at its own pace, and the accepting side never
//! replies to them at all (no acks, no pipelining concerns -- see
//! `client`/`server`'s own doc comments on why no-backfill/best-effort
//! delivery is an accepted v1 trade-off).

use kblockdblib::Value;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// No frame's payload may claim to be larger than this -- bounds how much
/// a misbehaving or malicious peer can make this process try to allocate
/// from one length prefix. Same value as `kblockdbserver::wire`'s own
/// limit, for the same reason, but not shared (see this module's doc
/// comment).
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Bumped whenever this module's wire format changes in a way an older
/// peer couldn't decode. Mirrors `kblockdbserver::wire::PROTOCOL_VERSION`'s
/// reasoning, applied here to peer links instead of end-user ones.
pub const PEER_PROTOCOL_VERSION: u8 = 1;

/// `created`/`updated`/`version` as reported by the peer that originated
/// this write -- applied verbatim by the receiving side (see
/// `server::ReplicationSink`), not re-derived, so the whole cluster
/// converges on the exact same metadata for a given logical write.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeEntry {
    pub database: String,
    pub coord: Vec<i32>,
    pub key: String,
    pub op: ChangeOp,
    pub created_at_ms: u64,
    pub modified_at_ms: u64,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChangeOp {
    Set(Value),
    Remove,
}

#[derive(Debug, PartialEq)]
pub enum PeerMessage {
    Hello {
        secret: String,
        server_id: String,
        /// The port the connecting server's own peer listener is on --
        /// combined with the connection's source IP, the address the
        /// accepting side dials back to (see `peers::PeerSet`).
        peer_port: u16,
        protocol_version: u8,
    },
    HelloOk,
    HelloRejected(String),
    ChangeBatch(Vec<ChangeEntry>),
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidUtf8,
    UnknownTag(u8),
    UnknownValueTag(u8),
}

impl From<DecodeError> for io::Error {
    fn from(e: DecodeError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}"))
    }
}

pub fn encode(msg: &PeerMessage) -> Vec<u8> {
    let mut buf = Vec::new();
    match msg {
        PeerMessage::Hello {
            secret,
            server_id,
            peer_port,
            protocol_version,
        } => {
            buf.push(0);
            buf.push(*protocol_version);
            put_string(&mut buf, secret);
            put_string(&mut buf, server_id);
            buf.extend_from_slice(&peer_port.to_le_bytes());
        }
        PeerMessage::HelloOk => buf.push(1),
        PeerMessage::HelloRejected(reason) => {
            buf.push(2);
            put_string(&mut buf, reason);
        }
        PeerMessage::ChangeBatch(entries) => {
            buf.push(3);
            buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for entry in entries {
                put_string(&mut buf, &entry.database);
                put_coord(&mut buf, &entry.coord);
                put_string(&mut buf, &entry.key);
                match &entry.op {
                    ChangeOp::Set(value) => {
                        buf.push(0);
                        put_value(&mut buf, value);
                    }
                    ChangeOp::Remove => buf.push(1),
                }
                buf.extend_from_slice(&entry.created_at_ms.to_le_bytes());
                buf.extend_from_slice(&entry.modified_at_ms.to_le_bytes());
                buf.extend_from_slice(&entry.version.to_le_bytes());
            }
        }
    }
    buf
}

pub fn decode(payload: &[u8]) -> Result<PeerMessage, DecodeError> {
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    match r.u8()? {
        0 => Ok(PeerMessage::Hello {
            protocol_version: r.u8()?,
            secret: r.string()?,
            server_id: r.string()?,
            peer_port: r.u16()?,
        }),
        1 => Ok(PeerMessage::HelloOk),
        2 => Ok(PeerMessage::HelloRejected(r.string()?)),
        3 => {
            let count = r.u32()? as usize;
            let entries = (0..count)
                .map(|_| {
                    let database = r.string()?;
                    let coord = r.coord()?;
                    let key = r.string()?;
                    let op = match r.u8()? {
                        0 => ChangeOp::Set(r.value()?),
                        1 => ChangeOp::Remove,
                        other => return Err(DecodeError::UnknownTag(other)),
                    };
                    Ok(ChangeEntry {
                        database,
                        coord,
                        key,
                        op,
                        created_at_ms: r.u64()?,
                        modified_at_ms: r.u64()?,
                        version: r.u64()?,
                    })
                })
                .collect::<Result<Vec<_>, DecodeError>>()?;
            Ok(PeerMessage::ChangeBatch(entries))
        }
        other => Err(DecodeError::UnknownTag(other)),
    }
}

/// Reads one length-prefixed frame's payload off `r`. `Ok(None)` means the
/// peer closed the connection cleanly, right at a frame boundary -- same
/// "0 bytes then EOF ≠ 1-3 bytes then EOF" distinction
/// `kblockdbserver::wire::read_frame` makes, duplicated here rather than
/// shared (see this module's doc comment).
async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut len_bytes = [0u8; 4];
    let mut filled = 0;
    while filled < len_bytes.len() {
        let n = r.read(&mut len_bytes[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-frame, while reading the length prefix",
            ));
        }
        filled += n;
    }
    let len = u32::from_le_bytes(len_bytes);
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds the {MAX_FRAME_LEN}-byte limit"),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

/// Writes one length-prefixed frame carrying `payload` to `w`.
async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    if payload.len() as u64 > MAX_FRAME_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
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

/// Reads a `PeerMessage` off `stream` via `read_frame`, decoding it on
/// success -- `None` on a clean close, same as `read_frame` itself.
pub async fn read_message<R: AsyncRead + Unpin>(stream: &mut R) -> io::Result<Option<PeerMessage>> {
    let Some(payload) = read_frame(stream).await? else {
        return Ok(None);
    };
    Ok(Some(decode(&payload)?))
}

/// Encodes and writes `msg` to `stream` via `write_frame`.
pub async fn write_message<W: AsyncWrite + Unpin>(
    stream: &mut W,
    msg: &PeerMessage,
) -> io::Result<()> {
    write_frame(stream, &encode(msg)).await
}

fn put_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn put_coord(buf: &mut Vec<u8>, coord: &[i32]) {
    buf.push(coord.len() as u8);
    for &c in coord {
        buf.extend_from_slice(&c.to_le_bytes());
    }
}

fn put_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Str(s) => {
            buf.push(Value::TAG_STR);
            put_string(buf, s);
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

/// A cursor over an in-memory frame payload -- mirrors
/// `kblockdbserver::wire`'s own `Reader`, kept separate rather than shared
/// since the two protocols' decoded types are unrelated.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos + n;
        if end > self.buf.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }

    fn coord(&mut self) -> Result<Vec<i32>, DecodeError> {
        let axes = self.u8()? as usize;
        (0..axes).map(|_| self.i32()).collect()
    }

    fn value(&mut self) -> Result<Value, DecodeError> {
        match self.u8()? {
            Value::TAG_STR => Ok(Value::Str(self.string()?)),
            Value::TAG_F64 => Ok(Value::F64(self.f64()?)),
            Value::TAG_I64 => Ok(Value::I64(self.u64()? as i64)),
            Value::TAG_BOOL => Ok(Value::Bool(self.u8()? != 0)),
            other => Err(DecodeError::UnknownValueTag(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: PeerMessage) {
        let encoded = encode(&msg);
        assert_eq!(decode(&encoded).unwrap(), msg);
    }

    #[test]
    fn hello_round_trips() {
        roundtrip(PeerMessage::Hello {
            secret: "shh".to_string(),
            server_id: "node-a".to_string(),
            peer_port: 8082,
            protocol_version: PEER_PROTOCOL_VERSION,
        });
    }

    #[test]
    fn hello_ok_round_trips() {
        roundtrip(PeerMessage::HelloOk);
    }

    #[test]
    fn hello_rejected_round_trips_with_its_message() {
        roundtrip(PeerMessage::HelloRejected("bad secret".to_string()));
    }

    #[test]
    fn change_batch_round_trips_every_value_type_and_both_ops() {
        roundtrip(PeerMessage::ChangeBatch(vec![
            ChangeEntry {
                database: "demo".to_string(),
                coord: vec![1, -2, 3, 0],
                key: "material".to_string(),
                op: ChangeOp::Set(Value::Str("stone".to_string())),
                created_at_ms: 10,
                modified_at_ms: 20,
                version: 1,
            },
            ChangeEntry {
                database: "demo".to_string(),
                coord: vec![1, -2, 3, 0],
                key: "density".to_string(),
                op: ChangeOp::Set(Value::F64(2.6)),
                created_at_ms: 10,
                modified_at_ms: 10,
                version: 0,
            },
            ChangeEntry {
                database: "demo".to_string(),
                coord: vec![1, -2, 3, 0],
                key: "hardness".to_string(),
                op: ChangeOp::Set(Value::I64(7)),
                created_at_ms: 10,
                modified_at_ms: 10,
                version: 0,
            },
            ChangeEntry {
                database: "demo".to_string(),
                coord: vec![1, -2, 3, 0],
                key: "flammable".to_string(),
                op: ChangeOp::Set(Value::Bool(true)),
                created_at_ms: 10,
                modified_at_ms: 10,
                version: 0,
            },
            ChangeEntry {
                database: "demo".to_string(),
                coord: vec![1, -2, 3, 0],
                key: "material".to_string(),
                op: ChangeOp::Remove,
                created_at_ms: 0,
                modified_at_ms: 30,
                version: 0,
            },
        ]));
    }

    #[test]
    fn empty_change_batch_round_trips() {
        roundtrip(PeerMessage::ChangeBatch(vec![]));
    }

    #[test]
    fn decode_rejects_an_unknown_tag() {
        assert_eq!(decode(&[99]), Err(DecodeError::UnknownTag(99)));
    }

    #[test]
    fn decode_rejects_a_truncated_frame() {
        // Hello's tag + protocol_version, then nothing else.
        assert_eq!(decode(&[0, 0]), Err(DecodeError::UnexpectedEof));
    }

    #[test]
    fn decode_rejects_invalid_utf8_in_a_string() {
        let mut buf = vec![2u8]; // HelloRejected
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&[0xff, 0xfe]);
        assert_eq!(decode(&buf), Err(DecodeError::InvalidUtf8));
    }

    #[test]
    fn decode_rejects_an_unknown_value_tag() {
        let mut buf = vec![3u8]; // ChangeBatch
        buf.extend_from_slice(&1u32.to_le_bytes());
        put_string(&mut buf, "demo");
        put_coord(&mut buf, &[0, 0, 0]);
        put_string(&mut buf, "k");
        buf.push(0); // Set
        buf.push(99); // bogus value tag
        assert_eq!(decode(&buf), Err(DecodeError::UnknownValueTag(99)));
    }

    #[tokio::test]
    async fn write_frame_then_read_frame_round_trips_a_payload() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"hello").await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let payload = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(payload, b"hello");
    }

    #[tokio::test]
    async fn read_frame_on_an_empty_stream_is_a_clean_close() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        assert_eq!(read_frame(&mut cursor).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_frame_rejects_a_length_prefix_over_the_limit() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_LEN + 1).to_le_bytes());
        let mut cursor = std::io::Cursor::new(buf);
        assert!(read_frame(&mut cursor).await.is_err());
    }

    #[tokio::test]
    async fn write_frame_then_read_frame_round_trips_an_encoded_message() {
        let msg = PeerMessage::HelloRejected("nope".to_string());
        let mut buf: Vec<u8> = Vec::new();
        write_message(&mut buf, &msg).await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let decoded = read_message(&mut cursor).await.unwrap().unwrap();
        assert_eq!(decoded, msg);
    }
}
