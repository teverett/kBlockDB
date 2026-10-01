package com.kblockdb.client;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The client-side half of kBlockDB's binary protocol wire format --
 * mirrors {@code kblockdbserver/src/wire.rs} exactly (frame layout, and
 * every request/response kind's byte shape) so this never drifts from
 * what the server actually speaks. This client only ever *encodes*
 * requests and *decodes* responses -- the other, server-side half of the
 * format (decoding requests, encoding responses) has no use here.
 *
 * <p>Package-private: {@link KBlockDBClient} is the public API built on
 * top of this.
 */
final class Wire {

    private Wire() {
    }

    /**
     * No frame's payload may claim to be larger than this -- matches
     * {@code kblockdbserver::wire::MAX_FRAME_LEN}.
     */
    static final long MAX_FRAME_LEN = 64L * 1024 * 1024;

    // --- Frame I/O ---
    //
    // Every message, either direction, is a length-prefixed frame:
    // [u32 LE payload_len][payload_len bytes].

    /** Writes one length-prefixed frame carrying {@code payload}. */
    static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        if (payload.length > MAX_FRAME_LEN) {
            throw new ProtocolException(
                    "payload of " + payload.length + " bytes exceeds the " + MAX_FRAME_LEN + "-byte frame limit");
        }
        writeU32(out, payload.length);
        out.write(payload);
        out.flush();
    }

    /**
     * Reads one length-prefixed frame's payload, or {@code null} if the
     * peer closed the connection cleanly, right at a frame boundary --
     * distinct from an {@link IOException}, which means it closed (or
     * errored) mid-frame, or claimed a payload larger than
     * {@link #MAX_FRAME_LEN}.
     */
    static byte[] readFrame(InputStream in) throws IOException {
        int b0 = in.read();
        if (b0 == -1) {
            return null; // clean disconnect, right at a frame boundary
        }
        int b1 = readByteOrThrow(in);
        int b2 = readByteOrThrow(in);
        int b3 = readByteOrThrow(in);
        long len = (b0 & 0xFFL) | ((b1 & 0xFFL) << 8) | ((b2 & 0xFFL) << 16) | ((b3 & 0xFFL) << 24);
        if (len > MAX_FRAME_LEN) {
            throw new ProtocolException("frame of " + len + " bytes exceeds the " + MAX_FRAME_LEN + "-byte limit");
        }
        byte[] payload = new byte[(int) len];
        readFully(in, payload);
        return payload;
    }

    private static int readByteOrThrow(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new EOFException("connection closed mid-frame, while reading the length prefix");
        }
        return b;
    }

    private static void readFully(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n == -1) {
                throw new EOFException(
                        "connection closed mid-frame, after " + off + " of " + buf.length + " payload bytes");
            }
            off += n;
        }
    }

    // --- Little-endian primitives ---
    //
    // java.io's Data{Input,Output}Stream are big-endian only, so every
    // multi-byte field below is written/read by hand, least-significant
    // byte first, to match the wire format exactly.

    static void writeU16(OutputStream out, int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    static void writeU32(OutputStream out, long v) throws IOException {
        out.write((int) (v & 0xFF));
        out.write((int) ((v >>> 8) & 0xFF));
        out.write((int) ((v >>> 16) & 0xFF));
        out.write((int) ((v >>> 24) & 0xFF));
    }

    private static void writeI64(OutputStream out, long v) throws IOException {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    private static void writeF64(OutputStream out, double v) throws IOException {
        writeI64(out, Double.doubleToLongBits(v));
    }

    /** {@code Hello}'s username/password fields: {@code [u8 len][len bytes]}. */
    private static void writeShortString(OutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 0xFF) {
            throw new IllegalArgumentException(
                    "'" + s + "' is " + bytes.length + " bytes, over the 255-byte limit for a Hello field");
        }
        out.write(bytes.length);
        out.write(bytes);
    }

    /** {@code <key>}: {@code [u16 LE key_len][key bytes]}. */
    private static void writeKey(OutputStream out, String key) throws IOException {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 0xFFFF) {
            throw new IllegalArgumentException("key is " + bytes.length + " bytes, over the 65535-byte limit");
        }
        writeU16(out, bytes.length);
        out.write(bytes);
    }

    /**
     * {@code <coord>}: {@code [u8 axes][axes * i32 LE]} -- signed, since
     * kBlockDB's coordinate space is zero-centered (see
     * {@code kblockdblib::World}'s doc comment on its axis bounds).
     * {@link #writeU32} is reused here rather than duplicated: Java widens
     * {@code int} to {@code long} by sign-extension, but {@code writeU32}
     * only ever reads the bottom 32 bits back out, so it already emits the
     * correct little-endian two's-complement bytes for a negative {@code
     * c} with no change needed.
     */
    private static void writeCoord(OutputStream out, int[] coord) throws IOException {
        if (coord.length > 0xFF) {
            throw new IllegalArgumentException(
                    "coordinate has " + coord.length + " axes, over the 255-axis limit");
        }
        out.write(coord.length);
        for (int c : coord) {
            writeU32(out, c);
        }
    }

    /**
     * {@code <value>}: {@code [u8 type_tag]} then {@code [8 bytes LE]}
     * for F64/I64, {@code [u32 LE len][len bytes]} for Str, or
     * {@code [1 byte]} (0 or 1) for Bool. Tags match
     * {@code kblockdblib::Value::TAG_*} (0=Str, 1=F64, 2=I64, 3=Bool).
     */
    static void writeValue(OutputStream out, Value value) throws IOException {
        if (value instanceof Value.Str s) {
            byte[] bytes = s.value().getBytes(StandardCharsets.UTF_8);
            out.write(0);
            writeU32(out, bytes.length);
            out.write(bytes);
        } else if (value instanceof Value.F64 f) {
            out.write(1);
            writeF64(out, f.value());
        } else if (value instanceof Value.I64 i) {
            out.write(2);
            writeI64(out, i.value());
        } else if (value instanceof Value.Bool b) {
            out.write(3);
            out.write(b.value() ? 1 : 0);
        } else {
            throw new IllegalArgumentException("unknown Value subtype: " + value.getClass());
        }
    }

    // --- Requests (client -> server) ---

    static byte[] encodeHello(String username, String password) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x00);
        writeShortString(buf, username);
        writeShortString(buf, password);
        return buf.toByteArray();
    }

    static byte[] encodeGet(int[] coord, String key) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x01);
        writeCoord(buf, coord);
        writeKey(buf, key);
        return buf.toByteArray();
    }

    static byte[] encodeSet(int[] coord, String key, Value value) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x02);
        writeCoord(buf, coord);
        writeKey(buf, key);
        writeValue(buf, value);
        return buf.toByteArray();
    }

    static byte[] encodeRemove(int[] coord, String key) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x03);
        writeCoord(buf, coord);
        writeKey(buf, key);
        return buf.toByteArray();
    }

    static byte[] encodeHealth() {
        return new byte[] {0x04};
    }

    static byte[] encodeStats() {
        return new byte[] {0x05};
    }

    static byte[] encodeGetRegion(int[] origin, int[] extent, String key) throws IOException {
        return encodeRegionRequest(0x06, origin, extent, key);
    }

    static byte[] encodeSetRegion(int[] origin, int[] extent, String key, List<Value> values)
            throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x07);
        writeCoord(buf, origin);
        writeCoord(buf, extent);
        writeKey(buf, key);
        writeU32(buf, values.size());
        for (Value value : values) {
            writeValue(buf, value);
        }
        return buf.toByteArray();
    }

    static byte[] encodeRemoveRegion(int[] origin, int[] extent, String key) throws IOException {
        return encodeRegionRequest(0x08, origin, extent, key);
    }

    private static byte[] encodeRegionRequest(int opcode, int[] origin, int[] extent, String key)
            throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(opcode);
        writeCoord(buf, origin);
        writeCoord(buf, extent);
        writeKey(buf, key);
        return buf.toByteArray();
    }

    static byte[] encodeListColumns() {
        return new byte[] {0x0A};
    }

    static byte[] encodeAddColumn(String key, ValueType valueType) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0B);
        writeKey(buf, key);
        buf.write(valueType.tag());
        return buf.toByteArray();
    }

    static byte[] encodeRemoveColumn(String key) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0C);
        writeKey(buf, key);
        return buf.toByteArray();
    }

    static byte[] encodeQuery(String query) throws IOException {
        byte[] bytes = query.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x09);
        writeU32(buf, bytes.length);
        buf.write(bytes);
        return buf.toByteArray();
    }

    // --- Responses (server -> client) ---

    sealed interface Response {
    }

    record HelloOk(int axes, long worldDim, boolean readOnly) implements Response {
    }

    record Ok() implements Response {
    }

    record ValueResp(Value value, CellMeta meta) implements Response {
    }

    record NotFound() implements Response {
    }

    record BadRequest(String message) implements Response {
    }

    record Unauthorized(String message) implements Response {
    }

    record Forbidden(String message) implements Response {
    }

    record Internal(String message) implements Response {
    }

    record HealthResp(Health health) implements Response {
    }

    record StatsResp(Stats stats) implements Response {
    }

    record RegionValues(List<Optional<Value>> values) implements Response {
        RegionValues {
            values = List.copyOf(values);
        }
    }

    record QueryResp(QueryResult result) implements Response {
    }

    record Columns(List<Column> columns) implements Response {
        Columns {
            columns = List.copyOf(columns);
        }
    }

    record Conflict(String message) implements Response {
    }

    static Response decodeResponse(byte[] payload) throws ProtocolException {
        Reader r = new Reader(payload);
        int status = r.u8();
        switch (status) {
            case 0x00:
                return new HelloOk(r.u8(), r.u32(), r.u8() != 0);
            case 0x01:
                return new Ok();
            case 0x02:
                return new ValueResp(r.value(), r.meta());
            case 0x03:
                return new NotFound();
            case 0x04:
                return new BadRequest(r.message());
            case 0x05:
                return new Unauthorized(r.message());
            case 0x06:
                return new Forbidden(r.message());
            case 0x07:
                return new Internal(r.message());
            case 0x08:
                return new HealthResp(new Health(r.u8(), r.u32(), r.u64()));
            case 0x09:
                return new StatsResp(new Stats(r.u64(), r.u64(), r.u64()));
            case 0x0A:
                return new RegionValues(r.regionValues());
            case 0x0B:
                return new QueryResp(r.queryResult());
            case 0x0C:
                return new Columns(r.columns());
            case 0x0D:
                return new Conflict(r.message());
            default:
                throw new ProtocolException("unknown status 0x" + Integer.toHexString(status));
        }
    }

    /**
     * A cursor over an in-memory frame payload -- every read here goes
     * through this instead of hand-tracking an index, so a truncated
     * field anywhere is always just a {@link ProtocolException}, never an
     * {@link ArrayIndexOutOfBoundsException}.
     */
    private static final class Reader {
        private final byte[] buf;
        private int pos;

        Reader(byte[] buf) {
            this.buf = buf;
        }

        int u8() throws ProtocolException {
            if (pos >= buf.length) {
                throw truncated();
            }
            return buf[pos++] & 0xFF;
        }

        int u16() throws ProtocolException {
            int lo = u8();
            int hi = u8();
            return lo | (hi << 8);
        }

        long u32() throws ProtocolException {
            long b0 = u8();
            long b1 = u8();
            long b2 = u8();
            long b3 = u8();
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }

        long i64() throws ProtocolException {
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v |= ((long) u8()) << (8 * i);
            }
            return v;
        }

        double f64() throws ProtocolException {
            return Double.longBitsToDouble(i64());
        }

        /**
         * Same 8-byte little-endian layout as {@link #i64()} -- Java has
         * no unsigned 64-bit type, and the bit pattern kBlockDB's
         * {@code u64} fields (timestamps, {@code version}) use fits
         * comfortably, and always positively, in a {@code long}.
         */
        long u64() throws ProtocolException {
            return i64();
        }

        byte[] bytes(int n) throws ProtocolException {
            if (n < 0 || pos > buf.length - n) {
                throw truncated();
            }
            byte[] out = new byte[n];
            System.arraycopy(buf, pos, out, 0, n);
            pos += n;
            return out;
        }

        String string(int len) throws ProtocolException {
            return new String(bytes(len), StandardCharsets.UTF_8);
        }

        String message() throws ProtocolException {
            return string(u16()); // identical shape -- u16 len prefix, utf8 bytes
        }

        Value value() throws ProtocolException {
            int tag = u8();
            switch (tag) {
                case 0:
                    return new Value.Str(string(count32()));
                case 1:
                    return new Value.F64(f64());
                case 2:
                    return new Value.I64(i64());
                case 3:
                    return new Value.Bool(u8() != 0);
                default:
                    throw new ProtocolException("unknown value type tag 0x" + Integer.toHexString(tag));
            }
        }

        /** {@code <meta>}: {@code [u64 LE created_at_ms][u64 LE modified_at_ms][u64 LE version]}. */
        CellMeta meta() throws ProtocolException {
            return new CellMeta(u64(), u64(), u64());
        }

        int count32() throws ProtocolException {
            long count = u32();
            if (count > Integer.MAX_VALUE) {
                throw new ProtocolException("count exceeds Java's supported collection size: " + count);
            }
            return (int) count;
        }

        List<Integer> coord() throws ProtocolException {
            int axes = u8();
            List<Integer> coord = new ArrayList<>(axes);
            for (int i = 0; i < axes; i++) {
                coord.add((int) u32());
            }
            return coord;
        }

        List<Optional<Value>> regionValues() throws ProtocolException {
            int count = count32();
            List<Optional<Value>> values = new ArrayList<>(Math.min(count, remaining()));
            for (int i = 0; i < count; i++) {
                values.add(u8() == 0 ? Optional.empty() : Optional.of(value()));
            }
            return values;
        }

        List<Column> columns() throws ProtocolException {
            int count = count32();
            List<Column> columns = new ArrayList<>(Math.min(count, remaining()));
            for (int i = 0; i < count; i++) {
                columns.add(new Column(string(u16()), valueType()));
            }
            return columns;
        }

        ValueType valueType() throws ProtocolException {
            int tag = u8();
            ValueType type = ValueType.fromTag(tag);
            if (type == null) {
                throw new ProtocolException("unknown value type tag 0x" + Integer.toHexString(tag));
            }
            return type;
        }

        QueryResult queryResult() throws ProtocolException {
            int kind = u8();
            if (kind == 1) {
                return new QueryResult.Affected(u64());
            }
            if (kind != 0) {
                throw new ProtocolException("unknown query result kind 0x" + Integer.toHexString(kind));
            }
            int rowCount = count32();
            List<QueryRow> rows = new ArrayList<>(Math.min(rowCount, remaining()));
            for (int i = 0; i < rowCount; i++) {
                List<Integer> coord = coord();
                int valueCount = count32();
                List<QueryValue> values = new ArrayList<>(Math.min(valueCount, remaining()));
                for (int j = 0; j < valueCount; j++) {
                    values.add(new QueryValue(string(u16()), value(), meta()));
                }
                rows.add(new QueryRow(coord, values));
            }
            return new QueryResult.Rows(rows);
        }

        int remaining() {
            return buf.length - pos;
        }

        private ProtocolException truncated() {
            return new ProtocolException("truncated frame");
        }
    }
}
