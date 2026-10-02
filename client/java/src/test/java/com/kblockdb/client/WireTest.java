package com.kblockdb.client;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link Wire}'s encoding to the exact byte layout documented in
 * {@code kblockdbserver/src/wire.rs}, and its response decoding against
 * every status/value-type kind that module's own server-side tests cover.
 */
class WireTest {

    // --- Frame I/O ---

    @Test
    void writeFrameThenReadFrameRoundTripsAPayload() throws IOException {
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        Wire.writeFrame(framed, "hello".getBytes(StandardCharsets.UTF_8));

        byte[] read = Wire.readFrame(new ByteArrayInputStream(framed.toByteArray()));
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), read);
    }

    @Test
    void readFrameOnAnEmptyStreamIsACleanClose() throws IOException {
        assertNull(Wire.readFrame(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    void readFrameOnATruncatedLengthPrefixIsAnError() {
        byte[] partial = {0x01, 0x00}; // only 2 of 4 length bytes
        assertThrows(IOException.class, () -> Wire.readFrame(new ByteArrayInputStream(partial)));
    }

    @Test
    void readFrameRejectsALengthPrefixOverTheLimit() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        Wire.writeU32(buf, Wire.MAX_FRAME_LEN + 1);
        byte[] bytes = buf.toByteArray();

        ProtocolException e = assertThrows(ProtocolException.class,
                () -> Wire.readFrame(new ByteArrayInputStream(bytes)));
        assertTrue(e.getMessage().contains("exceeds"), e.getMessage());
    }

    // --- Request encoding: pinned to the documented byte layout ---

    @Test
    void encodeHelloProducesTheDocumentedByteLayout() throws IOException {
        byte[] payload = Wire.encodeHello("admin", "hi", "mydb");
        byte[] expected = {
                0x00,
                5, 'a', 'd', 'm', 'i', 'n',
                2, 'h', 'i',
                4, 'm', 'y', 'd', 'b',
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeGetProducesTheDocumentedByteLayout() throws IOException {
        byte[] payload = Wire.encodeGet(new int[] {1, 2, 3}, "material");
        byte[] expected = {
                0x01,
                0x03, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // coord: 3 axes, LE u32 each
                8, 0, 'm', 'a', 't', 'e', 'r', 'i', 'a', 'l', // key: u16 LE len, utf8
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeGetOnAZeroAxisCoordinateIsLegal() throws IOException {
        byte[] payload = Wire.encodeGet(new int[0], "k");
        byte[] expected = {0x01, 0, 1, 0, 'k'};
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeRemoveProducesTheDocumentedByteLayout() throws IOException {
        byte[] payload = Wire.encodeRemove(new int[] {1, 2, 3}, "material");
        byte[] expected = {
                0x03,
                0x03, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
                8, 0, 'm', 'a', 't', 'e', 'r', 'i', 'a', 'l',
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeSetProducesTheDocumentedByteLayoutForAStringValue() throws IOException {
        byte[] payload = Wire.encodeSet(new int[] {1}, "k", new Value.Str("ab"));
        byte[] expected = {
                0x02,
                0x01, 1, 0, 0, 0, // coord = [1]
                1, 0, 'k', // key = "k"
                0, // TAG_STR
                2, 0, 0, 0, 'a', 'b', // u32 LE len, then utf8 bytes
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeSetProducesTheDocumentedByteLayoutForAnI64Value() throws IOException {
        byte[] payload = Wire.encodeSet(new int[] {1}, "k", new Value.I64(-7));
        byte[] expected = {
                0x02,
                0x01, 1, 0, 0, 0,
                1, 0, 'k',
                2, // TAG_I64
                (byte) 0xF9, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, // -7, LE i64
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeSetProducesTheDocumentedByteLayoutForAnF64Value() throws IOException {
        byte[] payload = Wire.encodeSet(new int[] {1}, "k", new Value.F64(2.5));
        long bits = Double.doubleToLongBits(2.5);
        byte[] expected = {
                0x02,
                0x01, 1, 0, 0, 0,
                1, 0, 'k',
                1, // TAG_F64
                (byte) bits, (byte) (bits >>> 8), (byte) (bits >>> 16), (byte) (bits >>> 24),
                (byte) (bits >>> 32), (byte) (bits >>> 40), (byte) (bits >>> 48), (byte) (bits >>> 56),
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeSetProducesTheDocumentedByteLayoutForABoolValue() throws IOException {
        byte[] payload = Wire.encodeSet(new int[] {1}, "k", new Value.Bool(true));
        byte[] expected = {
                0x02,
                0x01, 1, 0, 0, 0,
                1, 0, 'k',
                3, // TAG_BOOL
                1,
        };
        assertArrayEquals(expected, payload);

        byte[] falsePayload = Wire.encodeSet(new int[] {1}, "k", new Value.Bool(false));
        byte[] falseExpected = {
                0x02,
                0x01, 1, 0, 0, 0,
                1, 0, 'k',
                3,
                0,
        };
        assertArrayEquals(falseExpected, falsePayload);
    }

    @Test
    void encodeRejectsAKeyOverTheLengthLimit() {
        String tooLong = "k".repeat(0x10000);
        assertThrows(IllegalArgumentException.class, () -> Wire.encodeGet(new int[] {1}, tooLong));
    }

    @Test
    void encodeRejectsMoreThan255Axes() {
        int[] tooMany = new int[256];
        assertThrows(IllegalArgumentException.class, () -> Wire.encodeGet(tooMany, "k"));
    }

    @Test
    void encodeGetProducesTheDocumentedByteLayoutForANegativeCoordinate() throws IOException {
        // kBlockDB's coordinate space is zero-centered, so a negative
        // component is ordinary, not an error -- encoded as its plain i32
        // little-endian two's-complement bytes, same as any other int.
        byte[] payload = Wire.encodeGet(new int[] {-1, -2147483648, 3}, "k");
        byte[] expected = {
                0x01,
                0x03,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, // -1
                0x00, 0x00, 0x00, (byte) 0x80, // Integer.MIN_VALUE
                3, 0, 0, 0, // 3
                1, 0, 'k',
        };
        assertArrayEquals(expected, payload);
    }

    @Test
    void encodeHelloRejectsAUsernameOverTheLengthLimit() {
        String tooLong = "u".repeat(256);
        assertThrows(IllegalArgumentException.class, () -> Wire.encodeHello(tooLong, "pw", "db"));
    }

    @Test
    void encodesDatabaseManagementRequests() throws IOException {
        assertArrayEquals(new byte[] {0x0D}, Wire.encodeListDatabases());
        assertArrayEquals(
                new byte[] {0x0E, 2, 0, 'd', 'b'},
                Wire.encodeCreateDatabase("db"));
        assertArrayEquals(
                new byte[] {0x0F, 2, 0, 'd', 'b'},
                Wire.encodeRemoveDatabase("db"));
    }

    @Test
    void encodesHealthAndStatsRequests() {
        assertArrayEquals(new byte[] {0x04}, Wire.encodeHealth());
        assertArrayEquals(new byte[] {0x05}, Wire.encodeStats());
    }

    @Test
    void encodesRegionRequests() throws IOException {
        assertArrayEquals(
                new byte[] {0x06, 1, 1, 0, 0, 0, 1, 2, 0, 0, 0, 1, 0, 'k'},
                Wire.encodeGetRegion(new int[] {1}, new int[] {2}, "k"));
        assertArrayEquals(
                new byte[] {0x08, 1, 1, 0, 0, 0, 1, 2, 0, 0, 0, 1, 0, 'k'},
                Wire.encodeRemoveRegion(new int[] {1}, new int[] {2}, "k"));
    }

    @Test
    void encodesSetRegionRequest() throws IOException {
        assertArrayEquals(
                new byte[] {
                    0x07, 1, 1, 0, 0, 0, 1, 2, 0, 0, 0, 1, 0, 'k',
                    2, 0, 0, 0, 2, 7, 0, 0, 0, 0, 0, 0, 0, 3, 1
                },
                Wire.encodeSetRegion(
                        new int[] {1}, new int[] {2}, "k", List.of(new Value.I64(7), new Value.Bool(true))));
    }

    @Test
    void encodesQueryRequest() throws IOException {
        assertArrayEquals(
                new byte[] {0x09, 8, 0, 0, 0, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '*'},
                Wire.encodeQuery("SELECT *"));
    }

    @Test
    void encodesColumnRequests() throws IOException {
        assertArrayEquals(new byte[] {0x0A}, Wire.encodeListColumns());
        assertArrayEquals(
                new byte[] {0x0B, 1, 0, 'k', 1},
                Wire.encodeAddColumn("k", ValueType.F64));
        assertArrayEquals(new byte[] {0x0C, 1, 0, 'k'}, Wire.encodeRemoveColumn("k"));
    }

    @Test
    void encodesAddColumnWithEveryValueTypeTag() throws IOException {
        for (ValueType valueType : ValueType.values()) {
            byte[] encoded = Wire.encodeAddColumn("k", valueType);
            assertEquals(valueType.tag(), encoded[encoded.length - 1]);
        }
    }

    @Test
    void valueTypeOfMatchesEachValuesWireTag() throws IOException {
        for (Value value : List.of(
                new Value.Str("stone"), new Value.F64(2.6), new Value.I64(7), new Value.Bool(true))) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            Wire.writeValue(buf, value);
            assertEquals(ValueType.of(value).tag(), buf.toByteArray()[0]);
        }
    }

    // --- Response decoding ---

    @Test
    void decodesHelloOk() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x00);
        buf.write(0); // read_only = false
        buf.write(1); // database_selected = true
        buf.write(3);
        Wire.writeU32(buf, 10_000);
        Wire.writeU32(buf, 32);

        Wire.Response response = Wire.decodeResponse(buf.toByteArray());
        assertEquals(
                new Wire.HelloOk(false, Optional.of(new Wire.DatabaseShape(3, 10_000L, 32L))),
                response);
    }

    @Test
    void decodesHelloOkReportingAReadOnlyAccount() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x00);
        buf.write(1); // read_only = true
        buf.write(1); // database_selected = true
        buf.write(4);
        Wire.writeU32(buf, 1);
        Wire.writeU32(buf, 1);

        Wire.Response response = Wire.decodeResponse(buf.toByteArray());
        assertEquals(
                new Wire.HelloOk(true, Optional.of(new Wire.DatabaseShape(4, 1L, 1L))),
                response);
    }

    @Test
    void decodesHelloOkWithNoDatabaseSelected() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x00);
        buf.write(0); // read_only = false
        buf.write(0); // database_selected = false -- no shape fields follow

        Wire.Response response = Wire.decodeResponse(buf.toByteArray());
        assertEquals(new Wire.HelloOk(false, Optional.empty()), response);
    }

    @Test
    void decodesOkAndNotFound() throws IOException {
        assertEquals(new Wire.Ok(), Wire.decodeResponse(new byte[] {0x01}));
        assertEquals(new Wire.NotFound(), Wire.decodeResponse(new byte[] {0x03}));
    }

    @Test
    void decodesValueResponseForEveryValueType() throws IOException {
        CellMeta meta = new CellMeta(1000, 2000, 3);
        assertEquals(new Wire.ValueResp(new Value.Str("air"), meta), decodeValueResponse(new Value.Str("air"), meta));
        assertEquals(new Wire.ValueResp(new Value.F64(1.5), meta), decodeValueResponse(new Value.F64(1.5), meta));
        assertEquals(new Wire.ValueResp(new Value.I64(0), meta), decodeValueResponse(new Value.I64(0), meta));
        assertEquals(
                new Wire.ValueResp(new Value.Bool(true), meta), decodeValueResponse(new Value.Bool(true), meta));
    }

    private static Wire.Response decodeValueResponse(Value value, CellMeta meta) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x02);
        Wire.writeValue(buf, value);
        writeU64(buf, meta.createdAtMs());
        writeU64(buf, meta.modifiedAtMs());
        writeU64(buf, meta.version());
        return Wire.decodeResponse(buf.toByteArray());
    }

    /** Test-only: production code (see {@link Wire}) never encodes a {@code <meta>} -- only decodes one. */
    private static void writeU64(ByteArrayOutputStream buf, long v) {
        for (int i = 0; i < 8; i++) {
            buf.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    @Test
    void decodesEveryErrorKindWithItsMessage() throws IOException {
        assertEquals(new Wire.BadRequest("bad coord"), decodeErrorResponse(0x04, "bad coord"));
        assertEquals(new Wire.Unauthorized("nope"), decodeErrorResponse(0x05, "nope"));
        assertEquals(new Wire.Forbidden("read-only"), decodeErrorResponse(0x06, "read-only"));
        assertEquals(new Wire.Internal("disk on fire"), decodeErrorResponse(0x07, "disk on fire"));
    }

    @Test
    void decodesHealthResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x08);
        writeU64(buf, 123);
        Wire.writeU32(buf, 2);
        byte[] hostname = "db-1.example.com".getBytes(StandardCharsets.UTF_8);
        Wire.writeU16(buf, hostname.length);
        buf.write(hostname);
        assertEquals(
                new Wire.HealthResp(new Health("db-1.example.com", 2, 123)),
                Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodingATruncatedHealthResponseIsAProtocolException() throws IOException {
        // Everything but the hostname -- a client built against the
        // pre-hostname format would stop exactly here.
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x08);
        writeU64(buf, 123);
        Wire.writeU32(buf, 2);
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesStatsResponse() throws ProtocolException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x09);
        writeU64(buf, 2);
        writeU64(buf, 3);
        writeU64(buf, 4);
        assertEquals(new Wire.StatsResp(new Stats(2, 3, 4)), Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesRegionValuesResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0A);
        Wire.writeU32(buf, 2);
        buf.write(1);
        Wire.writeValue(buf, new Value.Str("stone"));
        buf.write(0);
        assertEquals(
                new Wire.RegionValues(List.of(Optional.of(new Value.Str("stone")), Optional.empty())),
                Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesSelectQueryResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0B);
        buf.write(0);
        Wire.writeU32(buf, 1);
        buf.write(1);
        Wire.writeU32(buf, 7);
        Wire.writeU32(buf, 1);
        Wire.writeU16(buf, 1);
        buf.write('k');
        Wire.writeValue(buf, new Value.I64(9));
        writeU64(buf, 10);
        writeU64(buf, 11);
        writeU64(buf, 1);
        QueryResult expected = new QueryResult.Rows(List.of(
                new QueryRow(List.of(7), List.of(
                        new QueryValue("k", new Value.I64(9), new CellMeta(10, 11, 1))))));
        assertEquals(new Wire.QueryResp(expected), Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesMutatingQueryResponse() throws ProtocolException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0B);
        buf.write(1);
        writeU64(buf, 7);
        assertEquals(
                new Wire.QueryResp(new QueryResult.Affected(7)),
                Wire.decodeResponse(buf.toByteArray()));
    }

    private static Wire.Response decodeErrorResponse(int status, String message) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(status);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        Wire.writeU16(buf, bytes.length);
        buf.write(bytes);
        return Wire.decodeResponse(buf.toByteArray());
    }

    @Test
    void decodeResponseRejectsAnUnknownStatus() {
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> Wire.decodeResponse(new byte[] {(byte) 0xFF}));
        assertTrue(e.getMessage().contains("0xff"), e.getMessage());
    }

    @Test
    void decodeResponseRejectsAnUnknownValueTag() {
        byte[] payload = {0x02, (byte) 0xFF};
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(payload));
    }

    @Test
    void decodeResponseRejectsATruncatedPayload() {
        // HelloOk promising a selected database's shape, but cut off after
        // read_only/database_selected.
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(new byte[] {0x00, 0, 1}));
    }

    @Test
    void decodeResponseRejectsAnEmptyPayload() {
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(new byte[0]));
    }

    @Test
    void collectionCountsCannotForceAllocationBeyondTheFrame() {
        byte[] region = {0x0A, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F};
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(region));

        byte[] queryRows = {0x0B, 0, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F};
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(queryRows));
    }

    @Test
    void decodesColumnsResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0C);
        Wire.writeU32(buf, 2);
        Wire.writeU16(buf, 8);
        buf.write("hardness".getBytes(StandardCharsets.UTF_8));
        buf.write(1); // f64
        Wire.writeU16(buf, 8);
        buf.write("material".getBytes(StandardCharsets.UTF_8));
        buf.write(0); // str

        Wire.Response response = Wire.decodeResponse(buf.toByteArray());
        assertEquals(
                new Wire.Columns(List.of(
                        new Column("hardness", ValueType.F64), new Column("material", ValueType.STR))),
                response);
    }

    @Test
    void decodesAnEmptyColumnsResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0C);
        Wire.writeU32(buf, 0);
        assertEquals(new Wire.Columns(List.of()), Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesConflictResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0D);
        byte[] message = "already exists".getBytes(StandardCharsets.UTF_8);
        Wire.writeU16(buf, message.length);
        buf.write(message);
        assertEquals(new Wire.Conflict("already exists"), Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesDatabasesResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0E);
        Wire.writeU32(buf, 2);
        byte[] a = "a".getBytes(StandardCharsets.UTF_8);
        Wire.writeU16(buf, a.length);
        buf.write(a);
        byte[] b = "b".getBytes(StandardCharsets.UTF_8);
        Wire.writeU16(buf, b.length);
        buf.write(b);
        assertEquals(new Wire.Databases(List.of("a", "b")), Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodesAnEmptyDatabasesResponse() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0E);
        Wire.writeU32(buf, 0);
        assertEquals(new Wire.Databases(List.of()), Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodingAColumnWithAnUnknownTypeTagIsAProtocolException() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0C);
        Wire.writeU32(buf, 1);
        Wire.writeU16(buf, 1);
        buf.write('k');
        buf.write(0xFE);
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void decodingATruncatedColumnsResponseIsAProtocolException() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x0C);
        Wire.writeU32(buf, 2); // claims two columns, carries none
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(buf.toByteArray()));
    }

    @Test
    void valueTypeRoundTripsThroughItsTag() {
        for (ValueType valueType : ValueType.values()) {
            assertEquals(valueType, ValueType.fromTag(valueType.tag()));
        }
        assertNull(ValueType.fromTag(0xFE));
    }
}
