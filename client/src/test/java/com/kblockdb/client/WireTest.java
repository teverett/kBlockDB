package com.kblockdb.client;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
        byte[] payload = Wire.encodeHello("admin", "hi");
        byte[] expected = {
                0x00,
                5, 'a', 'd', 'm', 'i', 'n',
                2, 'h', 'i',
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
    void encodeRejectsANegativeCoordinateComponent() {
        assertThrows(IllegalArgumentException.class, () -> Wire.encodeGet(new int[] {-1}, "k"));
    }

    @Test
    void encodeHelloRejectsAUsernameOverTheLengthLimit() {
        String tooLong = "u".repeat(256);
        assertThrows(IllegalArgumentException.class, () -> Wire.encodeHello(tooLong, "pw"));
    }

    // --- Response decoding ---

    @Test
    void decodesHelloOk() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x00);
        buf.write(3);
        Wire.writeU32(buf, 10_000);
        buf.write(0);

        Wire.Response response = Wire.decodeResponse(buf.toByteArray());
        assertEquals(new Wire.HelloOk(3, 10_000L, false), response);
    }

    @Test
    void decodesHelloOkReportingAReadOnlyAccount() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x00);
        buf.write(4);
        Wire.writeU32(buf, 1);
        buf.write(1);

        Wire.Response response = Wire.decodeResponse(buf.toByteArray());
        assertEquals(new Wire.HelloOk(4, 1L, true), response);
    }

    @Test
    void decodesOkAndNotFound() throws IOException {
        assertEquals(new Wire.Ok(), Wire.decodeResponse(new byte[] {0x01}));
        assertEquals(new Wire.NotFound(), Wire.decodeResponse(new byte[] {0x03}));
    }

    @Test
    void decodesValueResponseForEveryValueType() throws IOException {
        assertEquals(new Wire.ValueResp(new Value.Str("air")), decodeValueResponse(new Value.Str("air")));
        assertEquals(new Wire.ValueResp(new Value.F64(1.5)), decodeValueResponse(new Value.F64(1.5)));
        assertEquals(new Wire.ValueResp(new Value.I64(0)), decodeValueResponse(new Value.I64(0)));
    }

    private static Wire.Response decodeValueResponse(Value value) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(0x02);
        Wire.writeValue(buf, value);
        return Wire.decodeResponse(buf.toByteArray());
    }

    @Test
    void decodesEveryErrorKindWithItsMessage() throws IOException {
        assertEquals(new Wire.BadRequest("bad coord"), decodeErrorResponse(0x04, "bad coord"));
        assertEquals(new Wire.Unauthorized("nope"), decodeErrorResponse(0x05, "nope"));
        assertEquals(new Wire.Forbidden("read-only"), decodeErrorResponse(0x06, "read-only"));
        assertEquals(new Wire.Internal("disk on fire"), decodeErrorResponse(0x07, "disk on fire"));
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
        // HelloOk promising axes/world_dim/read_only, but cut off after axes.
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(new byte[] {0x00, 3}));
    }

    @Test
    void decodeResponseRejectsAnEmptyPayload() {
        assertThrows(ProtocolException.class, () -> Wire.decodeResponse(new byte[0]));
    }
}
