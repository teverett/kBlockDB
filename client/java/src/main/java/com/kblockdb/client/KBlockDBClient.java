package com.kblockdb.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.Optional;

/**
 * A client for kBlockDB's binary protocol -- a peer to the REST API, not a
 * replacement for it, with less per-call overhead (see
 * {@code kblockdbserver/src/wire.rs} and the project README's "Binary
 * protocol" section for the wire format and the protocol's semantics).
 *
 * <p>Each instance owns one TCP connection outright. Authentication is
 * per-<em>connection</em>, not per-request the way HTTP Basic Auth is:
 * {@link #connect} sends one {@code Hello} right after connecting, and
 * every request after that on this connection is treated as that account
 * until the connection closes, or {@link #reauthenticate} re-authenticates
 * as someone else on the same connection. One request, one response,
 * strictly in order -- this minimal protocol doesn't pipeline multiple
 * in-flight requests on one connection, so a caller wanting more
 * throughput than one connection's round-trip latency allows should open
 * more connections (one {@code KBlockDBClient} each), the same way it
 * would against the REST API.
 *
 * <p><b>Not thread-safe.</b> A single instance must not be used
 * concurrently from more than one thread.
 *
 * <pre>{@code
 * try (KBlockDBClient client = KBlockDBClient.connect("localhost", 8081, "admin", "change-me")) {
 *     client.set(new int[] {1, 2, 3}, "material", new Value.Str("stone"));
 *     Optional<Value> v = client.get(new int[] {1, 2, 3}, "material");
 * }
 * }</pre>
 */
public final class KBlockDBClient implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private int axes;
    private long worldDim;
    private boolean readOnly;

    private KBlockDBClient(Socket socket, InputStream in, OutputStream out, Wire.HelloOk hello) {
        this.socket = socket;
        this.in = in;
        this.out = out;
        applyHello(hello);
    }

    private void applyHello(Wire.HelloOk hello) {
        this.axes = hello.axes();
        this.worldDim = hello.worldDim();
        this.readOnly = hello.readOnly();
    }

    /**
     * Connects to {@code host:port} and authenticates as
     * {@code username}/{@code password} in one step -- there's no useful
     * "connected but not authenticated" state to hand back separately,
     * since every other request needs an authenticated connection anyway.
     *
     * @throws UnauthorizedException if the credentials are rejected
     * @throws IOException           on any connection or framing failure
     */
    public static KBlockDBClient connect(String host, int port, String username, String password)
            throws IOException {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");

        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
            // Without this, Nagle's algorithm can measurably delay every
            // small request/response this protocol sends -- exactly the
            // per-call overhead this protocol exists to avoid.
            socket.setTcpNoDelay(true);
            InputStream in = new BufferedInputStream(socket.getInputStream());
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());

            Wire.writeFrame(out, Wire.encodeHello(username, password));
            Wire.Response response = Wire.decodeResponse(requireFrame(in));
            if (response instanceof Wire.HelloOk hello) {
                return new KBlockDBClient(socket, in, out, hello);
            }
            throw toException(response);
        } catch (IOException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    /** The world's axis count, as reported by the most recent {@code Hello}. */
    public int axes() {
        return axes;
    }

    /** The world's cells-per-axis extent, as reported by the most recent {@code Hello}. */
    public long worldDim() {
        return worldDim;
    }

    /** Whether the currently authenticated account is read-only. */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Re-authenticates this same connection as {@code username}/
     * {@code password} -- every request after this one is treated as the
     * new account. Allowed, not required; most callers only ever
     * authenticate once, at {@link #connect}.
     *
     * @throws UnauthorizedException if the credentials are rejected
     */
    public void reauthenticate(String username, String password) throws IOException {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        Wire.writeFrame(out, Wire.encodeHello(username, password));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.HelloOk hello) {
            applyHello(hello);
            return;
        }
        throw toException(response);
    }

    /**
     * Reads the value at {@code (coord, key)}, or {@link Optional#empty()}
     * if nothing is set there.
     *
     * @throws BadRequestException if {@code coord} doesn't fit this world
     *                             (wrong axis count, out of bounds, ...)
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public Optional<Value> get(int[] coord, String key) throws IOException {
        Objects.requireNonNull(coord, "coord");
        Objects.requireNonNull(key, "key");
        Wire.writeFrame(out, Wire.encodeGet(coord, key));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.ValueResp v) {
            return Optional.of(v.value());
        }
        if (response instanceof Wire.NotFound) {
            return Optional.empty();
        }
        throw toException(response);
    }

    /**
     * Writes {@code value} at {@code (coord, key)}, overwriting whatever
     * was there before.
     *
     * @throws BadRequestException if {@code coord} doesn't fit this world,
     *                             or {@code value}'s type doesn't match
     *                             {@code key}'s existing type elsewhere in
     *                             this world
     * @throws ForbiddenException  if this connection's account is read-only
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public void set(int[] coord, String key, Value value) throws IOException {
        Objects.requireNonNull(coord, "coord");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Wire.writeFrame(out, Wire.encodeSet(coord, key, value));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return;
        }
        throw toException(response);
    }

    /**
     * Clears the value at {@code (coord, key)}, if any -- a harmless
     * no-op if nothing was set there.
     *
     * @throws BadRequestException if {@code coord} doesn't fit this world
     * @throws ForbiddenException  if this connection's account is read-only
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public void remove(int[] coord, String key) throws IOException {
        Objects.requireNonNull(coord, "coord");
        Objects.requireNonNull(key, "key");
        Wire.writeFrame(out, Wire.encodeRemove(coord, key));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return;
        }
        throw toException(response);
    }

    /** Closes the underlying connection. Idempotent. */
    @Override
    public void close() throws IOException {
        socket.close();
    }

    private static byte[] requireFrame(InputStream in) throws IOException {
        byte[] payload = Wire.readFrame(in);
        if (payload == null) {
            throw new EOFException("the server closed the connection");
        }
        return payload;
    }

    private static KBlockDBException toException(Wire.Response response) {
        if (response instanceof Wire.BadRequest r) {
            return new BadRequestException(r.message());
        }
        if (response instanceof Wire.Unauthorized r) {
            return new UnauthorizedException(r.message());
        }
        if (response instanceof Wire.Forbidden r) {
            return new ForbiddenException(r.message());
        }
        if (response instanceof Wire.Internal r) {
            return new InternalErrorException(r.message());
        }
        return new ProtocolException("unexpected response: " + response);
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best-effort cleanup after a failed connect/handshake
        }
    }
}
