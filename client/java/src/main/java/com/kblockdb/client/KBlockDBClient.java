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
import java.util.List;
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
 * <p><b>Databases.</b> {@link #connect} authenticates an account *and*
 * selects one database for the connection's whole life -- every data
 * request after it (get/set/query/...) operates on whichever database was
 * selected. If the named database doesn't exist yet, the account still
 * authenticates (so {@link #createDatabase} can be called), but no database
 * is selected: {@link #databaseSelected()} is {@code false}, and any data
 * request throws {@link BadRequestException} until {@link #createDatabase}
 * succeeds and {@link #useDatabase} selects it.
 *
 * <pre>{@code
 * try (KBlockDBClient client = KBlockDBClient.connect("localhost", 8081, "admin", "change-me", "mydb")) {
 *     client.set(new int[] {1, 2, 3}, "material", new Value.Str("stone"));
 *     Optional<Value> v = client.get(new int[] {1, 2, 3}, "material");
 * }
 * }</pre>
 */
public final class KBlockDBClient implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private String username;
    private String password;
    private String database;
    private int axes;
    private long worldDim;
    private long chunkDim;
    private int serverVersion;
    private boolean readOnly;
    private boolean databaseSelected;

    private KBlockDBClient(
            Socket socket, InputStream in, OutputStream out, String username, String password, String database,
            Wire.HelloOk hello) {
        this.socket = socket;
        this.in = in;
        this.out = out;
        this.username = username;
        this.password = password;
        this.database = database;
        applyHello(hello);
    }

    private void applyHello(Wire.HelloOk hello) {
        this.serverVersion = hello.serverVersion();
        this.readOnly = hello.readOnly();
        this.databaseSelected = hello.database().isPresent();
        Wire.DatabaseShape shape = hello.database().orElse(null);
        this.axes = shape != null ? shape.axes() : 0;
        this.worldDim = shape != null ? shape.worldDim() : 0;
        this.chunkDim = shape != null ? shape.chunkDim() : 0;
    }

    /**
     * Connects to {@code host:port}, authenticates as {@code username}/
     * {@code password}, and selects {@code database} -- all in one step,
     * since every other request needs an authenticated connection anyway.
     * If {@code database} doesn't exist yet, the connection still comes
     * back successfully (the account is authenticated), but
     * {@link #databaseSelected()} is {@code false} -- see this class's own
     * doc comment for the bootstrap flow to create and then select it.
     *
     * @throws UnauthorizedException if the credentials are rejected
     * @throws IOException           on any connection or framing failure
     */
    public static KBlockDBClient connect(String host, int port, String username, String password, String database)
            throws IOException {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        Objects.requireNonNull(database, "database");

        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
            // Without this, Nagle's algorithm can measurably delay every
            // small request/response this protocol sends -- exactly the
            // per-call overhead this protocol exists to avoid.
            socket.setTcpNoDelay(true);
            InputStream in = new BufferedInputStream(socket.getInputStream());
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());

            Wire.writeFrame(out, Wire.encodeHello(username, password, database));
            Wire.Response response = Wire.decodeResponse(requireFrame(in));
            if (response instanceof Wire.HelloOk hello) {
                return new KBlockDBClient(socket, in, out, username, password, database, hello);
            }
            throw toException(response);
        } catch (IOException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    /**
     * This server's binary protocol version, as reported by the most
     * recent {@code Hello} -- unlike {@link #axes()}/{@link #worldDim()}/
     * {@link #chunkDim()}, meaningful whether or not a database is
     * currently selected, since it's server-wide, not per-database. A
     * client that knows about more than one protocol version can compare
     * this against its own to decide how to speak to an older (or newer)
     * server -- see {@code kblockdbserver::wire}'s "Versioning" doc
     * comment.
     */
    public int serverVersion() {
        return serverVersion;
    }

    /**
     * The selected database's axis count, as reported by the most recent
     * {@code Hello}. Meaningless (always 0) when {@link #databaseSelected()}
     * is {@code false}.
     */
    public int axes() {
        return axes;
    }

    /**
     * The selected database's cells-per-axis extent, as reported by the
     * most recent {@code Hello}. Meaningless (always 0) when
     * {@link #databaseSelected()} is {@code false}.
     */
    public long worldDim() {
        return worldDim;
    }

    /**
     * Cells per axis in one chunk file, for the selected database.
     * Meaningless (always 0) when {@link #databaseSelected()} is
     * {@code false}.
     */
    public long chunkDim() {
        return chunkDim;
    }

    /**
     * Whether this connection currently has a database selected -- see
     * this class's own doc comment. {@code false} right after connecting to
     * a database that doesn't exist yet.
     */
    public boolean databaseSelected() {
        return databaseSelected;
    }

    /** Whether the currently authenticated account is read-only. */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Re-authenticates this same connection as {@code username}/
     * {@code password}, keeping the currently selected (or not-yet-selected)
     * database name. Allowed, not required; most callers only ever
     * authenticate once, at {@link #connect}.
     *
     * @throws UnauthorizedException if the credentials are rejected
     */
    public void reauthenticate(String username, String password) throws IOException {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        Wire.writeFrame(out, Wire.encodeHello(username, password, database));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.HelloOk hello) {
            this.username = username;
            this.password = password;
            applyHello(hello);
            return;
        }
        throw toException(response);
    }

    /**
     * Re-sends {@code Hello} on this same connection with the credentials
     * already given to {@link #connect}, naming {@code name} as the
     * database to select instead. The ergonomic fix for the bootstrap
     * wrinkle described in this class's own doc comment: after
     * {@link #createDatabase} creates the database {@link #connect} named,
     * call {@code useDatabase} with that same name to actually select it,
     * without needing to open a new connection or remember the original
     * password yourself.
     *
     * @throws UnauthorizedException if the cached credentials are somehow
     *                               no longer valid
     */
    public void useDatabase(String name) throws IOException {
        Objects.requireNonNull(name, "name");
        Wire.writeFrame(out, Wire.encodeHello(username, password, name));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.HelloOk hello) {
            this.database = name;
            applyHello(hello);
            return;
        }
        throw toException(response);
    }

    /**
     * Reads the value at {@code (coord, key)}, or {@link Optional#empty()}
     * if nothing is set there. Equivalent to {@link #getWithMeta} with the
     * meta half discarded -- see that method if you also want when this
     * value was set and how many times it's been overwritten.
     *
     * @throws BadRequestException if {@code coord} doesn't fit this world
     *                             (wrong axis count, out of bounds, ...)
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public Optional<Value> get(int[] coord, String key) throws IOException {
        return getWithMeta(coord, key).map(ValueWithMeta::value);
    }

    /**
     * Reads the value at {@code (coord, key)} together with its
     * {@link CellMeta}, or {@link Optional#empty()} if nothing is set
     * there. Both come from the same response the server sends for a
     * {@code Get} -- there's no separate request for metadata, so this
     * costs nothing extra over {@link #get} beyond decoding a few more
     * bytes already in hand.
     *
     * @throws BadRequestException if {@code coord} doesn't fit this world
     *                             (wrong axis count, out of bounds, ...)
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public Optional<ValueWithMeta> getWithMeta(int[] coord, String key) throws IOException {
        Objects.requireNonNull(coord, "coord");
        Objects.requireNonNull(key, "key");
        Wire.writeFrame(out, Wire.encodeGet(coord, key));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.ValueResp v) {
            return Optional.of(new ValueWithMeta(v.value(), v.meta()));
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

    /** Returns the server's identity, database count, and clock. */
    public Health health() throws IOException {
        Wire.writeFrame(out, Wire.encodeHealth());
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.HealthResp health) {
            return health.health();
        }
        throw toException(response);
    }

    /** Returns live on-disk statistics for the world. */
    public Stats stats() throws IOException {
        Wire.writeFrame(out, Wire.encodeStats());
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.StatsResp stats) {
            return stats.stats();
        }
        throw toException(response);
    }

    /**
     * Every database this server currently manages, sorted by name. Unlike
     * most requests, this doesn't need a selected database -- it works even
     * when {@link #databaseSelected()} is {@code false}.
     */
    public List<String> listDatabases() throws IOException {
        Wire.writeFrame(out, Wire.encodeListDatabases());
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Databases databases) {
            return databases.names();
        }
        throw toException(response);
    }

    /**
     * Creates a new database named {@code name}, using the server's
     * configured default shape -- there's no way to override axes/
     * world_dim/chunk_size over this protocol (use the REST API's
     * {@code PUT /rest/databases/{name}} for that). Doesn't need a selected
     * database, so this works even when {@link #databaseSelected()} is
     * {@code false} -- the usual way to bootstrap a brand-new database (see
     * this class's own doc comment): create it, then call
     * {@link #useDatabase} to select it.
     *
     * @throws ConflictException     if a database named {@code name}
     *                               already exists
     * @throws ForbiddenException    if this connection's account is
     *                               read-only
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public void createDatabase(String name) throws IOException {
        Objects.requireNonNull(name, "name");
        Wire.writeFrame(out, Wire.encodeCreateDatabase(name));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return;
        }
        throw toException(response);
    }

    /**
     * Deletes database {@code name} -- its directory and every byte of data
     * in it. Returns {@code false}, having changed nothing, if there was no
     * such database. Doesn't need a selected database, same as
     * {@link #listDatabases}/{@link #createDatabase}.
     *
     * <p>Irreversible, same as {@link #removeColumn}.
     *
     * @throws ForbiddenException    if this connection's account is
     *                               read-only
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public boolean removeDatabase(String name) throws IOException {
        Objects.requireNonNull(name, "name");
        Wire.writeFrame(out, Wire.encodeRemoveDatabase(name));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return true;
        }
        if (response instanceof Wire.NotFound) {
            return false;
        }
        throw toException(response);
    }

    /**
     * Reads one value per cell in axis-0-fastest order. Empty entries are
     * cells where {@code key} is unset.
     */
    public List<Optional<Value>> getRegion(int[] origin, int[] extent, String key) throws IOException {
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(extent, "extent");
        Objects.requireNonNull(key, "key");
        Wire.writeFrame(out, Wire.encodeGetRegion(origin, extent, key));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.RegionValues values) {
            return values.values();
        }
        throw toException(response);
    }

    /**
     * Writes one value per cell in axis-0-fastest order. The value count
     * must exactly equal the region volume.
     */
    public void setRegion(int[] origin, int[] extent, String key, List<Value> values) throws IOException {
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(extent, "extent");
        Objects.requireNonNull(key, "key");
        List<Value> checkedValues = List.copyOf(Objects.requireNonNull(values, "values"));
        Wire.writeFrame(out, Wire.encodeSetRegion(origin, extent, key, checkedValues));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return;
        }
        throw toException(response);
    }

    /** Clears {@code key} from every cell in the region. */
    public void removeRegion(int[] origin, int[] extent, String key) throws IOException {
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(extent, "extent");
        Objects.requireNonNull(key, "key");
        Wire.writeFrame(out, Wire.encodeRemoveRegion(origin, extent, key));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return;
        }
        throw toException(response);
    }

    /**
     * Executes a kBlockDB {@code SELECT}, {@code SET}, {@code UPDATE}, or
     * {@code DELETE} statement.
     */
    public QueryResult query(String query) throws IOException {
        Objects.requireNonNull(query, "query");
        Wire.writeFrame(out, Wire.encodeQuery(query));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.QueryResp result) {
            return result.result();
        }
        throw toException(response);
    }

    /**
     * Every column in this world's schema, sorted by key -- including
     * columns created implicitly by a {@code set} rather than by
     * {@link #addColumn(String, ValueType)}.
     *
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public List<Column> columns() throws IOException {
        Wire.writeFrame(out, Wire.encodeListColumns());
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Columns columns) {
            return columns.columns();
        }
        throw toException(response);
    }

    /**
     * Creates a column for {@code key}, fixing it to {@code valueType}.
     * Only needed to declare a column's type up front -- writing a value
     * creates its column implicitly otherwise.
     *
     * @throws ConflictException     if {@code key} already has a column
     * @throws BadRequestException   if {@code key} is empty or contains a
     *                               tab or newline
     * @throws ForbiddenException    if this connection's account is read-only
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public void addColumn(String key, ValueType valueType) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valueType, "valueType");
        Wire.writeFrame(out, Wire.encodeAddColumn(key, valueType));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return;
        }
        throw toException(response);
    }

    /**
     * Drops {@code key}'s column and every value ever written for it,
     * across the whole world. Returns false if there was no such column.
     *
     * <p>Not reversible: re-creating the column later starts it empty,
     * and it may be given a different {@link ValueType} than it had.
     *
     * @throws ForbiddenException    if this connection's account is read-only
     * @throws UnauthorizedException if this connection hasn't authenticated
     */
    public boolean removeColumn(String key) throws IOException {
        Objects.requireNonNull(key, "key");
        Wire.writeFrame(out, Wire.encodeRemoveColumn(key));
        Wire.Response response = Wire.decodeResponse(requireFrame(in));
        if (response instanceof Wire.Ok) {
            return true;
        }
        if (response instanceof Wire.NotFound) {
            return false;
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
        if (response instanceof Wire.Conflict r) {
            return new ConflictException(r.message());
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
