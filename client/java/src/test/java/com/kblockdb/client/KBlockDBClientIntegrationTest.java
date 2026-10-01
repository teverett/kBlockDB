package com.kblockdb.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Spawns a real {@code kblockdbserver} process (the same way
 * {@code kblockdbperf}'s and {@code kblockdbcli}'s own Rust integration
 * tests do -- see {@code kblockdbperf/src/tests.rs}) and drives it over a
 * real TCP connection through {@link KBlockDBClient}, checking the whole
 * client against the real server rather than just {@link Wire}'s encoding
 * in isolation (see {@link WireTest} for that).
 *
 * <p>Skips (rather than fails) if the {@code kblockdbserver} binary hasn't
 * been built yet, since running this module's tests doesn't imply the
 * Rust workspace was built first.
 */
class KBlockDBClientIntegrationTest {

    private static final String ADMIN_PASSWORD = "kblockdb-java-client-test-password";

    private static Path dataDir;
    private static Path configPath;
    private static Process server;
    private static int binaryPort;

    @BeforeAll
    static void startServer() throws IOException, InterruptedException {
        Path binary = findKblockdbserverBinary();
        assumeTrue(binary != null,
                "kblockdbserver binary not found -- run `cargo build -p kblockdbserver` first");

        dataDir = Files.createTempDirectory("kblockdb-java-client-test-data-");
        configPath = Files.createTempFile("kblockdb-java-client-test-config-", ".toml");
        Files.writeString(configPath, "admin_password = \"" + ADMIN_PASSWORD + "\"\n");

        int httpPort = freePort();
        binaryPort = freePort();

        server = new ProcessBuilder(
                binary.toString(),
                "--data-dir", dataDir.toString(),
                "--http-addr", "127.0.0.1:" + httpPort,
                "--binary-addr", "127.0.0.1:" + binaryPort,
                "--config", configPath.toString())
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();

        waitUntilReady(binaryPort);
    }

    @AfterAll
    static void stopServer() throws InterruptedException, IOException {
        if (server != null) {
            server.destroy();
            if (!server.waitFor(5, TimeUnit.SECONDS)) {
                server.destroyForcibly();
            }
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
        if (configPath != null) {
            Files.deleteIfExists(configPath);
        }
    }

    @Test
    void connectReportsTheWorldsShape() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            assertEquals(3, client.axes());
            assertEquals(10_000L, client.worldDim());
            assertFalse(client.isReadOnly());
        }
    }

    @Test
    void connectWithTheWrongPasswordIsUnauthorized() {
        assertThrows(UnauthorizedException.class,
                () -> KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", "wrong"));
    }

    @Test
    void setThenGetThenRemoveRoundTripsThroughARealConnection() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            int[] coord = {1, 2, 3};

            client.set(coord, "material", new Value.Str("stone"));
            assertEquals(Optional.of(new Value.Str("stone")), client.get(coord, "material"));

            client.remove(coord, "material");
            assertEquals(Optional.empty(), client.get(coord, "material"));
        }
    }

    @Test
    void negativeCoordinatesRoundTripThroughARealConnection() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            int[] coord = {-1, -2, -3};

            client.set(coord, "material", new Value.Str("stone"));
            assertEquals(Optional.of(new Value.Str("stone")), client.get(coord, "material"));

            client.remove(coord, "material");
            assertEquals(Optional.empty(), client.get(coord, "material"));
        }
    }

    @Test
    void everyValueTypeRoundTrips() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            client.set(new int[] {5, 5, 5}, "str-key", new Value.Str("air"));
            client.set(new int[] {5, 5, 6}, "i64-key", new Value.I64(-42));
            client.set(new int[] {5, 5, 7}, "f64-key", new Value.F64(2.5));
            client.set(new int[] {5, 5, 8}, "bool-key", new Value.Bool(true));

            assertEquals(Optional.of(new Value.Str("air")), client.get(new int[] {5, 5, 5}, "str-key"));
            assertEquals(Optional.of(new Value.I64(-42)), client.get(new int[] {5, 5, 6}, "i64-key"));
            assertEquals(Optional.of(new Value.F64(2.5)), client.get(new int[] {5, 5, 7}, "f64-key"));
            assertEquals(Optional.of(new Value.Bool(true)), client.get(new int[] {5, 5, 8}, "bool-key"));
        }
    }

    @Test
    void getWithMetaOfAFreshSetReportsVersionZero() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            int[] coord = {2, 2, 2};
            client.set(coord, "material", new Value.Str("stone"));

            ValueWithMeta result = client.getWithMeta(coord, "material").orElseThrow();
            assertEquals(new Value.Str("stone"), result.value());
            assertEquals(0, result.meta().version());
            assertEquals(result.meta().createdAtMs(), result.meta().modifiedAtMs());
            assertTrue(result.meta().createdAtMs() > 0);
        }
    }

    @Test
    void getWithMetaReportsAnIncrementingVersionAndStableCreatedAt() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            int[] coord = {3, 3, 3};
            client.set(coord, "material", new Value.Str("stone"));
            ValueWithMeta first = client.getWithMeta(coord, "material").orElseThrow();

            client.set(coord, "material", new Value.Str("air"));
            ValueWithMeta second = client.getWithMeta(coord, "material").orElseThrow();

            assertEquals(new Value.Str("air"), second.value());
            assertEquals(1, second.meta().version());
            assertEquals(first.meta().createdAtMs(), second.meta().createdAtMs());
            assertTrue(second.meta().modifiedAtMs() >= first.meta().modifiedAtMs());
        }
    }

    @Test
    void getWithMetaOfANeverSetCellIsEmpty() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            assertEquals(Optional.empty(), client.getWithMeta(new int[] {9, 9, 9}, "never-set"));
        }
    }

    @Test
    void aWrongAxisCountCoordinateIsABadRequestNotAClosedConnection() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            assertThrows(BadRequestException.class, () -> client.get(new int[] {1, 2}, "material"));

            // The connection must still be usable after that.
            assertEquals(Optional.empty(), client.get(new int[] {9, 9, 9}, "never-set"));
        }
    }

    @Test
    void healthReportsTheWorldShapeAndServerTime() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            Health health = client.health();
            assertEquals(3, health.axes());
            assertEquals(10_000, health.worldDim());
            assertEquals(32, health.chunkDim());
            assertTrue(health.timestamp() > 0);
            // Which hostname the test machine has isn't knowable here;
            // that one was reported at all is.
            assertFalse(health.hostname().isEmpty());
        }
    }

    @Test
    void statsReportsPersistedData() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            client.set(new int[] {30, 30, 30}, "stats-key", new Value.I64(1));
            Stats stats = client.stats();
            assertTrue(stats.totalChunks() > 0);
            assertTrue(stats.totalBytes() > 0);
        }
    }

    @Test
    void regionApisSetGetAndRemoveValues() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            int[] origin = {40, 40, 40};
            int[] extent = {2, 1, 1};
            client.setRegion(origin, extent, "region-key", List.of(new Value.I64(1), new Value.I64(2)));
            assertEquals(
                    List.of(Optional.of(new Value.I64(1)), Optional.of(new Value.I64(2))),
                    client.getRegion(origin, extent, "region-key"));

            client.removeRegion(origin, extent, "region-key");
            assertEquals(
                    List.of(Optional.empty(), Optional.empty()),
                    client.getRegion(origin, extent, "region-key"));
        }
    }

    @Test
    void queryReturnsRowsAndAffectedCellCounts() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            assertEquals(
                    new QueryResult.Affected(2),
                    client.query("SET (query_key = 7) IN (50,50,50) TO (52,51,51)"));

            QueryResult.Rows rows = (QueryResult.Rows) client.query(
                    "SELECT query_key FROM (50,50,50) TO (52,51,51)");
            assertEquals(2, rows.totalRows());
            assertEquals(List.of(50, 50, 50), rows.rows().get(0).coord());
            assertEquals(new Value.I64(7), rows.rows().get(0).values().get(0).value());
        }
    }

    // --- Test server plumbing ---

    private static Path findKblockdbserverBinary() {
        // user.dir is this Maven module's own directory (client/java) --
        // the Cargo workspace root (where target/<profile>/ actually is)
        // is two levels up from there.
        Path repoRoot = Path.of(System.getProperty("user.dir"))
                .toAbsolutePath()
                .getParent()
                .getParent();
        for (String profile : List.of("debug", "release")) {
            for (String name : List.of("kblockdbserver", "kblockdbserver.exe")) {
                Path candidate = repoRoot.resolve("target").resolve(profile).resolve(name);
                if (Files.isRegularFile(candidate)) {
                    return candidate;
                }
            }
        }
        return null;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitUntilReady(int port) throws IOException, InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadlineNanos) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new IOException("kblockdbserver on port " + port + " never became ready within 10s");
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path p : paths.sorted(Comparator.comparing(Path::toString).reversed()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    @Test
    void addListAndRemoveColumnsRoundTripThroughARealConnection() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            client.addColumn("java-client-column", ValueType.I64);
            assertTrue(client.columns().contains(new Column("java-client-column", ValueType.I64)));

            // Re-adding the same key conflicts rather than silently
            // changing (or re-confirming) its type.
            assertThrows(ConflictException.class,
                    () -> client.addColumn("java-client-column", ValueType.STR));

            assertTrue(client.removeColumn("java-client-column"));
            assertFalse(client.columns().contains(new Column("java-client-column", ValueType.I64)));
        }
    }

    @Test
    void removingAColumnThatDoesntExistReturnsFalse() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            assertFalse(client.removeColumn("java-client-no-such-column"));
        }
    }

    @Test
    void writingACellCreatesItsColumnAndRemovingTheColumnDropsItsValues() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            int[] coord = {7, 7, 7};
            client.set(coord, "java-client-implicit", new Value.Str("stone"));
            assertTrue(client.columns().contains(new Column("java-client-implicit", ValueType.STR)));

            assertTrue(client.removeColumn("java-client-implicit"));
            assertEquals(Optional.empty(), client.get(coord, "java-client-implicit"));
        }
    }

    @Test
    void aRemovedColumnCanComeBackWithADifferentType() throws IOException {
        try (KBlockDBClient client = KBlockDBClient.connect("127.0.0.1", binaryPort, "admin", ADMIN_PASSWORD)) {
            client.addColumn("java-client-retyped", ValueType.STR);
            assertTrue(client.removeColumn("java-client-retyped"));

            client.addColumn("java-client-retyped", ValueType.BOOL);
            assertTrue(client.columns().contains(new Column("java-client-retyped", ValueType.BOOL)));
            assertTrue(client.removeColumn("java-client-retyped"));
        }
    }
}
