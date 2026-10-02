import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from kblockdb import (
    Affected,
    BadRequestError,
    Bool,
    Column,
    ConflictError,
    F64,
    ForbiddenError,
    I64,
    KBlockDBClient,
    ProtocolError,
    Rows,
    Str,
    UnauthorizedError,
    ValueType,
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ClientIntegrationTest(unittest.TestCase):
    server: subprocess.Popen[bytes]
    binary_port: int
    database: str
    password = "kblockdb-python-client-test-password"

    @classmethod
    def setUpClass(cls) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        target_dirs = [repo_root / "target"]
        override = os.environ.get("CARGO_TARGET_DIR")
        if override:
            target_dirs.insert(0, Path(override))
        candidates = [
            target_dir / profile / name
            for target_dir in target_dirs
            for profile in ("debug", "release")
            for name in ("kblockdbserver", "kblockdbserver.exe")
        ]
        binary = next((path for path in candidates if path.is_file()), None)
        if binary is None:
            message = (
                "kblockdbserver binary not found; run cargo build -p kblockdbserver"
            )
            # CI sets this so a mislocated binary fails the job instead of
            # silently reducing it to zero end-to-end coverage.
            if os.environ.get("KBLOCKDB_REQUIRE_SERVER") == "1":
                raise RuntimeError(message)
            raise unittest.SkipTest(message)

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="kblockdb-python-client-test-"
        )
        root = Path(cls.temp_dir.name)
        config = root / "server.toml"
        config.write_text(
            f'admin_password = "{cls.password}"\n'
            "\n[[users]]\n"
            'username = "viewer"\n'
            'password = "viewer-password"\n'
            "read_only = true\n",
            encoding="utf-8",
        )
        cls.binary_port = _free_port()
        cls.server = subprocess.Popen(
            [
                str(binary),
                "--data-dir",
                str(root / "data"),
                "--http-port",
                str(_free_port()),
                "--binary-port",
                str(cls.binary_port),
                "--config",
                str(config),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        ready = False
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", cls.binary_port), 0.2):
                    ready = True
                    break
            except OSError:
                time.sleep(0.05)
        if not ready:
            cls.server.kill()
            cls.server.wait()
            cls.temp_dir.cleanup()
            raise RuntimeError("kblockdbserver did not become ready")

        # The server manages no databases until one is explicitly created --
        # `cls.database` is the one almost every test below operates
        # against, created once here rather than per test.
        cls.database = "db"
        with KBlockDBClient.connect(
            "127.0.0.1", cls.binary_port, "admin", cls.password, cls.database
        ) as bootstrap:
            bootstrap.create_database(cls.database)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.terminate()
        try:
            cls.server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.server.kill()
            cls.server.wait()
        cls.temp_dir.cleanup()

    def connect(self) -> KBlockDBClient:
        return KBlockDBClient.connect(
            "127.0.0.1", self.binary_port, "admin", self.password, self.database
        )

    def test_connect_and_authentication(self) -> None:
        with self.connect() as client:
            self.assertTrue(client.database_selected)
            self.assertEqual(3, client.axes)
            self.assertEqual(10_000, client.world_dim)
            self.assertEqual(32, client.chunk_dim)
            self.assertFalse(client.read_only)
        with self.assertRaises(UnauthorizedError):
            KBlockDBClient.connect(
                "127.0.0.1", self.binary_port, "admin", "wrong", self.database
            )

    def test_reauthenticate_updates_role_and_enforces_read_only(self) -> None:
        with self.connect() as client:
            client.reauthenticate("viewer", "viewer-password")
            self.assertTrue(client.read_only)
            with self.assertRaises(ForbiddenError):
                client.set((1, 1, 1), "read-only-key", I64(1))
            client.reauthenticate("admin", self.password)
            self.assertFalse(client.read_only)
            client.set((1, 1, 1), "read-only-key", I64(1))

    def test_every_value_type_and_metadata_round_trip(self) -> None:
        with self.connect() as client:
            values = (Str("stone"), I64(-42), F64(2.5), Bool(True))
            for index, value in enumerate(values):
                coord = (10, 10, 10 + index)
                key = f"value-{index}"
                client.set(coord, key, value)
                self.assertEqual(value, client.get(coord, key))
                result = client.get_with_meta(coord, key)
                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(0, result.meta.version)
                client.remove(coord, key)
                self.assertIsNone(client.get(coord, key))

    def test_health_and_stats(self) -> None:
        with self.connect() as client:
            health = client.health()
            # `cls.database` was created in setUpClass -- at least one
            # database always exists by the time any test runs.
            self.assertGreaterEqual(health.database_count, 1)
            self.assertGreater(health.timestamp, 0)
            # Which hostname the test machine has isn't knowable here;
            # that one was reported at all is.
            self.assertTrue(health.hostname)
            client.set((20, 20, 20), "stats-key", I64(1))
            stats = client.stats()
            self.assertGreater(stats.total_chunks, 0)
            self.assertGreater(stats.total_bytes, 0)

    def test_region_operations(self) -> None:
        with self.connect() as client:
            origin = (30, 30, 30)
            extent = (2, 1, 1)
            client.set_region(origin, extent, "region-key", (I64(1), I64(2)))
            self.assertEqual(
                (I64(1), I64(2)),
                client.get_region(origin, extent, "region-key"),
            )
            client.remove_region(origin, extent, "region-key")
            self.assertEqual(
                (None, None), client.get_region(origin, extent, "region-key")
            )

    def test_query_returns_rows_and_affected_counts(self) -> None:
        with self.connect() as client:
            self.assertEqual(
                Affected(2),
                client.query(
                    "SET (python_query_key = 7) "
                    "IN (40,40,40) TO (42,41,41)"
                ),
            )
            result = client.query(
                "SELECT python_query_key FROM (40,40,40) TO (42,41,41)"
            )
            self.assertIsInstance(result, Rows)
            assert isinstance(result, Rows)
            self.assertEqual(2, result.total_rows)
            self.assertEqual((40, 40, 40), result.rows[0].coord)
            self.assertEqual(I64(7), result.rows[0].values[0].value)

    def test_bad_request_does_not_close_connection(self) -> None:
        with self.connect() as client:
            with self.assertRaises(BadRequestError):
                client.get((1, 2), "material")
            self.assertIsNone(client.get((99, 99, 99), "never-set"))

    def test_framing_failure_permanently_breaks_the_connection(self) -> None:
        with self.connect() as client:
            # A read interrupted mid-frame leaves the stream at an unknown
            # offset, so every later request must fail instead of decoding
            # another request's bytes as its own response.
            client._socket.close()
            with self.assertRaises(OSError):
                client.get((1, 2, 3), "material")
            with self.assertRaises(ProtocolError):
                client.get((1, 2, 3), "material")

    def test_a_closed_client_rejects_further_requests(self) -> None:
        client = self.connect()
        client.close()
        with self.assertRaises(ProtocolError):
            client.health()


    def test_add_list_and_remove_columns_round_trip(self) -> None:
        with self.connect() as client:
            client.add_column("py-client-column", ValueType.I64)
            self.assertIn(Column("py-client-column", ValueType.I64), client.columns())

            # Re-adding the same key conflicts rather than silently
            # changing (or re-confirming) its type.
            with self.assertRaises(ConflictError):
                client.add_column("py-client-column", ValueType.STR)

            self.assertTrue(client.remove_column("py-client-column"))
            self.assertNotIn(
                Column("py-client-column", ValueType.I64), client.columns()
            )

    def test_removing_a_column_that_does_not_exist_returns_false(self) -> None:
        with self.connect() as client:
            self.assertFalse(client.remove_column("py-client-no-such-column"))

    def test_writing_a_cell_creates_its_column_and_removal_drops_its_values(
        self,
    ) -> None:
        with self.connect() as client:
            client.set((7, 7, 7), "py-client-implicit", Str("stone"))
            self.assertIn(
                Column("py-client-implicit", ValueType.STR), client.columns()
            )

            self.assertTrue(client.remove_column("py-client-implicit"))
            self.assertIsNone(client.get((7, 7, 7), "py-client-implicit"))

    def test_a_removed_column_can_come_back_with_a_different_type(self) -> None:
        with self.connect() as client:
            client.add_column("py-client-retyped", ValueType.STR)
            self.assertTrue(client.remove_column("py-client-retyped"))

            client.add_column("py-client-retyped", ValueType.BOOL)
            self.assertIn(
                Column("py-client-retyped", ValueType.BOOL), client.columns()
            )
            self.assertTrue(client.remove_column("py-client-retyped"))

    def test_a_conflict_does_not_close_the_connection(self) -> None:
        with self.connect() as client:
            client.add_column("py-client-conflict", ValueType.STR)
            with self.assertRaises(ConflictError):
                client.add_column("py-client-conflict", ValueType.STR)
            self.assertIsNotNone(client.health())
            self.assertTrue(client.remove_column("py-client-conflict"))

    # --- Multi-database support ---

    def test_connecting_to_an_unknown_database_authenticates_without_selecting_one(
        self,
    ) -> None:
        with KBlockDBClient.connect(
            "127.0.0.1", self.binary_port, "admin", self.password, "py-bootstrap-db"
        ) as client:
            self.assertFalse(client.database_selected)
            self.assertIsNone(client.axes)
            self.assertIsNone(client.world_dim)
            self.assertIsNone(client.chunk_dim)
            with self.assertRaises(ProtocolError):
                client.get((0, 0, 0), "k")

            client.create_database("py-bootstrap-db")
            # Still not selected until Hello runs again.
            with self.assertRaises(ProtocolError):
                client.get((0, 0, 0), "k")

            client.use_database("py-bootstrap-db")
            self.assertTrue(client.database_selected)
            self.assertEqual(3, client.axes)
            self.assertIsNone(client.get((0, 0, 0), "k"))

            self.assertTrue(client.remove_database("py-bootstrap-db"))

    def test_create_database_rejects_a_shape_override(self) -> None:
        with self.connect() as client:
            with self.assertRaises(ValueError):
                client.create_database("py-shape-override", axes=1)

    def test_list_databases_includes_the_shared_test_database(self) -> None:
        with self.connect() as client:
            self.assertIn(self.database, client.list_databases())

    def test_removing_an_unknown_database_returns_false(self) -> None:
        with self.connect() as client:
            self.assertFalse(client.remove_database("py-no-such-database"))

    def test_two_databases_are_isolated(self) -> None:
        with self.connect() as client:
            client.create_database("py-other-db")
        try:
            with KBlockDBClient.connect(
                "127.0.0.1",
                self.binary_port,
                "admin",
                self.password,
                "py-other-db",
            ) as other:
                self.assertIsNone(other.get((1, 1, 1), "py-isolation-key"))
                other.set((1, 1, 1), "py-isolation-key", Str("other"))

            with self.connect() as client:
                self.assertIsNone(client.get((1, 1, 1), "py-isolation-key"))
        finally:
            with self.connect() as client:
                self.assertTrue(client.remove_database("py-other-db"))

    def test_a_read_only_account_can_list_but_not_create_or_remove_databases(
        self,
    ) -> None:
        with self.connect() as client:
            client.reauthenticate("viewer", "viewer-password")
            self.assertIn(self.database, client.list_databases())
            with self.assertRaises(ForbiddenError):
                client.create_database("py-read-only-attempt")
            with self.assertRaises(ForbiddenError):
                client.remove_database(self.database)
            client.reauthenticate("admin", self.password)


if __name__ == "__main__":
    unittest.main()
