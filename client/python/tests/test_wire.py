import struct
import unittest

from kblockdb import (
    Affected,
    Bool,
    CellMeta,
    F64,
    Health,
    I64,
    ProtocolError,
    QueryRow,
    QueryValue,
    Rows,
    Stats,
    Str,
)
from kblockdb import _wire


class WireEncodingTest(unittest.TestCase):
    def test_encodes_hello(self) -> None:
        self.assertEqual(
            b"\x00\x05admin\x02hi", _wire.encode_hello("admin", "hi")
        )

    def test_encodes_cell_operations(self) -> None:
        coord = b"\x03" + struct.pack("<iii", 1, -2, 3)
        key = b"\x08\x00material"
        self.assertEqual(b"\x01" + coord + key, _wire.encode_get((1, -2, 3), "material"))
        self.assertEqual(
            b"\x02" + coord + key + b"\x02" + struct.pack("<q", -7),
            _wire.encode_set((1, -2, 3), "material", I64(-7)),
        )
        self.assertEqual(
            b"\x03" + coord + key,
            _wire.encode_remove((1, -2, 3), "material"),
        )

    def test_encodes_every_value_type(self) -> None:
        prefix = b"\x02\x01\x01\x00\x00\x00\x01\x00k"
        self.assertEqual(
            prefix + b"\x00\x02\x00\x00\x00ab",
            _wire.encode_set((1,), "k", Str("ab")),
        )
        self.assertEqual(
            prefix + b"\x01" + struct.pack("<d", 2.5),
            _wire.encode_set((1,), "k", F64(2.5)),
        )
        self.assertEqual(
            prefix + b"\x03\x01",
            _wire.encode_set((1,), "k", Bool(True)),
        )

    def test_encodes_extended_operations(self) -> None:
        self.assertEqual(b"\x04", _wire.encode_health())
        self.assertEqual(b"\x05", _wire.encode_stats())
        region = b"\x01\x01\x00\x00\x00\x01\x02\x00\x00\x00\x01\x00k"
        self.assertEqual(b"\x06" + region, _wire.encode_get_region((1,), (2,), "k"))
        self.assertEqual(
            b"\x07"
            + region
            + struct.pack("<I", 2)
            + b"\x02"
            + struct.pack("<q", 7)
            + b"\x03\x01",
            _wire.encode_set_region((1,), (2,), "k", (I64(7), Bool(True))),
        )
        self.assertEqual(
            b"\x08" + region, _wire.encode_remove_region((1,), (2,), "k")
        )
        self.assertEqual(
            b"\x09\x08\x00\x00\x00SELECT *", _wire.encode_query("SELECT *")
        )

    def test_rejects_out_of_range_fields(self) -> None:
        with self.assertRaises(ValueError):
            _wire.encode_hello("u" * 256, "pw")
        with self.assertRaises(ValueError):
            _wire.encode_get(tuple(range(256)), "k")
        with self.assertRaises(ValueError):
            _wire.encode_get((2**31,), "k")
        with self.assertRaises(ValueError):
            _wire.encode_set((1,), "k", I64(2**63))


class WireDecodingTest(unittest.TestCase):
    def test_decodes_hello_health_and_stats(self) -> None:
        self.assertEqual(
            _wire.HelloOk(3, 10_000, False),
            _wire.decode_response(b"\x00\x03" + struct.pack("<I", 10_000) + b"\x00"),
        )
        self.assertEqual(
            _wire.HealthResponse(Health(3, 10_000, 123)),
            _wire.decode_response(
                b"\x08\x03" + struct.pack("<I", 10_000) + struct.pack("<Q", 123)
            ),
        )
        self.assertEqual(
            _wire.StatsResponse(Stats(2, 3, 4)),
            _wire.decode_response(b"\x09" + struct.pack("<QQQ", 2, 3, 4)),
        )

    def test_decodes_value_with_metadata(self) -> None:
        payload = (
            b"\x02\x00\x05\x00\x00\x00stone" + struct.pack("<QQQ", 10, 11, 1)
        )
        self.assertEqual(
            _wire.ValueResponse(Str("stone"), CellMeta(10, 11, 1)),
            _wire.decode_response(payload),
        )

    def test_decodes_region_values(self) -> None:
        payload = (
            b"\x0a"
            + struct.pack("<I", 3)
            + b"\x01\x02"
            + struct.pack("<q", 7)
            + b"\x00\x01\x03\x01"
        )
        self.assertEqual(
            _wire.RegionValues((I64(7), None, Bool(True))),
            _wire.decode_response(payload),
        )

    def test_decodes_query_results(self) -> None:
        rows_payload = (
            b"\x0b\x00"
            + struct.pack("<I", 1)
            + b"\x01"
            + struct.pack("<i", 7)
            + struct.pack("<I", 1)
            + b"\x01\x00k\x02"
            + struct.pack("<q", 9)
            + struct.pack("<QQQ", 10, 11, 1)
        )
        expected = Rows(
            (QueryRow((7,), (QueryValue("k", I64(9), CellMeta(10, 11, 1)),)),)
        )
        self.assertEqual(_wire.QueryResponse(expected), _wire.decode_response(rows_payload))
        self.assertEqual(
            _wire.QueryResponse(Affected(7)),
            _wire.decode_response(b"\x0b\x01" + struct.pack("<Q", 7)),
        )

    def test_rejects_malformed_responses(self) -> None:
        for payload in (b"", b"\xff", b"\x02\xff", b"\x00\x03"):
            with self.subTest(payload=payload), self.assertRaises(ProtocolError):
                _wire.decode_response(payload)


if __name__ == "__main__":
    unittest.main()
