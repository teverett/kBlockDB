import struct
import unittest

from kblockdb import (
    Affected,
    Bool,
    CellMeta,
    Column,
    F64,
    Health,
    I64,
    ProtocolError,
    QueryRow,
    QueryValue,
    Rows,
    Stats,
    Str,
    ValueType,
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
    def test_decoding_a_health_response_without_a_hostname_is_an_error(self) -> None:
        # Exactly what a pre-hostname server would send: the frame ends
        # where the hostname should start.
        with self.assertRaises(ProtocolError):
            _wire.decode_response(
                b"\x08\x03" + struct.pack("<II", 10_000, 32) + struct.pack("<Q", 123)
            )

    def test_decodes_hello_health_and_stats(self) -> None:
        self.assertEqual(
            _wire.HelloOk(3, 10_000, False),
            _wire.decode_response(b"\x00\x03" + struct.pack("<I", 10_000) + b"\x00"),
        )
        self.assertEqual(
            _wire.HealthResponse(Health("db-1.example.com", 3, 10_000, 32, 123)),
            _wire.decode_response(
                b"\x08\x03"
                + struct.pack("<II", 10_000, 32)
                + struct.pack("<Q", 123)
                + struct.pack("<H", 16)
                + b"db-1.example.com"
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


class ColumnWireTest(unittest.TestCase):
    def test_encodes_column_requests(self) -> None:
        self.assertEqual(b"\x0a", _wire.encode_list_columns())
        self.assertEqual(
            b"\x0b\x01\x00k\x01", _wire.encode_add_column("k", ValueType.F64)
        )
        self.assertEqual(b"\x0c\x01\x00k", _wire.encode_remove_column("k"))

    def test_add_column_carries_each_types_wire_tag(self) -> None:
        for value_type in ValueType:
            with self.subTest(value_type=value_type):
                encoded = _wire.encode_add_column("k", value_type)
                self.assertEqual(value_type.value, encoded[-1])

    def test_value_type_of_matches_each_values_wire_tag(self) -> None:
        for value in (Str("stone"), F64(2.6), I64(7), Bool(True)):
            with self.subTest(value=value):
                self.assertEqual(
                    ValueType.of(value).value, _wire._pack_value(value)[0]
                )

    def test_value_type_wire_names(self) -> None:
        self.assertEqual(
            ["str", "f64", "i64", "bool"], [t.wire_name for t in ValueType]
        )

    def test_decodes_columns_response(self) -> None:
        payload = (
            b"\x0c"
            + struct.pack("<I", 2)
            + struct.pack("<H", 8)
            + b"hardness\x01"
            + struct.pack("<H", 8)
            + b"material\x00"
        )
        self.assertEqual(
            _wire.ColumnsResponse(
                (
                    Column("hardness", ValueType.F64),
                    Column("material", ValueType.STR),
                )
            ),
            _wire.decode_response(payload),
        )

    def test_decodes_an_empty_columns_response(self) -> None:
        self.assertEqual(
            _wire.ColumnsResponse(()),
            _wire.decode_response(b"\x0c" + struct.pack("<I", 0)),
        )

    def test_decodes_conflict_as_an_error_response(self) -> None:
        payload = b"\x0d" + struct.pack("<H", 4) + b"nope"
        self.assertEqual(
            _wire.ErrorResponse(0x0D, "nope"), _wire.decode_response(payload)
        )

    def test_rejects_malformed_column_responses(self) -> None:
        payloads = (
            # Claims two columns but carries none.
            b"\x0c" + struct.pack("<I", 2),
            # One column with a type tag no ValueType uses.
            b"\x0c" + struct.pack("<I", 1) + struct.pack("<H", 1) + b"k\xfe",
        )
        for payload in payloads:
            with self.subTest(payload=payload), self.assertRaises(ProtocolError):
                _wire.decode_response(payload)


if __name__ == "__main__":
    unittest.main()
