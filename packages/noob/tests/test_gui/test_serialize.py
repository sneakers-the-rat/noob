"""Tests for the GUI value serializer."""

from dataclasses import dataclass

import pytest

from noob.gui.serialize import encode_value


class TestJsonValues:
    @pytest.mark.parametrize(
        "value", [None, True, False, 0, 1, -1, 3.14, "hello", "", "with spaces"]
    )
    def test_scalars(self, value: object) -> None:
        enc = encode_value(value)
        assert enc.envelope == {"kind": "json", "data": value}
        assert enc.payload is None

    def test_lists(self) -> None:
        enc = encode_value([1, 2, 3])
        assert enc.envelope == {"kind": "json", "data": [1, 2, 3]}
        assert enc.payload is None

    def test_dict(self) -> None:
        enc = encode_value({"a": 1, "b": [2, 3]})
        assert enc.envelope == {"kind": "json", "data": {"a": 1, "b": [2, 3]}}
        assert enc.payload is None


class TestBytes:
    def test_bytes(self) -> None:
        enc = encode_value(b"hello")
        assert enc.envelope == {"kind": "bytes", "size": 5}
        assert enc.payload == b"hello"

    def test_bytearray(self) -> None:
        enc = encode_value(bytearray(b"\x00\x01\x02"))
        assert enc.envelope == {"kind": "bytes", "size": 3}
        assert enc.payload == b"\x00\x01\x02"


class TestNdarray:
    """Duck-typed ndarray detection — no numpy needed."""

    @dataclass
    class FakeArr:
        shape: tuple[int, ...]
        dtype: str
        _bytes: bytes

        def tobytes(self) -> bytes:
            return self._bytes

    def test_basic_ndarray(self) -> None:
        arr = self.FakeArr(shape=(2, 3), dtype="uint8", _bytes=b"abcdef")
        enc = encode_value(arr)
        assert enc.envelope == {"kind": "ndarray", "shape": [2, 3], "dtype": "uint8"}
        assert enc.payload == b"abcdef"

    def test_ndarray_takes_precedence_over_jsonable_attrs(self) -> None:
        # An object that happens to have a `tobytes` method shouldn't fall through to repr.
        arr = self.FakeArr(shape=(4,), dtype="float32", _bytes=b"\x00" * 16)
        enc = encode_value(arr)
        assert enc.envelope["kind"] == "ndarray"
        assert enc.envelope["dtype"] == "float32"
        assert enc.payload is not None and len(enc.payload) == 16


class TestRepr:
    def test_unjsonable_object_falls_back_to_repr(self) -> None:
        class Weird:
            def __repr__(self) -> str:
                return "<weird thing>"

        enc = encode_value(Weird())
        assert enc.envelope == {"kind": "repr", "data": "<weird thing>"}
        assert enc.payload is None

    def test_repr_is_truncated(self) -> None:
        class Big:
            def __repr__(self) -> str:
                return "x" * 5000

        enc = encode_value(Big())
        assert enc.envelope["kind"] == "repr"
        # truncated to MAX_REPR_LEN = 1024
        assert len(enc.envelope["data"]) == 1024
