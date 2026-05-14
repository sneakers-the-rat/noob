"""
Encoding event values for transport to the GUI.

Encoded values are split into a JSON-serializable metadata envelope and
an optional ``bytes`` payload that travels in a sibling websocket frame.
The envelope's ``kind`` field tells the client how to interpret things.

Kinds:

- ``json``    — value is JSON-serializable, carried inline in the envelope ``data`` field
- ``ndarray`` — duck-typed numpy-like array; raw ``.tobytes()`` carried as the binary payload,
                with ``shape`` and ``dtype`` in the envelope
- ``bytes``   — bytes/bytearray/memoryview; carried directly as the binary payload
- ``repr``    — fallback; ``data`` is a truncated ``repr()`` of the value

We deliberately do not depend on numpy. Arrays are detected by attribute.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

MAX_REPR_LEN = 1024
MAX_INLINE_LIST_LEN = 1024
"""Lists/tuples up to this size are serialized inline as JSON kind."""


ValueKind = Literal["json", "ndarray", "bytes", "repr"]


class EncodedEnvelope(TypedDict, total=False):
    kind: ValueKind
    data: Any
    shape: list[int]
    dtype: str
    size: int


@dataclass
class EncodedValue:
    """Pairing of a JSON-serializable envelope with optional raw bytes payload"""

    envelope: EncodedEnvelope
    payload: bytes | None = None


def _looks_like_ndarray(value: Any) -> bool:
    return (
        hasattr(value, "tobytes")
        and hasattr(value, "shape")
        and hasattr(value, "dtype")
        and not isinstance(value, (bytes, bytearray, memoryview))
    )


def _is_jsonable(value: Any) -> bool:
    """
    Cheap check before falling through to repr.

    Tries to dump to JSON; bails on TypeError/ValueError.
    Limits list/dict scan depth implicitly via json.dumps recursion.
    """
    try:
        json.dumps(value, allow_nan=False, default=None)
        return True
    except (TypeError, ValueError):
        return False


def encode_value(value: Any) -> EncodedValue:
    """
    Encode a single event value to an envelope plus optional binary payload.

    Order of dispatch is important: we check ndarray-likes *before* bytes-likes
    because some array libs expose ``__bytes__`` or behave bytes-ish.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return EncodedValue(envelope={"kind": "json", "data": value})

    if _looks_like_ndarray(value):
        try:
            shape = [int(d) for d in value.shape]
            dtype = str(value.dtype)
            payload = bytes(value.tobytes())
        except Exception:  # noqa: BLE001
            return EncodedValue(
                envelope={"kind": "repr", "data": repr(value)[:MAX_REPR_LEN]}
            )
        return EncodedValue(
            envelope={"kind": "ndarray", "shape": shape, "dtype": dtype},
            payload=payload,
        )

    if isinstance(value, (bytes, bytearray, memoryview)):
        payload = bytes(value)
        return EncodedValue(
            envelope={"kind": "bytes", "size": len(payload)}, payload=payload
        )

    if isinstance(value, (list, tuple)):
        if len(value) <= MAX_INLINE_LIST_LEN and _is_jsonable(value):
            return EncodedValue(envelope={"kind": "json", "data": list(value)})

    if isinstance(value, dict) and _is_jsonable(value):
        return EncodedValue(envelope={"kind": "json", "data": value})

    if _is_jsonable(value):
        return EncodedValue(envelope={"kind": "json", "data": value})

    return EncodedValue(envelope={"kind": "repr", "data": repr(value)[:MAX_REPR_LEN]})
