"""
Demo nodes for the live-running GUI. Numpy-free so the demo works in a
fresh install of just ``noob[gui]``.

The ``FakeArray`` class implements the duck-typed ``shape``/``dtype``/
``tobytes`` triple expected by :func:`noob.gui.serialize.encode_value` so
the GUI can render arrays as images without us pulling in numpy.
"""

import math
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Annotated as A

from noob.types import Name


@dataclass
class FakeArray:
    """Minimal ndarray-shaped object: shape + dtype + tobytes()."""

    shape: tuple[int, ...]
    dtype: str
    _bytes: bytes

    def tobytes(self) -> bytes:
        return self._bytes


def counter(
    sleep_for: float = 0.05,
) -> Generator[A[int, Name("count")], None, None]:
    """Steady tick — used as a synchronizer for the rest of the demo."""
    i = 0
    while True:
        time.sleep(sleep_for)
        yield i
        i += 1


def sine(count: int, period: float = 30.0, amplitude: float = 1.0) -> float:
    """Plottable scalar."""
    return amplitude * math.sin(count * (2 * math.pi / period))


def gradient_image(
    count: int, width: int = 96, height: int = 64
) -> A[FakeArray, Name("frame")]:
    """
    Animated grayscale gradient — uint8 (H, W). Renders as an image/video in
    the GUI inspector.
    """
    shift = count % width
    rows = bytearray(width * height)
    for y in range(height):
        row_base = y * width
        for x in range(width):
            rows[row_base + x] = (x + shift + y) & 0xFF
    return FakeArray(shape=(height, width), dtype="uint8", _bytes=bytes(rows))
