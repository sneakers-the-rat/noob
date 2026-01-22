from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import count
from typing import Any, TypeVar

from noob.event import Event, MetaSignal
from noob.node.base import Node
from noob.types import Epoch, make_sub_epoch

_TInput = TypeVar("_TInput")


@dataclass
class MapResult:
    """
    Special return type from Map nodes containing pre-created events with sub-epochs.

    When a runner encounters a MapResult instead of a raw value, it:
    1. Creates sub-epochs in the scheduler for each event
    2. Stores the pre-created events directly
    3. Continues processing in each sub-epoch

    Attributes:
        events: List of Event objects with assigned sub-epochs
        parent_epoch: The epoch the map was called in
        map_node_id: ID of the map node that created these events
    """

    events: list[Event]
    parent_epoch: Epoch
    map_node_id: str


class Map(Node):
    """
    Cardinality expansion node.

    Given a node that emits 1 (iterable) event, split it into separate events,
    each processed in its own sub-epoch.

    The Map node is special:
    - It returns a MapResult instead of a raw value
    - The runner handles MapResult by creating sub-epochs
    - Downstream nodes are called once per item in the mapped sequence
    - Non-mapped dependencies are "broadcast" to all sub-epochs

    Example:
        If node `a` emits ["x", "y", "z"] in epoch ((0, "root"),),
        the map node `b` creates events in epochs:
        - ((0, "root"), (0, "b")) -> "x"
        - ((0, "root"), (1, "b")) -> "y"
        - ((0, "root"), (2, "b")) -> "z"
    """

    _current_epoch: Epoch | None = None
    _counter: count = None  # type: ignore[assignment]

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self._counter = count()

    def set_epoch_context(self, epoch: Epoch) -> None:
        """
        Called by runner before process to set the current epoch context.

        This allows the map node to know which epoch it's processing in,
        so it can create proper sub-epochs.
        """
        self._current_epoch = epoch

    def process(self, value: Sequence[_TInput]) -> MapResult | MetaSignal:
        """
        Split an iterable into separate events with sub-epoch assignments.

        Args:
            value: An iterable to map over

        Returns:
            MapResult containing events with sub-epochs, or MetaSignal.NoEvent for empty input
        """
        if self._current_epoch is None:
            raise RuntimeError(
                "Map.process called without epoch context. "
                "The runner must call set_epoch_context() before process()."
            )

        # Handle empty iterables
        if not value:
            return MetaSignal.NoEvent

        timestamp = datetime.now(UTC)
        events = []

        for i, item in enumerate(value):
            sub_epoch = make_sub_epoch(self._current_epoch, i, self.id)
            event = Event(
                id=next(self._counter),
                timestamp=timestamp,
                node_id=self.id,
                signal="value",
                epoch=sub_epoch,
                value=item,
            )
            events.append(event)

        return MapResult(
            events=events,
            parent_epoch=self._current_epoch,
            map_node_id=self.id,
        )
