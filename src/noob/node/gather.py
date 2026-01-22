from dataclasses import dataclass
from multiprocessing import Lock
from multiprocessing.synchronize import Lock as LockType
from typing import Any, Generic, TypeVar

from pydantic import PrivateAttr

from noob.event import MetaSignal
from noob.node.base import Node
from noob.types import Epoch, epoch_parent

_TInput = TypeVar("_TInput")


@dataclass
class GatherResult:
    """
    Special return type from Gather nodes when collapsing map sub-epochs.

    When a runner encounters a GatherResult instead of a raw value, it:
    1. Emits the gathered value at the target (collapsed) epoch
    2. Cleans up sub-epoch tracking in the scheduler
    3. Continues processing at the collapsed epoch level

    Attributes:
        value: The gathered list of values
        target_epoch: The epoch to emit the result at (usually the parent epoch)
        source_map_node_id: ID of the map node whose events were gathered
    """

    value: list[Any]
    target_epoch: Epoch
    source_map_node_id: str | None = None


class Gather(Node, Generic[_TInput]):
    """
    Cardinality reduction node.

    Given a node that emits >1 events, gather them into a single iterable.

    Three modes of operation:

    1. **Fixed count mode** - gather a fixed number of events:

    .. code-block:: yaml

        nodename:
          type: gather
          params:
            n: 5
          depends:
            - value: othernode.signal

    2. **Trigger mode** - gather events until a trigger is received:

    .. code-block:: yaml

        nodename:
          type: gather
          depends:
            - value: othernode.signal_1
            - trigger: thirdnode.signal_2

    3. **Map collapse mode** - gather all sub-epoch events from a map operation.
       This mode is automatically detected when the gather depends on a mapped signal.
       When `collapse_map=True` (default), the gather will:
       - Wait for all sub-epoch events from the upstream map
       - Collapse them back to the parent epoch
       - Return a GatherResult that the runner handles specially
    """

    n: int | None = None
    collapse_map: bool = True
    """When True and depending on a mapped signal, collapse sub-epochs back to parent."""

    _items: list[_TInput] = PrivateAttr(default_factory=list)
    _lock: LockType = PrivateAttr(default_factory=Lock)
    _current_epoch: Epoch | None = None
    _expected_count: int | None = None
    _source_map_node_id: str | None = None
    _gathered_epochs: set[Epoch] = PrivateAttr(default_factory=set)

    def set_epoch_context(
        self,
        epoch: Epoch,
        expected_count: int | None = None,
        source_map_node_id: str | None = None,
    ) -> None:
        """
        Called by runner before process to set the current epoch context.

        Args:
            epoch: The current epoch being processed
            expected_count: For map collapse mode, the expected number of sub-epochs
            source_map_node_id: For map collapse mode, the ID of the upstream map node
        """
        self._current_epoch = epoch
        self._expected_count = expected_count
        self._source_map_node_id = source_map_node_id

    def process(
        self, value: _TInput, trigger: Any | None = None
    ) -> list[_TInput] | GatherResult | MetaSignal:
        """Collect value in a list, emit based on mode.

        Returns:
            - In map collapse mode: GatherResult when all sub-epochs collected
            - In fixed count mode: list when n items collected
            - In trigger mode: list when trigger received
            - MetaSignal.NoEvent otherwise
        """
        if trigger is not None and self.n is not None:
            raise ValueError("Cannot use trigger mode while `n` is set")

        with self._lock:
            self._items.append(value)

            # Track which epoch this value came from (for map collapse mode)
            if self._current_epoch is not None:
                self._gathered_epochs.add(self._current_epoch)

            if self._should_return(trigger):
                try:
                    items = self._items
                    # In map collapse mode, return GatherResult
                    if (
                        self.collapse_map
                        and self._expected_count is not None
                        and self._current_epoch is not None
                    ):
                        parent = epoch_parent(self._current_epoch)
                        if parent is not None:
                            return GatherResult(
                                value=items,
                                target_epoch=parent,
                                source_map_node_id=self._source_map_node_id,
                            )
                    return items
                finally:
                    # clear list after returning
                    self._items = []
                    self._gathered_epochs = set()
                    self._expected_count = None
                    self._source_map_node_id = None
            return MetaSignal.NoEvent

    def _should_return(self, trigger: Any | None) -> bool:
        # Map collapse mode: return when we've gathered expected count
        if self.collapse_map and self._expected_count is not None:
            return len(self._items) >= self._expected_count

        # Fixed count mode
        if self.n is not None:
            return len(self._items) >= self.n

        # Trigger mode
        return trigger is not None
