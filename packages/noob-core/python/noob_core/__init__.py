"""
The ``noob-core`` package: the optimized rust core for the noob scheduler,
plus the python adapters that make it a drop-in for the pure-python scheduler.

The compiled rust extension lives in :mod:`noob_core._core` and holds all
scheduler state and logic. :class:`RustScheduler` / :class:`RustTopoSorter`
here are thin python adapters: they make the rust core quack like
:class:`noob.scheduler.Scheduler` / :class:`noob.toposort.TopoSorter`
(matching attribute names and ``isinstance`` checks, and providing the python
generator methods rust can't express). The rust core accepts and returns real
:class:`.Epoch` / :class:`.NodeSignal` / :class:`.MetaEvent` objects and raises
:mod:`noob.exceptions` types directly, so the adapters are near-pure
delegation.

``noob-core`` is an *optional accelerator*: it depends on ``noob`` and only
works alongside it (the rust core and these adapters are both defined in terms
of noob's domain types). :mod:`noob.scheduler` imports :class:`RustScheduler`
from here when the package is installed, and falls back to the pure-python
scheduler otherwise.

.. admonition:: Import-order workaround

    ``noob.scheduler`` selects the rust scheduler with a plain
    ``from noob_core import RustScheduler`` - clean, no special-casing on the
    noob side. The cost of that cleanliness is paid here: ``noob`` and
    ``noob_core`` import each other (``noob`` -> ``noob_core`` to select the
    scheduler; ``noob_core`` -> ``noob`` for the domain types). To keep both
    import orders working - and never silently fall back to the slow scheduler
    - this module is laid out so that :class:`RustScheduler` is fully defined
    *before* the noob imports that trigger ``noob``'s load. That way, when
    importing ``noob_core`` first re-enters through ``noob.scheduler``,
    ``RustScheduler`` already exists and gets selected. ``from __future__
    import annotations`` makes that possible by keeping class bodies free of
    eager noob references (all annotations are strings). The mid-module imports
    below are load-bearing; don't hoist them to the top.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from collections.abc import Iterator, Mapping, MutableSequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Self

from noob_core._core import (
    AlreadyDoneError,
    CoreScheduler,
    CoreTopoSorter,
    EpochCompletedError,
    EpochExistsError,
    NotAddedError,
    SchedulerError,
)

if TYPE_CHECKING:
    # annotation-only noob references; never evaluated at runtime
    # (PEP 563), so importing them here doesn't trigger noob's load
    from noob.edge import Edge
    from noob.event import Event, MetaEvent
    from noob.node import NodeSpecification
    from noob.toposort import GraphItem, _NodeInfo
    from noob.types import NodeID, NodeSignal, SignalName

__all__ = [
    "AlreadyDoneError",
    "CoreScheduler",
    "CoreTopoSorter",
    "EpochCompletedError",
    "EpochExistsError",
    "NotAddedError",
    "RustScheduler",
    "RustTopoSorter",
    "SchedulerError",
]


class RustScheduler:
    """
    Drop-in replacement for :class:`noob.scheduler.Scheduler` backed by the
    rust core from the optional ``noob-core`` package.

    See :class:`noob.scheduler.Scheduler` for documentation of the scheduler
    contract - the python implementation is the reference for the public API,
    every method here is a pure delegation to
    :class:`noob_core._core.CoreScheduler` , including run control and event
    iteration ( :meth:`.iter_epoch` / :meth:`.iter_events` are thin generator
    shells over core stepping methods - all graph and run state lives in rust).

    Defined before the runtime noob imports below so that selecting the rust
    scheduler works regardless of whether ``noob`` or ``noob_core`` is imported
    first (see the module docstring).
    """

    def __init__(
        self,
        nodes: dict[str, NodeSpecification],
        edges: list[Edge],
        source_nodes: list[NodeID] | None = None,
        _logger: logging.Logger | None = None,
    ) -> None:
        self.nodes = nodes
        self.edges = edges
        self._logger = _logger if _logger is not None else init_logger("noob.scheduler")
        self._core = CoreScheduler(nodes, edges, source_nodes, self._logger)
        self.source_nodes: list[NodeID] = self._core.source_nodes()
        self._epochs_view = _EpochsView(self._core)

    @classmethod
    def from_specification(cls, nodes: dict[str, NodeSpecification], edges: list[Edge]) -> Self:
        """
        Create an instance of a Scheduler from :class:`.NodeSpecification` and :class:`.Edge`
        """
        return cls(nodes=nodes, edges=edges)

    # --- state accessors ---

    @property
    def _epochs(self) -> _EpochsView:
        return self._epochs_view

    @property
    def subepochs(self) -> dict[Epoch, set[Epoch]]:
        return defaultdict(set, self._core.subepochs())

    _subepochs = subepochs

    @property
    def _epoch_log(self) -> deque[int]:
        return deque(self._core.epoch_log(), maxlen=100)

    @cached_property
    def graph_signals(self) -> set[tuple[NodeID, SignalName]]:
        """The set of (node id, signal) tuples that are depended on in the graph."""
        return self._core.graph_signals()

    # --- scheduling ---

    def add_epoch(self, epoch: int | Epoch | None = None) -> Epoch:
        """Add another epoch with a prepared graph to the scheduler."""
        return self._core.add_epoch(epoch)

    def add_subepoch(self, epoch: Epoch) -> Epoch:
        """
        Creates a topo sorter with all the nodes downstream of the node that created the epoch.
        """
        return self._core.add_subepoch(epoch)

    def is_active(self, epoch: Epoch | None = None) -> bool:
        """Graph remains active while it holds at least one epoch that is active."""
        return self._core.is_active(epoch)

    def get_ready(
        self, epoch: Epoch | None = None, node_id: NodeID | None = None
    ) -> list[MetaEvent]:
        """Output the set of nodes that are ready across different epochs."""
        return self._core.get_ready(epoch, node_id)

    def node_is_ready(self, node: NodeID, epoch: Epoch | None = None) -> bool:
        """Check if a single node is ready in a single or any epoch"""
        return self._core.node_is_ready(node, epoch)

    def node_is_done(self, node: NodeID, epoch: Epoch) -> bool:
        """Node is expired or done in specified epoch"""
        return self._core.node_is_done(node, epoch)

    def __getitem__(self, epoch: Epoch | int) -> RustTopoSorter:
        return RustTopoSorter._from_core(self._core.getitem(epoch))

    def sources_finished(self, epoch: Epoch | None = None) -> bool:
        """
        Check the source nodes of the given epoch have been processed.
        If epoch is None, check the source nodes of the latest epoch.
        """
        return self._core.sources_finished(epoch)

    def update(
        self, events: MutableSequence[Event | MetaEvent] | MutableSequence[Event]
    ) -> MutableSequence[Event] | MutableSequence[Event | MetaEvent]:
        """
        When a set of events are received, update the graphs within the scheduler.
        """
        return self._core.update(events)

    def done(
        self,
        epoch: Epoch,
        node_id: str,
        signal: SignalName | None = None,
        with_signals: bool = True,
    ) -> MetaEvent | None:
        """Mark a node in a given epoch as done."""
        return self._core.done(epoch, node_id, signal, with_signals)

    def expire(
        self,
        epoch: Epoch,
        node_id: str,
        signal: SignalName | None = None,
        with_signals: bool = True,
        unlock_optionals: bool = True,
        cascade: bool = True,
    ) -> MetaEvent | None:
        """
        Mark a node as having been completed without making its dependent nodes ready.
        """
        return self._core.expire(epoch, node_id, signal, with_signals, unlock_optionals, cascade)

    # --- run control & iteration ---

    def queue_epoch(self, epoch: Epoch | int) -> Epoch:
        """Grant permission to run a specific (root) epoch (a ``process`` call)."""
        return self._core.queue_epoch(epoch)

    def queue_epochs(self, n: int) -> list[Epoch]:
        """Grant permission to run the next ``n`` epochs (a bounded ``start`` call)."""
        return self._core.queue_epochs(n)

    def set_freerun(self, enabled: bool) -> None:
        """Grant (or revoke) permission to run any epoch as soon as it is ready."""
        self._core.set_freerun(enabled)

    def iter_epoch(
        self, epoch: Epoch | int | None = None, node_id: NodeID | None = None
    ) -> Iterator[MetaEvent | None]:
        """
        Iterate scheduling events for a single epoch until it is no longer active.
        See :meth:`noob.scheduler.Scheduler.iter_epoch` .
        """
        if epoch is None:
            roots = [e for e in self._core.epoch_keys() if len(e) == 1]
            if not roots:
                return
            epoch = max(roots, key=lambda e: e[0].epoch)
        elif isinstance(epoch, int):
            epoch = Epoch(epoch)

        while self._core.is_active(epoch):
            batch = self._core.get_ready(epoch, node_id)
            if batch:
                yield from batch
            else:
                yield None

    def iter_events(self, node_id: NodeID | None = None) -> Iterator[MetaEvent | None]:
        """
        Iterate scheduling events until stopped.
        See :meth:`noob.scheduler.Scheduler.iter_events` -
        each step is computed in the rust core by ``next_events`` .
        """
        while True:
            batch = self._core.next_events(node_id)
            if batch:
                yield from batch
            else:
                yield None

    def epoch_completed(self, epoch: Epoch) -> bool:
        """Check if the epoch has been completed."""
        return self._core.epoch_completed(epoch)

    def end_epoch(self, epoch: Epoch | int | None = None) -> MetaEvent | None:
        return self._core.end_epoch(epoch)

    def enable_node(self, node_id: str) -> None:
        """Enable the node in the scheduler and its NodeSpecification"""
        self.nodes[node_id].enabled = True
        self._core.enable_node(node_id)

    def disable_node(self, node_id: str) -> None:
        """Disable the node in the scheduler and its NodeSpecification"""
        self.nodes[node_id].enabled = False
        self._core.disable_node(node_id)

    def clear(self) -> None:
        """Remove epoch records, restarting the scheduler"""
        self._core.clear()

    # --- graph queries ---

    def has_cycle(self) -> bool:
        """Checks that the graph is acyclic."""
        return self._core.has_cycle()

    def generations(self) -> list[tuple[GraphItem, ...]]:
        """
        Get the topological generations of the graph:
        tuples for each set of nodes that can be run at the same time.
        """
        return self._core.generations()

    def asset_generations(self) -> dict[NodeID, list[tuple[str, ...]]]:
        """
        :meth:`.generations` except only including nodes with direct dependencies on assets.
        """
        return defaultdict(list, self._core.asset_generations())

    def upstream_nodes(self, node: NodeID) -> set[NodeID]:
        """All the nodes that have an effect on the given node"""
        return self._core.upstream_nodes(node)

    def __deepcopy__(self, memo: dict) -> RustScheduler:
        from copy import deepcopy

        new = object.__new__(RustScheduler)
        new.nodes = deepcopy(self.nodes, memo)
        new.edges = deepcopy(self.edges, memo)
        new._logger = self._logger
        new._core = self._core.__deepcopy__(memo)
        new.source_nodes = list(self.source_nodes)
        new._epochs_view = _EpochsView(new._core)
        return new

    def __repr__(self) -> str:
        return f"RustScheduler(nodes={list(self.nodes)}, epochs={[repr(e) for e in self._epochs]})"


# --- load-bearing import order (see module docstring) ---
# RustScheduler is defined above so that, when importing noob_core first
# re-enters here through noob.scheduler, it is already available to be selected.
# These runtime noob imports trigger noob's load; keep them below RustScheduler.
from noob.logging import init_logger  # noqa: E402
from noob.toposort import TopoSorter  # noqa: E402
from noob.types import Epoch  # noqa: E402


class RustTopoSorter(TopoSorter):
    """
    Rust-backed topological sorter, a drop-in counterpart of
    :class:`.TopoSorter` (which it subclasses so ``isinstance`` checks hold).

    All graph state lives in a :class:`noob_core._core.CoreTopoSorter` , which
    knows nothing about the scheduler. The scheduler *contains* topo sorters:
    when a sorter is obtained from :class:`RustScheduler` (e.g.
    ``scheduler[epoch]`` ) it shares state with the epoch it came from, so
    mutating it with :meth:`.get_ready` / :meth:`.done` advances the real
    epoch.

    The set- and dict-valued properties are snapshots built by the rust core:
    mutating the returned containers does not write through (the python
    TopoSorter exposes its mutable internals instead). Nothing in noob or its
    tests relies on writing through those containers.
    """

    __slots__ = ("_sorter",)

    def __init__(
        self,
        nodes: dict[str, NodeSpecification] | None = None,
        edges: list[Edge] | None = None,
    ) -> None:
        # deliberately does NOT call super().__init__: every TopoSorter slot
        # is shadowed below by a property reading from the rust sorter
        self._sorter = CoreTopoSorter(nodes, edges)

    @classmethod
    def _from_core(cls, sorter: CoreTopoSorter) -> RustTopoSorter:
        """Wrap an existing rust sorter handle, e.g. one owned by a scheduler"""
        instance = object.__new__(cls)
        instance._sorter = sorter
        return instance

    # --- state accessors, shadowing the parent's slots ---

    @property
    def signals(self) -> dict[NodeID, set[NodeSignal]]:  # type: ignore[override]
        return defaultdict(set, self._sorter.signals())

    @property
    def _node2info(self) -> dict[GraphItem, _NodeInfo]:  # type: ignore[override]
        return self._sorter.node_info()

    @property
    def _ready_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return self._sorter.ready_nodes()

    @property
    def _out_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return self._sorter.out_nodes()

    @property
    def _done_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return self._sorter.done_nodes()

    @property
    def _ran_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return self._sorter.ran_nodes()

    @property
    def _disabled_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return self._sorter.disabled_nodes()

    @property
    def _npassedout(self) -> int:  # type: ignore[override]
        return self._sorter.counters()[0]

    @property
    def _nfinished(self) -> int:  # type: ignore[override]
        return self._sorter.counters()[1]

    # --- mutations and queries ---

    def mark_ready(self, *nodes: GraphItem) -> None:
        self._sorter.mark_ready(list(nodes))

    def mark_out(self, *nodes: GraphItem) -> None:
        self._sorter.mark_out(list(nodes))

    def mark_expired(
        self, *nodes: GraphItem, unlock_optionals: bool = True, cascade: bool = False
    ) -> tuple[GraphItem, ...]:
        return self._sorter.mark_expired(list(nodes), unlock_optionals, cascade)

    def add(self, node: GraphItem, *predecessors: GraphItem, required: bool = True) -> None:
        self._sorter.add(node, list(predecessors), required)

    def get_ready(self, node_id: NodeID | None = None) -> tuple[GraphItem, ...]:
        return self._sorter.get_ready(node_id)

    def is_active(self) -> bool:
        return self._sorter.is_active()

    def done(self, *nodes: GraphItem) -> None:
        self._sorter.done(list(nodes))

    def resurrect(self, *nodes: GraphItem) -> None:
        self._sorter.resurrect(list(nodes))

    def find_cycle(self) -> list[GraphItem] | None:
        return self._sorter.find_cycle()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RustTopoSorter):
            return self._sorter.__eq__(other._sorter)
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        eq = self.__eq__(other)
        return NotImplemented if eq is NotImplemented else not eq

    __hash__ = None  # type: ignore[assignment] # mutable, like TopoSorter

    def __deepcopy__(self, memo: dict) -> RustTopoSorter:
        return RustTopoSorter._from_core(self._sorter.__deepcopy__(memo))


class _EpochsView(Mapping):
    """
    Read-through mapping emulating ``Scheduler._epochs`` :
    Epoch-keyed, insertion-ordered access to live sorter handles.
    """

    __slots__ = ("_core",)

    def __init__(self, core: CoreScheduler) -> None:
        self._core = core

    def __getitem__(self, epoch: Epoch | int) -> RustTopoSorter:
        if not isinstance(epoch, Epoch):
            epoch = Epoch(epoch)
        if not self._core.contains_epoch(epoch):
            raise KeyError(epoch)
        return RustTopoSorter._from_core(self._core.sorter(epoch))

    def __iter__(self) -> Iterator[Epoch]:
        return iter(self._core.epoch_keys())

    def __len__(self) -> int:
        return len(self._core.epoch_keys())
