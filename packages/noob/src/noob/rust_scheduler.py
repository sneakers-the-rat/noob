"""
Python-side wrapper around the rust scheduler core in ``noob-core``.

:class:`RustScheduler` is a drop-in replacement for
:class:`noob.scheduler.Scheduler` : all scheduler and per-epoch toposorter
state lives in rust ( ``noob_core.CoreScheduler`` ), and only native types
(strings, ints, bools, tuples, lists) cross the python:rust barrier.
This module converts :class:`.Edge` / :class:`.NodeSpecification` /
:class:`.Epoch` / :class:`.Event` objects to native types on the way in,
and reconstructs :class:`.Epoch` / :class:`.MetaEvent` /
:class:`.TopoSorter` -compatible views on the way out.

This module is only importable when the optional ``noob-core`` package is
installed; :mod:`noob.scheduler` falls back to the pure-python scheduler
otherwise.
"""

import contextlib
import logging
from collections import defaultdict, deque
from collections.abc import Callable, Iterator, Mapping, MutableSequence
from datetime import UTC, datetime
from functools import cached_property, wraps
from typing import Any, ParamSpec, Self, TypeVar
from uuid import uuid4

import noob_core

from noob.edge import Edge
from noob.event import Event, MetaEvent, MetaEventType, MetaSignal
from noob.exceptions import (
    AlreadyDoneError,
    EpochCompletedError,
    EpochExistsError,
    NotAddedError,
)
from noob.logging import init_logger
from noob.node import NodeSpecification
from noob.toposort import GraphItem, TopoSorter, _NodeInfo
from noob.types import Epoch, EpochSegment, NodeID, NodeSignal, SignalName

_NativeEpoch = list[tuple[str, int]]
_NativeItem = str | tuple[str, str]

_ERROR_MAP: dict[type[Exception], type[Exception]] = {
    noob_core.AlreadyDoneError: AlreadyDoneError,
    noob_core.NotAddedError: NotAddedError,
    noob_core.EpochExistsError: EpochExistsError,
    noob_core.EpochCompletedError: EpochCompletedError,
}
_CORE_ERRORS = tuple(_ERROR_MAP)

_P = ParamSpec("_P")
_T = TypeVar("_T")


def _translates_errors(fn: Callable[_P, _T]) -> Callable[_P, _T]:
    """Re-raise noob_core exceptions as their noob.exceptions equivalents"""

    @wraps(fn)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        try:
            return fn(*args, **kwargs)
        except _CORE_ERRORS as e:
            raise _ERROR_MAP[type(e)](str(e)) from e

    return wrapper


def _epoch_from_native(native: _NativeEpoch) -> Epoch:
    return Epoch(tuple(EpochSegment(node_id, number) for node_id, number in native))


def _item_from_native(item: _NativeItem) -> GraphItem:
    return item if isinstance(item, str) else NodeSignal(*item)


class RustTopoSorter(TopoSorter):
    """
    Rust-backed topological sorter, a drop-in counterpart of
    :class:`.TopoSorter` (which it subclasses so ``isinstance`` checks hold).

    All graph state lives in a ``noob_core.CoreTopoSorter`` , which knows
    nothing about the scheduler. The scheduler *contains* topo sorters: when a
    sorter is obtained from :class:`RustScheduler` (e.g. ``scheduler[epoch]`` )
    it shares state with the epoch it came from, so mutating it with
    :meth:`.get_ready` / :meth:`.done` advances the real epoch.

    The set- and dict-valued properties are snapshots built from native rust
    data: mutating the returned containers does not write through (the
    python TopoSorter exposes its mutable internals instead). Nothing in noob
    or its tests relies on writing through those containers.
    """

    __slots__ = ("_sorter",)

    def __init__(
        self,
        nodes: dict[str, NodeSpecification] | None = None,
        edges: list[Edge] | None = None,
    ) -> None:
        # deliberately does NOT call super().__init__: every TopoSorter slot
        # is shadowed below by a property reading from the rust sorter
        self._sorter = noob_core.CoreTopoSorter(
            [(node_id, bool(node.enabled)) for node_id, node in (nodes or {}).items()],
            [
                (e.source_node, e.source_signal, e.target_node, bool(e.required))
                for e in (edges or [])
            ],
        )

    @classmethod
    def _from_core(cls, sorter: "noob_core.CoreTopoSorter") -> "RustTopoSorter":
        """Wrap an existing rust sorter handle, e.g. one owned by a scheduler"""
        instance = object.__new__(cls)
        instance._sorter = sorter
        return instance

    # ------------------------------------------------------------------
    # state accessors, shadowing the parent's slots
    # ------------------------------------------------------------------

    @property
    def signals(self) -> dict[NodeID, set[NodeSignal]]:  # type: ignore[override]
        signals: dict[NodeID, set[NodeSignal]] = defaultdict(set)
        for node_id, items in self._sorter.signals():
            signals[node_id] = {NodeSignal(*item) for item in items}
        return signals

    @property
    def _node2info(self) -> dict[GraphItem, _NodeInfo]:  # type: ignore[override]
        info: dict[GraphItem, _NodeInfo] = {}
        for item, nqueue, succ, pred, opt_pred, opt_succ in self._sorter.node_info():
            rec = _NodeInfo(_item_from_native(item))
            rec.nqueue = nqueue
            rec.successors = {_item_from_native(i) for i in succ}
            rec.predecessors = {_item_from_native(i) for i in pred}
            rec.optional_predecessors = {_item_from_native(i) for i in opt_pred}
            # optional_successors only ever holds node ids, see _NodeInfo
            rec.optional_successors = {str(i) for i in opt_succ}
            info[rec.node] = rec
        return info

    @property
    def _ready_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return {_item_from_native(i) for i in self._sorter.ready_nodes()}

    @property
    def _out_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return {_item_from_native(i) for i in self._sorter.out_nodes()}

    @property
    def _done_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return {_item_from_native(i) for i in self._sorter.done_nodes()}

    @property
    def _ran_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return {_item_from_native(i) for i in self._sorter.ran_nodes()}

    @property
    def _disabled_nodes(self) -> set[GraphItem]:  # type: ignore[override]
        return {_item_from_native(i) for i in self._sorter.disabled_nodes()}

    @property
    def _npassedout(self) -> int:  # type: ignore[override]
        return self._sorter.counters()[0]

    @property
    def _nfinished(self) -> int:  # type: ignore[override]
        return self._sorter.counters()[1]

    # ------------------------------------------------------------------
    # mutations and queries
    # ------------------------------------------------------------------

    @_translates_errors
    def mark_ready(self, *nodes: GraphItem) -> None:
        self._sorter.mark_ready(list(nodes))

    @_translates_errors
    def mark_out(self, *nodes: GraphItem) -> None:
        self._sorter.mark_out(list(nodes))

    @_translates_errors
    def mark_expired(self, *nodes: GraphItem, unlock_optionals: bool = True) -> None:
        self._sorter.mark_expired(list(nodes), unlock_optionals)

    @_translates_errors
    def add(self, node: GraphItem, *predecessors: GraphItem, required: bool = True) -> None:
        self._sorter.add(node, list(predecessors), required)

    @_translates_errors
    def get_ready(self, node_id: NodeID | None = None) -> tuple[GraphItem, ...]:
        return tuple(_item_from_native(i) for i in self._sorter.get_ready(node_id))

    @_translates_errors
    def is_active(self) -> bool:
        return self._sorter.is_active()

    @_translates_errors
    def done(self, *nodes: GraphItem) -> None:
        self._sorter.done(list(nodes))

    @_translates_errors
    def resurrect(self, *nodes: GraphItem) -> None:
        self._sorter.resurrect(list(nodes))

    @_translates_errors
    def find_cycle(self) -> list[GraphItem] | None:
        cycle = self._sorter.find_cycle()
        return None if cycle is None else [_item_from_native(i) for i in cycle]

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RustTopoSorter):
            return self._sorter.__eq__(other._sorter)
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        eq = self.__eq__(other)
        return NotImplemented if eq is NotImplemented else not eq

    __hash__ = None  # type: ignore[assignment] # mutable, like TopoSorter

    def __deepcopy__(self, memo: dict) -> "RustTopoSorter":
        return RustTopoSorter._from_core(self._sorter.__deepcopy__(memo))


class _EpochsView(Mapping):
    """
    Read-through mapping emulating ``Scheduler._epochs`` :
    Epoch-keyed, insertion-ordered access to live sorter views.
    """

    __slots__ = ("_core",)

    def __init__(self, core: "noob_core.CoreScheduler") -> None:
        self._core = core

    def __getitem__(self, epoch: Epoch | int) -> RustTopoSorter:
        if not isinstance(epoch, Epoch):
            epoch = Epoch(epoch)
        if not self._core.contains_epoch(tuple(epoch)):
            raise KeyError(epoch)
        return RustTopoSorter._from_core(self._core.sorter(tuple(epoch)))

    def __iter__(self) -> Iterator[Epoch]:
        return (_epoch_from_native(key) for key in self._core.epoch_keys())

    def __len__(self) -> int:
        return len(self._core.epoch_keys())


class RustScheduler:
    """
    Drop-in replacement for :class:`noob.scheduler.Scheduler` backed by the
    rust core from the optional ``noob-core`` package.

    See :class:`noob.scheduler.Scheduler` for documentation of the scheduler
    contract - the python implementation is the reference, this class
    replicates its observable behavior.
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
        self._core = noob_core.CoreScheduler(
            [(node_id, bool(node.enabled)) for node_id, node in nodes.items()],
            [(e.source_node, e.source_signal, e.target_node, bool(e.required)) for e in edges],
            list(source_nodes) if source_nodes else None,
        )
        self.source_nodes: list[NodeID] = (
            list(source_nodes) if source_nodes else self._core.source_nodes()
        )
        self._epochs_view = _EpochsView(self._core)

    @classmethod
    def from_specification(cls, nodes: dict[str, NodeSpecification], edges: list[Edge]) -> Self:
        """
        Create an instance of a Scheduler from :class:`.NodeSpecification` and :class:`.Edge`
        """
        return cls(nodes=nodes, edges=edges)

    # ------------------------------------------------------------------
    # state accessors
    # ------------------------------------------------------------------

    @property
    def _epochs(self) -> _EpochsView:
        return self._epochs_view

    @property
    def subepochs(self) -> dict[Epoch, set[Epoch]]:
        subepochs: dict[Epoch, set[Epoch]] = defaultdict(set)
        for parent, subs in self._core.subepoch_map():
            subepochs[_epoch_from_native(parent)] = {_epoch_from_native(s) for s in subs}
        return subepochs

    _subepochs = subepochs

    @property
    def _epoch_log(self) -> deque[int]:
        return deque(self._core.epoch_log(), maxlen=100)

    @cached_property
    def graph_signals(self) -> set[tuple[NodeID, SignalName]]:
        """The set of (node id, signal) tuples that are depended on in the graph."""
        return {(e.source_node, e.source_signal) for e in self.edges}

    # ------------------------------------------------------------------
    # scheduling
    # ------------------------------------------------------------------

    @_translates_errors
    def add_epoch(self, epoch: int | Epoch | None = None) -> Epoch:
        """Add another epoch with a prepared graph to the scheduler."""
        if epoch is not None:
            if isinstance(epoch, Epoch):
                native = tuple(epoch)
            elif isinstance(epoch, int):
                native = tuple(Epoch(epoch))
            else:
                raise TypeError("Can only create an epoch from an epoch or integer")
        else:
            native = None
        return _epoch_from_native(self._core.add_epoch(native))

    @_translates_errors
    def add_subepoch(self, epoch: Epoch) -> Epoch:
        """
        Creates a topo sorter with all the nodes downstream of the node that created the epoch.
        """
        self._core.add_subepoch(tuple(epoch))
        return epoch

    @_translates_errors
    def is_active(self, epoch: Epoch | None = None) -> bool:
        """Graph remains active while it holds at least one epoch that is active."""
        return self._core.is_active(None if epoch is None else tuple(epoch))

    @_translates_errors
    def get_ready(
        self, epoch: Epoch | None = None, node_id: NodeID | None = None
    ) -> list[MetaEvent]:
        """Output the set of nodes that are ready across different epochs."""
        ready, warned = self._core.get_ready(None if epoch is None else tuple(epoch), node_id)
        for ep_native, signal in warned:
            self._logger.warning(
                "Scheduler attempted to return signal tuple %s in %s - "
                "something is wrong with how the graph is instantiated or run, "
                "or a node is emitting incorrect events manually, "
                "all signals should be marked done/expired by events passed in `update`. "
                "Ignoring - nodes downstream of this signal will not run.",
                NodeSignal(*signal),
                _epoch_from_native(ep_native),
            )
        return [
            MetaEvent(
                id=uuid4().int,
                timestamp=datetime.now(),
                node_id="meta",
                signal=MetaEventType.NodeReady,
                epoch=_epoch_from_native(ep_native),
                value=value,
            )
            for ep_native, value in ready
        ]

    @_translates_errors
    def node_is_ready(self, node: NodeID, epoch: Epoch | None = None) -> bool:
        """Check if a single node is ready in a single or any epoch"""
        return self._core.node_is_ready(node, None if epoch is None else tuple(epoch))

    @_translates_errors
    def node_is_done(self, node: NodeID, epoch: Epoch) -> bool:
        """Node is expired or done in specified epoch"""
        return self._core.node_is_done(node, tuple(epoch))

    @_translates_errors
    def __getitem__(self, epoch: Epoch | int) -> RustTopoSorter:
        if epoch == -1:
            return RustTopoSorter._from_core(self._core.sorter(self._core.latest_epoch()))
        if not isinstance(epoch, Epoch):
            epoch = Epoch(epoch)
        self._core.ensure(tuple(epoch))
        return RustTopoSorter._from_core(self._core.sorter(tuple(epoch)))

    @_translates_errors
    def sources_finished(self, epoch: Epoch | None = None) -> bool:
        """
        Check the source nodes of the given epoch have been processed.
        If epoch is None, check the source nodes of the latest epoch.
        """
        return self._core.sources_finished(None if epoch is None else tuple(epoch))

    def update(
        self, events: MutableSequence[Event | MetaEvent] | MutableSequence[Event]
    ) -> MutableSequence[Event] | MutableSequence[Event | MetaEvent]:
        """
        When a set of events are received, update the graphs within the scheduler.

        Mirrors the python ``Scheduler.update`` exactly, including routing all
        epoch finalization through :meth:`.end_epoch` via :meth:`.done` /
        :meth:`.expire` , so the call structure stays observably identical.
        """
        if not events:
            return events

        end_events: MutableSequence[MetaEvent] = []
        nodes_done = set()
        # process subepochs first so they're created when we handle parent epochs
        events = sorted(events, key=lambda ee: len(ee["epoch"]), reverse=True)
        for e in events:
            if e["node_id"] == "meta":
                continue
            elif (node_done := (e["epoch"], e["node_id"])) not in nodes_done:
                nodes_done.add(node_done)
                with contextlib.suppress(AlreadyDoneError, NotAddedError):
                    epoch_ended = self.done(e["epoch"], e["node_id"], with_signals=False)
                    if epoch_ended:
                        end_events.append(epoch_ended)
                        continue

            if (e["node_id"], e["signal"]) not in self.graph_signals:
                continue

            if e["value"] is MetaSignal.NoEvent:
                epoch_ended = self.expire(
                    epoch=e["epoch"], node_id=e["node_id"], signal=e["signal"]
                )
            else:
                epoch_ended = self.done(epoch=e["epoch"], node_id=e["node_id"], signal=e["signal"])

            if epoch_ended:
                end_events.append(epoch_ended)

        return [*events, *end_events]

    @_translates_errors
    def done(
        self,
        epoch: Epoch,
        node_id: str,
        signal: SignalName | None = None,
        with_signals: bool = True,
    ) -> MetaEvent | None:
        """Mark a node in a given epoch as done."""
        active = self._core.done(tuple(epoch), node_id, signal, with_signals)
        if not active:
            return self.end_epoch(epoch)
        return None

    @_translates_errors
    def expire(
        self,
        epoch: Epoch,
        node_id: str,
        signal: SignalName | None = None,
        with_signals: bool = True,
        unlock_optionals: bool = True,
    ) -> MetaEvent | None:
        """
        Mark a node as having been completed without making its dependent nodes ready.
        """
        active = self._core.expire(tuple(epoch), node_id, signal, with_signals, unlock_optionals)
        if not active:
            return self.end_epoch(epoch)
        return None

    @_translates_errors
    def epoch_completed(self, epoch: Epoch) -> bool:
        """Check if the epoch has been completed."""
        return self._core.epoch_completed(tuple(epoch))

    @_translates_errors
    def end_epoch(self, epoch: Epoch | int | None = None) -> MetaEvent | None:
        if epoch is None or epoch == -1:
            native = self._core.end_epoch(None)
        elif isinstance(epoch, Epoch):
            native = self._core.end_epoch(tuple(epoch))
        elif isinstance(epoch, int):
            native = self._core.end_epoch(tuple(Epoch(epoch)))
        else:
            raise TypeError("Can only end an epoch with an integer or Epoch")
        if native is None:
            return None
        ep = _epoch_from_native(native)
        self._logger.debug("Ending epoch %s", ep)
        return self._end_event(ep)

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

    # ------------------------------------------------------------------
    # graph queries
    # ------------------------------------------------------------------

    @_translates_errors
    def has_cycle(self) -> bool:
        """Checks that the graph is acyclic."""
        return self._core.has_cycle()

    @_translates_errors
    def generations(self) -> list[tuple[GraphItem, ...]]:
        """
        Get the topological generations of the graph:
        tuples for each set of nodes that can be run at the same time.
        """
        return [tuple(_item_from_native(i) for i in gen) for gen in self._core.generations()]

    def asset_generations(self) -> dict[NodeID, list[tuple[str, ...]]]:
        """
        :meth:`.generations` except only including nodes with direct dependencies on assets.
        """
        generations = defaultdict(list)
        asset_ids = set(e.source_signal for e in self.edges if e.source_node == "assets")
        for gen in self.generations():
            for asset in asset_ids:
                gen_deps = tuple(
                    [
                        g
                        for g in gen
                        if not isinstance(g, NodeSignal)
                        and any(
                            e.source_node == "assets"
                            and e.source_signal == asset
                            and e.target_node == g
                            for e in self.edges
                        )
                    ]
                )
                if gen_deps:
                    generations[asset].append(gen_deps)
        return generations

    @_translates_errors
    def upstream_nodes(self, node: NodeID) -> set[NodeID]:
        """
        All the nodes that have an effect on the given node
        """
        return set(self._core.upstream_nodes(node))

    # ------------------------------------------------------------------

    def _end_event(self, epoch: Epoch) -> MetaEvent:
        return MetaEvent(
            id=uuid4().int,
            timestamp=datetime.now(UTC),
            node_id="meta",
            signal=MetaEventType.EpochEnded,
            epoch=epoch,
            value=epoch,
        )

    def __deepcopy__(self, memo: dict) -> "RustScheduler":
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
        return (
            f"RustScheduler(nodes={list(self.nodes)}, " f"epochs={[repr(e) for e in self._epochs]})"
        )
