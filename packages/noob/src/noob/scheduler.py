import contextlib
import logging
import os
from collections import defaultdict, deque
from collections.abc import Iterator, MutableSequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cached_property
from itertools import count
from typing import Any, Self
from uuid import uuid4

from noob.const import VIRTUAL_NODES as _VIRTUAL_NODES
from noob.edge import Edge
from noob.event import Event, MetaEvent, MetaEventType, MetaSignal
from noob.exceptions import AlreadyDoneError, EpochCompletedError, EpochExistsError, NotAddedError
from noob.logging import init_logger
from noob.node import NodeSpecification
from noob.toposort import GraphItem, NodeSignal, TopoSorter
from noob.types import Epoch, NodeID, SignalName


@dataclass()
class Scheduler:
    nodes: dict[str, NodeSpecification]
    edges: list[Edge]
    source_nodes: list[NodeID] = field(default_factory=list)
    _logger: logging.Logger = field(default_factory=lambda: init_logger("noob.scheduler"))

    _clock: count = field(default_factory=count)
    _epochs: dict[Epoch, TopoSorter] = field(default_factory=dict)
    _subepochs: dict[Epoch, set[Epoch]] = field(default_factory=lambda: defaultdict(set))
    _epoch_log: deque[int] = field(default_factory=lambda: deque(maxlen=100))
    _subgraphs: dict[NodeID, tuple[dict[str, NodeSpecification], list[Edge]]] = field(
        default_factory=dict
    )
    _frozen_sorters: dict[tuple[NodeID, ...], TopoSorter] = field(default_factory=dict)

    _freerun: bool = False
    """Permission to run any epoch as soon as it is ready, creating epochs as needed"""
    _run_queue: list[int] = field(default_factory=list)
    """Root epochs we have permission to run, in the order permission was granted"""
    _grant_clock: count = field(default_factory=count)
    """The next epoch number to grant for implicit (`start n`/freerun) run permission"""
    _progress: dict[NodeID, int] = field(default_factory=dict)
    """Per-node stateful watermark: the lowest root epoch the node has not yet completed.
    Stateful nodes are only ready in an epoch when their watermark has reached it."""
    _pending_meta: list[MetaEvent] = field(default_factory=list)
    """MetaEvents generated as side effects (e.g. NodeCanceled from expiry cascades),
    drained and returned by :meth:`.update`"""

    def __post_init__(self):
        self._get_sources()

    @classmethod
    def from_specification(cls, nodes: dict[str, NodeSpecification], edges: list[Edge]) -> Self:
        """
        Create an instance of a Scheduler from :class:`.NodeSpecification` and :class:`.Edge`

        """
        return cls(nodes=nodes, edges=edges)

    def _get_sources(self) -> Self:
        """
        Get the IDs of the nodes that do not depend on other nodes.

        * `input` nodes are special implicit source nodes. Other nodes
        * CAN depend on it and still be a source node.

        """
        if not self.source_nodes:
            graph = self._init_graph()
            self.source_nodes = [
                id_
                for id_ in graph.ready_nodes
                if id_ not in _VIRTUAL_NODES and not isinstance(id_, NodeSignal)
            ]
        return self

    @property
    def subepochs(self) -> dict[Epoch, set[Epoch]]:
        return self._subepochs

    @cached_property
    def graph_signals(self) -> set[tuple[NodeID, SignalName]]:
        """
        The set of (node id, signal) tuples that are depended on in the graph.

        Nodes can have many more signals than we actually care about for structuring the graph,
        this set is only the ones that we care about.
        """
        return {(e.source_node, e.source_signal) for e in self.edges}

    def add_epoch(self, epoch: int | Epoch | None = None) -> Epoch:
        """
        Add another epoch with a prepared graph to the scheduler.
        """
        if epoch is not None:
            if isinstance(epoch, int):
                this_epoch = Epoch(epoch)
            elif isinstance(epoch, Epoch):
                this_epoch = epoch
            else:
                raise TypeError("Can only create an epoch from an epoch or integer")
            # ensure that the next iteration of the clock will return the next number
            # if we create epochs out of order
            self._clock = count(
                max([this_epoch[0].epoch, *[ep[0].epoch for ep in self._epochs], *self._epoch_log])
                + 1
            )
        else:
            this_epoch = Epoch(next(self._clock))

        if this_epoch in self._epochs:
            raise EpochExistsError(f"Epoch {this_epoch} is already scheduled")
        elif this_epoch in self._epoch_log:
            raise EpochCompletedError(f"Epoch {this_epoch} has already been completed!")

        graph = self._init_graph(epoch=this_epoch)
        self._epochs[this_epoch] = graph

        return this_epoch

    def add_subepoch(self, epoch: Epoch) -> Epoch:
        """
        Add subepoch!

        Creates a topo sorter with all the nodes downstream of the node that created the epoch.
        """

        if epoch.parent is None:
            raise ValueError(f"Cannot create a subepoch for root epoch {epoch}")

        parent_epoch = self[epoch.parent]
        sorter = self._init_graph(epoch)

        # mark any nodes that are completed in the parent as completed in the subepoch
        # EXCEPT don't expire the node that induced the subepoch or its signals -
        # we expect that the subepoch is typically created during an `update` call
        # where we'll be handling done or expiredness of the signals separately.
        parent_deps = set(sorter.node_info)
        exclude_current = sorter.signals[epoch[-1].node_id] | {epoch[-1].node_id}
        for parent_dep in parent_deps:
            if parent_dep in parent_epoch.ran_nodes:
                sorter.done(parent_dep)
            elif parent_dep in parent_epoch.done_nodes and parent_dep not in exclude_current:
                sorter.mark_expired(parent_dep, unlock_optionals=False)
            elif parent_dep in parent_epoch.out_nodes:
                sorter.mark_out(parent_dep)

        self._epochs[epoch] = sorter
        for parent in epoch.parents:
            self._subepochs[parent].add(epoch)

        # a node inducing subepochs expires the node in the (immediate) parent epoch.
        # this is bookkeeping, not cancellation: the node's work continues in the subepochs
        if epoch[-1].node_id not in parent_epoch.done_nodes:
            self.expire(
                epoch.parent,
                epoch[-1].node_id,
                with_signals=False,
                unlock_optionals=False,
                cascade=False,
            )

        return epoch

    def is_active(self, epoch: Epoch | None = None) -> bool:
        """
        Graph remains active while it holds at least one epoch that is active.
        """
        if epoch is not None:
            if epoch not in self._epochs:
                # if an epoch has been completed and had its graph cleared, it's no longer active
                # if an epoch has not been started, it is also not active.
                return False
            return any(self._epochs[e].is_active() for e in {*self._subepochs[epoch], epoch})
        else:
            return any(graph.is_active() for graph in self._epochs.values())

    def get_ready(
        self, epoch: Epoch | None = None, node_id: NodeID | None = None
    ) -> list[MetaEvent]:
        """
        Output the set of nodes that are ready across different epochs.

        Args:
            epoch (Epoch | None): if an Epoch, get ready events for that epoch,
                if ``None`` , get ready events for all epochs.
            node_id (str | None): If present, only get ready events for a single node
        """

        if epoch is not None:
            graphs = [
                (ep, self._epochs[ep])
                for ep in {*self._subepochs.get(epoch, set()), epoch}
                if ep in self._epochs
            ]
        else:
            graphs = list(self._epochs.items())
        graphs = sorted(
            graphs, key=lambda g: (tuple(e.node_id for e in g[0]), tuple(e.epoch for e in g[0]))
        )

        ready_nodes = []
        for epoch, graph in graphs:
            if node_id is None:
                candidates = tuple(graph.ready_nodes)
            else:
                candidates = tuple(node for node in graph.ready_nodes if node == node_id)
            for node in candidates:
                if isinstance(node, NodeSignal):
                    self._logger.warning(
                        "Scheduler attempted to return signal tuple %s in %s - "
                        "something is wrong with how the graph is instantiated or run, "
                        "or a node is emitting incorrect events manually, "
                        "all signals should be marked done/expired by events passed in `update`. "
                        "Ignoring - nodes downstream of this signal will not run.",
                        node,
                        epoch,
                    )
                    graph.mark_expired(node)
                    continue
                if self._withheld(node, epoch):
                    # stateful nodes run in strict epoch order:
                    # leave the node ready, to be returned once earlier epochs complete
                    continue
                graph.mark_out(node)
                if node in _VIRTUAL_NODES or (node not in self.nodes or self.nodes[node].enabled):
                    ready_nodes.append(
                        MetaEvent(
                            id=uuid4().int,
                            timestamp=datetime.now(),
                            node_id="meta",
                            signal=MetaEventType.NodeReady,
                            epoch=epoch,
                            value=node,
                        )
                    )
        return ready_nodes

    def node_is_ready(self, node: NodeID, epoch: Epoch | None = None) -> bool:
        """
        Check if a single node is ready in a single or any epoch

        Args:
            node (NodeID): the node to check
            epoch (int | None): the epoch to check, if ``None`` , any epoch
        """
        # slight duplication of the above because we don't want to *get* the ready nodes,
        # which marks them as "out" in the TopoSorter

        # if we've already run this, the node is ready - don't create another epoch
        if epoch in self._epoch_log:
            return True

        graphs = (
            self._epochs.items()
            if epoch is None
            else [(ep, self[ep]) for ep in [epoch, *self._subepochs[epoch]]]
        )
        is_ready = any(node in graph.ready_nodes for epoch, graph in graphs)
        return is_ready

    def node_is_done(self, node: NodeID, epoch: Epoch) -> bool:
        """Node is expired or done in specified epoch"""
        if epoch in self._epoch_log:
            return True

        if self._subepochs[epoch]:
            return all(node in self._epochs[e].done_nodes for e in self._subepochs[epoch] | {epoch})
        else:
            return node in self._epochs[epoch].done_nodes

    # --------------------------------------------------
    # statefulness - stateful nodes run in strict epoch order
    # --------------------------------------------------

    def _withheld(self, node: NodeID, epoch: Epoch) -> bool:
        """
        Whether a node that is ready in the sorter graph should be withheld from running:
        stateful nodes run in strict epoch order,
        both across root epochs and across sibling subepochs.
        """
        spec = self.nodes.get(node)
        if spec is None or not spec.stateful:
            return False
        if self._stateful_watermark(node) < epoch.root[0].epoch:
            return True
        if len(epoch) > 1:
            # sibling subepochs run in order too
            chain, index = epoch[-1].node_id, epoch[-1].epoch
            parent = epoch.parent
            for sibling in self._subepochs.get(parent, ()):  # type: ignore[arg-type]
                if (
                    sibling[-1].node_id == chain
                    and sibling[-1].epoch < index
                    and sibling in self._epochs
                    and not self._node_complete_in_graph(node, sibling)
                ):
                    return True
        return False

    def _stateful_watermark(self, node: NodeID) -> int:
        """
        The lowest root epoch the node has not yet completed:
        all root epochs below the watermark are complete for the node.
        Monotonic, advanced lazily as epochs complete.
        """
        progress = self._progress.get(node, 0)
        while self._root_complete(node, progress):
            progress += 1
        self._progress[node] = progress
        return progress

    def _root_complete(self, node: NodeID, root: int) -> bool:
        """The node is done/expired in the given root epoch and all of its subepochs"""
        if root in self._epoch_log:
            return True
        epoch = Epoch(root)
        if epoch not in self._epochs:
            return False
        return self._node_complete_in(node, epoch)

    def _node_complete_in(self, node: NodeID, epoch: Epoch) -> bool:
        """
        The node is done/expired in the given epoch and all of its subepochs,
        only counting graphs the node is a member of
        (subepoch subgraphs may not contain the node at all).
        """
        if epoch.root[0].epoch in self._epoch_log:
            return True
        if epoch not in self._epochs:
            return False
        for e in (epoch, *self._subepochs.get(epoch, ())):  # type: ignore[arg-type]
            if e in self._epochs and not self._node_complete_in_graph(node, e):
                return False
        return True

    def _node_complete_in_graph(self, node: NodeID, epoch: Epoch) -> bool:
        graph = self._epochs[epoch]
        return node not in graph.node_info or node in graph.done_nodes

    # --------------------------------------------------
    # cancellation metaevents
    # --------------------------------------------------

    def _record_canceled(self, epoch: Epoch, expired: tuple[GraphItem, ...]) -> None:
        """
        Record ``NodeCanceled`` MetaEvents for nodes expired by a cancellation cascade,
        to be returned from :meth:`.update` alongside the events that caused them.
        """
        for item in expired:
            if isinstance(item, NodeSignal) or item in _VIRTUAL_NODES:
                continue
            self._pending_meta.append(self._meta(MetaEventType.NodeCanceled, epoch, item))

    def _drain_meta(self) -> list[MetaEvent]:
        pending = self._pending_meta
        self._pending_meta = []
        return pending

    @staticmethod
    def _meta(signal: MetaEventType, epoch: Epoch, value: Any) -> MetaEvent:
        return MetaEvent(
            id=uuid4().int,
            timestamp=datetime.now(UTC),
            node_id="meta",
            signal=signal,
            epoch=epoch,
            value=value,
        )

    # --------------------------------------------------
    # run control & iteration
    # --------------------------------------------------

    def queue_epoch(self, epoch: Epoch | int) -> Epoch:
        """
        Grant permission to run a specific (root) epoch,
        i.e. when receiving a ``process`` call for that epoch.
        """
        if isinstance(epoch, int):
            epoch = Epoch(epoch)
        root = epoch.root[0].epoch
        if root not in self._run_queue and root not in self._epoch_log:
            self._run_queue.append(root)
        self._grant_clock = count(max(next(self._grant_clock), root + 1))
        return epoch

    def queue_epochs(self, n: int) -> list[Epoch]:
        """
        Grant permission to run the next ``n`` epochs,
        i.e. when receiving a bounded ``start(n)`` call.
        """
        granted = []
        for _ in range(n):
            i = next(self._grant_clock)
            if i not in self._run_queue:
                self._run_queue.append(i)
            granted.append(Epoch(i))
        return granted

    def set_freerun(self, enabled: bool) -> None:
        """
        Grant (or revoke) permission to run any epoch as soon as its inputs are
        available, creating new epochs as previous ones complete -
        i.e. when receiving an unbounded ``start`` (or a ``stop`` ).
        """
        self._freerun = enabled

    def iter_epoch(
        self, epoch: Epoch | int | None = None, node_id: NodeID | None = None
    ) -> Iterator[MetaEvent | None]:
        """
        Iterate scheduling events for a single epoch, until the epoch is no longer active.

        Yields ``NodeReady`` :class:`.MetaEvent` s as nodes become ready to run,
        and ``None`` when no more progress can be made until the scheduler is
        updated - concurrent callers should wait for an update
        (in whatever way is appropriate for them) and resume iterating.

        Args:
            epoch (Epoch | int | None): The epoch to iterate. If ``None`` ,
                the most recently created root epoch.
            node_id (str | None): If present, only yield events for the given node.
        """
        if epoch is None:
            roots = [e for e in self._epochs if len(e) == 1]
            if not roots:
                return
            epoch = max(roots, key=lambda e: e[0].epoch)
        elif isinstance(epoch, int):
            epoch = Epoch(epoch)

        while self.is_active(epoch):
            batch = self.get_ready(epoch, node_id)
            if batch:
                yield from batch
            else:
                yield None

    def iter_events(self, node_id: NodeID | None = None) -> Iterator[MetaEvent | None]:
        """
        Iterate scheduling events until stopped.

        Runs epochs as granted by :meth:`.queue_epoch` , :meth:`.queue_epochs` ,
        and :meth:`.set_freerun` , creating their graphs as needed
        (in freerun mode, new epochs are created as previous ones complete).
        Granted epochs are served in the order permission was granted;
        stateful nodes additionally run in strict epoch order (see :meth:`.get_ready` ).

        Yields ``NodeReady`` and ``EpochEnded`` :class:`.MetaEvent` s, and ``None``
        when no more progress can be made until the scheduler is updated or granted
        more run permission - callers should wait and resume iterating.
        The iterator never ends: stopping is the caller's decision
        (e.g. on a ``stop`` command, or by closing the generator).

        When iterating for a single node (the zmq runner case, where each node
        runner sees only the events of its direct upstream), epochs are ended
        once the node has completed in them - the rest of the graph is not
        observable, so the epoch's lifetime locally is the node's participation.

        Args:
            node_id (str | None): If present, only yield events for the given node,
                and manage epoch lifecycle from that node's point of view.
        """
        pending_end: set[Epoch] = set()
        while True:
            # create graphs for granted epochs so that source nodes become ready
            for granted in list(self._run_queue):
                if granted in self._epoch_log:
                    with contextlib.suppress(ValueError):
                        self._run_queue.remove(granted)
                    continue
                if Epoch(granted) not in self._epochs:
                    with contextlib.suppress(EpochExistsError, EpochCompletedError):
                        self.add_epoch(granted)

            # in freerun, create the next epoch once everything else has completed
            if self._freerun:
                if node_id is None:
                    exhausted = not self.is_active()
                else:
                    exhausted = all(
                        self._node_complete_in_graph(node_id, e) for e in list(self._epochs)
                    )
                if exhausted:
                    nxt = next(self._grant_clock)
                    while nxt in self._epoch_log:
                        nxt = next(self._grant_clock)
                    with contextlib.suppress(EpochExistsError, EpochCompletedError):
                        self.add_epoch(nxt)

            # epoch lifecycle, before serving more work:
            # when iterating for a single node, the rest of the graph is not
            # locally observable, so end epochs once the node completes in them
            if node_id is not None:
                ended = False
                for epoch in [e for e in self._epochs if len(e) == 1]:
                    if self._node_complete_in(node_id, epoch) and not self.epoch_completed(epoch):
                        self.end_epoch(epoch)
                        pending_end.add(epoch)
                for epoch in sorted(pending_end, key=lambda e: e[0].epoch):
                    if self.epoch_completed(epoch):
                        pending_end.discard(epoch)
                        ended = True
                        yield self._meta(MetaEventType.EpochEnded, epoch, epoch)
                if ended:
                    # re-derive state: the caller may have reacted to the endings
                    continue

            # serve one batch from granted epochs in grant order,
            # falling back to anything ready when freerunning.
            # an incomplete granted epoch blocks later grants -
            # cancellation (NodeCanceled) completes epochs that can never run.
            # stateful nodes run in epoch order, so their grants are served sorted;
            # stateless nodes honor the order permission was granted.
            spec = self.nodes.get(node_id) if node_id is not None else None
            stateful = spec is not None and bool(spec.stateful)
            grant_order = (
                sorted(self._run_queue) if node_id is None or stateful else list(self._run_queue)
            )
            batch: list[MetaEvent] = []
            batch_epoch: Epoch | None = None
            for granted in grant_order:
                target = Epoch(granted)
                if target not in self._epochs:
                    continue
                batch = self.get_ready(target, node_id)
                if batch:
                    batch_epoch = target
                    break
                if node_id is not None and not self._node_complete_in(node_id, target):
                    # blocked waiting for this grant's inputs
                    break
            if not batch and self._freerun:
                roots = sorted((e for e in self._epochs if len(e) == 1), key=lambda e: e[0].epoch)
                for epoch in roots:
                    batch = self.get_ready(epoch, node_id)
                    if batch:
                        batch_epoch = epoch
                        break

            if batch and batch_epoch is not None:
                pending_end.add(batch_epoch)
                yield from batch
                continue

            yield None

    def __getitem__(self, epoch: Epoch | int) -> TopoSorter:
        if epoch == -1:
            if len(self._epochs) == 1:
                return next(iter(self._epochs.values()))
            else:
                max_epoch = max(*[e[0].epoch for e in self._epochs])
                return self._epochs[Epoch(max_epoch)]
        elif isinstance(epoch, int):
            epoch = Epoch(epoch)

        if epoch not in self._epochs:
            if len(epoch) == 1:
                self.add_epoch(epoch)
            else:
                self.add_subepoch(epoch)

        return self._epochs[epoch]

    def sources_finished(self, epoch: Epoch | None = None) -> bool:
        """
        Check the source nodes of the given epoch have been processed.
        If epoch is None, check the source nodes of the latest epoch.

        """
        if epoch is None and len(self._epochs) == 0:
            return True

        graph = self[-1] if epoch is None else self._epochs[epoch]
        return all(src in graph.done_nodes for src in self.source_nodes)

    def update(
        self, events: MutableSequence[Event | MetaEvent] | MutableSequence[Event]
    ) -> MutableSequence[Event] | MutableSequence[Event | MetaEvent]:
        """
        When a set of events are received, update the graphs within the scheduler.
        Currently only has :meth:`TopoSorter.done` implemented.

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
                # FIXME: This exception suppression is a *bit* broad - fix underlying issue
                # The zmq runner has an incomplete graph, and so sometimes we don't have
                # all the nodes in the graph when we go to mark the node done.
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

        ret_events = [*events, *self._drain_meta(), *end_events]

        return ret_events

    def done(
        self,
        epoch: Epoch,
        node_id: str,
        signal: SignalName | None = None,
        with_signals: bool = True,
    ) -> MetaEvent | None:
        """
        Mark a node in a given epoch as done.

        Args:
            with_signals (bool): When marking this node as done, also mark all its signals as done.
        """
        if epoch[0].epoch in self._epoch_log:
            self._logger.debug(
                "Marking node %s as done in epoch %s, " "but epoch was already completed. ignoring",
                node_id,
                epoch,
            )
            return None

        to_mark = NodeSignal(node_id, signal) if signal is not None else node_id
        try:
            self[epoch].done(to_mark)
        except AlreadyDoneError as e:
            if not self._subepochs[epoch]:
                raise AlreadyDoneError(f"Node {node_id} already done in {epoch}") from e

        self._done_subepochs(epoch, node_id, signal)
        for parent in epoch.parents:
            self[parent].mark_expired(to_mark, unlock_optionals=False)

        if signal is None and with_signals:
            self[epoch].done(*self[epoch].signals[node_id].difference(self[epoch].done_nodes))

        if not self.is_active(epoch):
            return self.end_epoch(epoch)
        return None

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
        i.e. when the node emitted ``NoEvent``

        Args:
            cascade (bool): If ``True`` (default), transitively expire nodes that can
                never run because their required predecessors expired without running -
                a ``NoEvent`` cancels everything downstream for the epoch.
                ``NodeCanceled`` MetaEvents are emitted for the canceled nodes
                (returned from :meth:`.update` ).
                Use ``False`` for bookkeeping expirations
                (e.g. when forking subepochs), where downstream nodes run elsewhere.
        """
        to_mark = NodeSignal(node_id, signal) if signal is not None else node_id
        expired = self[epoch].mark_expired(
            to_mark, unlock_optionals=unlock_optionals, cascade=cascade
        )
        if cascade:
            self._record_canceled(epoch, expired)
        # if any immediate successors are already marked as "ready," we also want to cancel them.
        if info := self[epoch].node_info.get(to_mark):
            for successor in info.successors:
                self[epoch].ready_nodes.discard(successor)
        if signal is None and with_signals:
            for graph_node in self[epoch].signals[node_id]:
                self.expire(
                    epoch,
                    node_id=node_id,
                    signal=graph_node[1],
                    unlock_optionals=unlock_optionals,
                    cascade=cascade,
                )

        if not self.is_active(epoch):
            return self.end_epoch(epoch)

        return None

    def epoch_completed(self, epoch: Epoch) -> bool:
        """
        Check if the epoch has been completed.
        """
        previously_completed = (
            len(self._epoch_log) > 0
            and epoch not in self._epochs
            and (epoch in self._epoch_log or epoch < min(self._epoch_log))
        )
        active_completed = epoch in self._epochs and not any(
            self._epochs[ep].is_active() for ep in [epoch, *self._subepochs[epoch]]
        )
        return previously_completed or active_completed

    def end_epoch(self, epoch: Epoch | int | None = None) -> MetaEvent | None:
        if epoch is None or epoch == -1:
            if len(self._epochs) == 0:
                return None
            ep = list(self._epochs)[-1]
        elif isinstance(epoch, int):
            ep = Epoch(epoch)
        elif isinstance(epoch, Epoch):
            ep = epoch
        else:
            raise TypeError("Can only end an epoch with an integer or Epoch")
        self._logger.debug("Ending epoch %s", ep)
        if len(ep) == 1:
            self._epoch_log.append(ep[0].epoch)
            for subep in {ep, *self._subepochs[ep]}:
                with contextlib.suppress(KeyError):
                    del self._epochs[subep]

        return MetaEvent(
            id=uuid4().int,
            timestamp=datetime.now(UTC),
            node_id="meta",
            signal=MetaEventType.EpochEnded,
            epoch=ep,
            value=ep,
        )

    def enable_node(self, node_id: str) -> None:
        """
        Enable edges attached to the node and the
        NodeSpecification enable switches to True

        """
        self.nodes[node_id].enabled = True
        self._frozen_sorters = {}

    def disable_node(self, node_id: str) -> None:
        """
        Disable edges attached to the node and the
        NodeSpecification enable switches to False

        """
        self.nodes[node_id].enabled = False
        self._frozen_sorters = {}
        for graph in self._epochs.values():
            graph.mark_expired(node_id)

    def clear(self) -> None:
        """
        Remove epoch records and run state, restarting the scheduler
        """
        self._epochs = {}
        self._epoch_log = deque(maxlen=100)
        self._freerun = False
        self._run_queue = []
        self._grant_clock = count()
        self._progress = {}
        self._pending_meta = []

    def _init_graph(self, epoch: Epoch | None = None) -> TopoSorter:
        """
        Produce a :class:`.TopoSorter` based on the graph induced by
        a set of :class:`.Node` and a set of :class:`.Edge` that yields node ids.
        """
        frozen_key = ("tube",) if epoch is None else tuple(e.node_id for e in epoch)

        if frozen_key not in self._frozen_sorters:
            if epoch and epoch.parent:
                nodes, edges = self._subgraph(epoch[-1].node_id)
                sorter = TopoSorter(nodes, edges)
            else:
                sorter = TopoSorter(self.nodes, self.edges)
            self._frozen_sorters[frozen_key] = sorter

        return deepcopy(self._frozen_sorters[frozen_key])

    def has_cycle(self) -> bool:
        """
        Checks that the graph is acyclic.
        """
        graph = self._init_graph()
        cycle = graph.find_cycle()
        return bool(cycle)

    def generations(self) -> list[tuple[GraphItem, ...]]:
        """
        Get the topological generations of the graph:
        tuples for each set of nodes that can be run at the same time.

        Order within a generation is not guaranteed to be stable.
        """
        sorter = self._init_graph()
        generations = []
        while sorter.is_active():
            ready = sorter.get_ready()
            generations.append(ready)
            sorter.done(*ready)
        return generations

    def asset_generations(self) -> dict[NodeID, list[tuple[str, ...]]]:
        """
        :meth:`.generations` except only including nodes with direct dependencies on assets,
        to determine when the asset should be initialized vs. received in the ZMQ Runner.

        Packed in a dictionary with the asset ID as the key,
        and the value as the generations for that asset.
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

    def upstream_nodes(self, node: NodeID) -> set[NodeID]:
        """
        All the nodes that have an effect on the given node

        From:
        * Dependencies
        * If the node has optional dependencies, nodes whose NoEvents it should listen to
        """
        upstream = {e.source_node for e in self.edges if e.target_node == node}
        sorter = self._init_graph()
        for item, info in sorter.node_info.items():
            if node in info.optional_successors:
                upstream.add(item[0] if isinstance(item, NodeSignal) else item)
        return upstream

    def _subgraph(self, node_id: str) -> tuple[dict[str, NodeSpecification], list[Edge]]:
        """
        Subgraph that is downstream of a given node (including the node itself).
        """
        from noob.tube import downstream_nodes

        if node_id not in self._subgraphs:
            downstream = downstream_nodes(self.edges, node_id)
            self._subgraphs[node_id] = (
                {node_id: self.nodes[node_id] for node_id in downstream if node_id in self.nodes},
                [e for e in self.edges if e.target_node in downstream],
            )
        return self._subgraphs[node_id]

    def _done_subepochs(
        self, epoch: Epoch, node_id: NodeID, signal: SignalName | None = None
    ) -> None:
        """
        Called when a node in a parent epoch is marked done -
        mark the node done in all subepochs,
        but ensure that nodes that are exclusively downstream of this node
        (i.e. no dependencies on nodes within the mapped subepoch)
        are removed from the graph.

        This is to support gather-like operations from non-gather nodes in 3rd party tubes:
        nodes downstream of both this node and other nodes in the subepoch run in subepochs,
        but nodes that are exclusively downstream of this node only run in the parent epoch
        """
        from noob.tube import downstream_nodes

        if not self._subepochs[epoch]:
            return

        our_subgraph = set(self._subgraph(node_id)[0])
        _exclusive_subgraphs = {}

        to_mark = NodeSignal(node_id, signal) if signal is not None else node_id

        for subepoch in self._subepochs[epoch]:
            if (
                to_mark in self._epochs[subepoch].ran_nodes
                or to_mark not in self._epochs[subepoch].node_info
            ):
                # fine
                continue
            elif to_mark in self._epochs[subepoch].done_nodes:
                # needs to be resurrected
                self._epochs[subepoch].resurrect(to_mark)

            self._epochs[subepoch].done(to_mark)

            # mark all nodes that are exclusively downstream of this node expired
            subep_node = subepoch[-1].node_id
            if subep_node not in _exclusive_subgraphs:
                _exclusive_subgraphs[subep_node] = downstream_nodes(
                    self.edges, subep_node, exclude={node_id}
                )
            exclusive_subgraph = our_subgraph - _exclusive_subgraphs[subep_node] - {node_id}

            for exclusive in exclusive_subgraph:
                self._epochs[subepoch].mark_expired(exclusive)


PythonScheduler = Scheduler
"""The pure-python scheduler, always available regardless of the active implementation."""

if os.environ.get("NOOB_SCHEDULER", "").lower() not in ("python", "py"):
    # use the optimized rust scheduler when the optional noob-core package
    # is installed. set NOOB_SCHEDULER=python to force the pure-python one.
    with contextlib.suppress(ImportError):
        from noob_core import RustScheduler as Scheduler  # noqa: F811
