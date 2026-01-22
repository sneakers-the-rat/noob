import logging
from collections import deque
from collections.abc import MutableSequence
from datetime import UTC, datetime
from itertools import count
from threading import Condition
from typing import Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from noob.const import META_SIGNAL
from noob.event import Event, MetaEvent, MetaEventType, MetaSignal
from noob.exceptions import EpochCompletedError, EpochExistsError, NotOutYetError
from noob.logging import init_logger
from noob.node import Edge, NodeSpecification
from noob.toposort import TopoSorter
from noob.types import Epoch, NodeID, epoch_parent, make_root_epoch, make_sub_epoch

_VIRTUAL_NODES = ("input", "assets")
"""
Virtual nodes that don't actually exist as nodes,
but can be depended on 
(and can be present or absent, and so shouldn't be marked as trivially done)
"""


class Scheduler(BaseModel):
    nodes: dict[str, NodeSpecification]
    edges: list[Edge]
    source_nodes: list[NodeID] = Field(default_factory=list)
    logger: logging.Logger = Field(default_factory=lambda: init_logger("noob.scheduler"))

    _clock: count = PrivateAttr(default_factory=count)
    _epochs: dict[Epoch, TopoSorter] = PrivateAttr(default_factory=dict)
    _ready_condition: Condition = PrivateAttr(default_factory=Condition)
    _epoch_condition: Condition = PrivateAttr(default_factory=Condition)
    _epoch_log: deque[Epoch] = PrivateAttr(default_factory=lambda: deque(maxlen=100))
    _subepoch_counts: dict[Epoch, dict[str, int]] = PrivateAttr(default_factory=dict)
    """Tracks expected sub-epoch counts per parent epoch per map node"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def from_specification(cls, nodes: dict[str, NodeSpecification], edges: list[Edge]) -> Self:
        """
        Create an instance of a Scheduler from :class:`.NodeSpecification` and :class:`.Edge`

        """
        return cls(nodes=nodes, edges=edges)

    @model_validator(mode="after")
    def get_sources(self) -> Self:
        """
        Get the IDs of the nodes that do not depend on other nodes.

        * `input` nodes are special implicit source nodes. Other nodes
        * CAN depend on it and still be a source node.

        """
        if not self.source_nodes:
            graph = self._init_graph(nodes=self.nodes, edges=self.edges)
            self.source_nodes = [id_ for id_ in graph.ready_nodes if id_ not in _VIRTUAL_NODES]
        return self

    def add_epoch(self, epoch: Epoch | None = None) -> Epoch:
        """
        Add another epoch with a prepared graph to the scheduler.

        Args:
            epoch: If provided, use this epoch. Otherwise, create a new root epoch
                with the next clock value.

        Returns:
            The epoch that was added.
        """
        with self._ready_condition:
            if epoch is not None:
                this_epoch = epoch
                # ensure that the next iteration of the clock will return the next number
                # if we create epochs out of order
                root_indices = [e[0][0] for e in list(self._epochs.keys()) + list(self._epoch_log)]
                if root_indices:
                    max_root = max(root_indices)
                    if epoch[0][0] >= max_root:
                        self._clock = count(epoch[0][0] + 1)
            else:
                this_epoch = make_root_epoch(next(self._clock))

            if this_epoch in self._epochs:
                raise EpochExistsError(f"Epoch {this_epoch} is already scheduled")
            elif this_epoch in self._epoch_log:
                raise EpochCompletedError(f"Epoch {this_epoch} has already been completed!")

            graph = self._init_graph(nodes=self.nodes, edges=self.edges)
            self._epochs[this_epoch] = graph
            self._ready_condition.notify_all()
        return this_epoch

    def add_sub_epochs(
        self, parent_epoch: Epoch, count: int, map_node_id: str
    ) -> list[Epoch]:
        """
        Create sub-epochs for a map expansion.

        Sub-epochs only contain the subgraph downstream of the map node,
        since upstream nodes have already run in the parent epoch.

        Args:
            parent_epoch: The epoch the map node was called in
            count: Number of items being mapped over
            map_node_id: ID of the map node creating these sub-epochs

        Returns:
            List of sub-epochs created
        """
        sub_epochs = []
        with self._ready_condition:
            # Track expected count for gather nodes
            if parent_epoch not in self._subepoch_counts:
                self._subepoch_counts[parent_epoch] = {}
            self._subepoch_counts[parent_epoch][map_node_id] = count

            # Find all nodes downstream of the map node (including map itself)
            downstream_nodes = self._get_downstream_nodes(map_node_id)

            # Filter nodes and edges to only downstream ones
            filtered_nodes = {
                k: v for k, v in self.nodes.items() if k in downstream_nodes
            }
            filtered_edges = [
                e for e in self.edges
                if e.source_node in downstream_nodes and e.target_node in downstream_nodes
            ]

            for i in range(count):
                sub_epoch = make_sub_epoch(parent_epoch, i, map_node_id)
                if sub_epoch not in self._epochs:
                    # Create a subgraph with only downstream nodes
                    graph = self._init_graph(nodes=filtered_nodes, edges=filtered_edges)
                    # Mark the map node as done - it already emitted in this sub-epoch
                    graph.mark_out(map_node_id)
                    graph.done(map_node_id)
                    self._epochs[sub_epoch] = graph

                    # If the graph is already inactive (no more nodes to process),
                    # end the epoch immediately. This happens for innermost maps
                    # that have no downstream nodes in the sub-epoch graph.
                    if not graph.is_active():
                        self.end_epoch(sub_epoch)

                sub_epochs.append(sub_epoch)
            self._ready_condition.notify_all()
        return sub_epochs

    def _get_downstream_nodes(self, node_id: str, exclude_sinks: bool = True) -> set[str]:
        """
        Get all nodes downstream of a given node (including the node itself).

        Uses BFS to find all nodes reachable from the given node via edges.

        Args:
            node_id: Starting node
            exclude_sinks: If True, exclude sink nodes (nodes with no successors,
                like Return) from the result. Sinks should run in the parent epoch
                to collect accumulated results.
        """
        # Build adjacency list: source -> targets
        adjacency: dict[str, list[str]] = {}
        for edge in self.edges:
            if edge.source_node not in adjacency:
                adjacency[edge.source_node] = []
            adjacency[edge.source_node].append(edge.target_node)

        # BFS from node_id
        downstream = {node_id}
        queue = [node_id]
        while queue:
            current = queue.pop(0)
            for successor in adjacency.get(current, []):
                if successor not in downstream:
                    downstream.add(successor)
                    queue.append(successor)

        if exclude_sinks:
            # Remove sink nodes (no outgoing edges) - they should run in parent epoch
            sinks = {n for n in downstream if n not in adjacency or not adjacency[n]}
            downstream -= sinks

        return downstream

    def _get_subepoch_nodes(self, map_node_id: str) -> set[str]:
        """
        Get nodes that run in sub-epochs created by a map node.

        This is similar to _get_downstream_nodes but stops at Gather nodes.
        Nodes downstream of a Gather run in the parent epoch after gather,
        not in sub-epochs.

        Args:
            map_node_id: The map node that created the sub-epochs
        """
        # Build adjacency list: source -> targets
        adjacency: dict[str, list[str]] = {}
        for edge in self.edges:
            if edge.source_node not in adjacency:
                adjacency[edge.source_node] = []
            adjacency[edge.source_node].append(edge.target_node)

        # BFS from map_node_id, stopping at Gather nodes (include them but not their successors)
        subepoch_nodes = {map_node_id}
        queue = [map_node_id]
        while queue:
            current = queue.pop(0)
            # If current is a Gather, don't traverse its successors
            # (Gather runs in sub-epochs but its output goes to parent)
            if current != map_node_id and self.nodes[current].type_ == "gather":
                continue
            for successor in adjacency.get(current, []):
                if successor not in subepoch_nodes:
                    subepoch_nodes.add(successor)
                    queue.append(successor)

        # Remove sink nodes - they should run in parent epoch
        sinks = {n for n in subepoch_nodes if n not in adjacency or not adjacency[n]}
        subepoch_nodes -= sinks

        return subepoch_nodes

    def get_expected_subepoch_count(self, parent_epoch: Epoch, map_node_id: str) -> int | None:
        """Get the expected number of sub-epochs for a given parent and map node."""
        if parent_epoch not in self._subepoch_counts:
            return None
        return self._subepoch_counts[parent_epoch].get(map_node_id)

    def is_active(self, epoch: Epoch | None = None) -> bool:
        """
        Graph remains active while it holds at least one epoch that is active.

        An epoch with pending sub-epochs is considered active even if its
        TopoSorter reports no more work.
        """
        if epoch is not None:
            if epoch not in self._epochs:
                # if an epoch has been completed and had its graph cleared, it's no longer active
                # if an epoch has not been started, it is also not active.
                return False
            # Check if this epoch has pending sub-epochs
            if epoch in self._subepoch_counts and self._subepoch_counts[epoch]:
                return True
            return self._epochs[epoch].is_active()
        else:
            return any(self.is_active(ep) for ep in self._epochs)

    def get_ready(self, epoch: Epoch | None = None) -> list[MetaEvent]:
        """
        Output the set of nodes that are ready across different epochs.

        Args:
            epoch (Epoch | None): if an Epoch, get ready events for that epoch,
                if ``None`` , get ready events for all epochs.
        """

        graphs = self._epochs.items() if epoch is None else [(epoch, self._epochs[epoch])]

        with self._ready_condition:
            ready_nodes = [
                MetaEvent(
                    id=uuid4().int,
                    timestamp=datetime.now(),
                    node_id="meta",
                    signal=MetaEventType.NodeReady,
                    epoch=ep,
                    value=node_id,
                )
                for ep, graph in graphs
                for node_id in graph.get_ready()
                if node_id in _VIRTUAL_NODES or self.nodes[node_id].enabled
            ]

        return ready_nodes

    def node_is_ready(self, node: NodeID, epoch: Epoch | None = None) -> bool:
        """
        Check if a single node is ready in a single or any epoch

        Args:
            node (NodeID): the node to check
            epoch (Epoch | None): the epoch to check, if ``None`` , any epoch
        """
        # slight duplication of the above because we don't want to *get* the ready nodes,
        # which marks them as "out" in the TopoSorter

        # if we've already run this, the node is ready - don't create another epoch
        if epoch in self._epoch_log:
            return True

        graphs = self._epochs.items() if epoch is None else [(epoch, self[epoch])]
        is_ready = any(node_id == node for ep, graph in graphs for node_id in graph.ready_nodes)
        return is_ready

    def __getitem__(self, epoch: Epoch) -> TopoSorter:
        if epoch not in self._epochs:
            self.add_epoch(epoch)
        return self._epochs[epoch]

    def sources_finished(self, epoch: Epoch | None = None) -> bool:
        """
        Check the source nodes of the given epoch have been processed.
        If epoch is None, check the source nodes of the latest epoch.

        """
        if epoch is None and len(self._epochs) == 0:
            return True

        if epoch is None:
            # Get the most recent root epoch
            root_epochs = [e for e in self._epochs.keys() if len(e) == 1]
            if not root_epochs:
                return True
            epoch = max(root_epochs, key=lambda e: e[0][0])

        graph = self._epochs[epoch]
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
        with self._ready_condition, self._epoch_condition:
            marked_done = set()
            for e in events:
                if (done_marker := (e["epoch"], e["node_id"])) in marked_done or e[
                    "node_id"
                ] == "meta":
                    continue
                else:
                    marked_done.add(done_marker)

                if e["signal"] == META_SIGNAL and e["value"] == MetaSignal.NoEvent:
                    epoch_ended = self.expire(epoch=e["epoch"], node_id=e["node_id"])
                else:
                    epoch_ended = self.done(epoch=e["epoch"], node_id=e["node_id"])

                if epoch_ended:
                    end_events.append(epoch_ended)

            # condition uses an RLock, so waiters only run here,
            # even though `done` also notifies.
            self._ready_condition.notify_all()

        ret_events = [*events, *end_events]

        return ret_events

    def done(self, epoch: Epoch, node_id: str) -> MetaEvent | None:
        """
        Mark a node in a given epoch as done.

        """
        with self._ready_condition, self._epoch_condition:
            if epoch in self._epoch_log:
                self.logger.debug(
                    "Marking node %s as done in epoch %s, "
                    "but epoch was already completed. ignoring",
                    node_id,
                    epoch,
                )
                return None

            try:
                self[epoch].done(node_id)
            except NotOutYetError:
                # in parallel mode, we don't `get_ready` the preceding ready nodes
                # so we have to manually mark them as "out"
                self[epoch].mark_out(node_id)
                self[epoch].done(node_id)

            self._ready_condition.notify_all()
            if not self.is_active(epoch):
                return self.end_epoch(epoch)
        return None

    def expire(self, epoch: Epoch, node_id: str) -> MetaEvent | None:
        """
        Mark a node as having been completed without making its dependent nodes ready.
        i.e. when the node emitted ``NoEvent``
        """
        with self._ready_condition, self._epoch_condition:
            self[epoch].mark_expired(node_id)
            self._ready_condition.notify_all()
            if not self.is_active(epoch):
                return self.end_epoch(epoch)

        return None

    def await_node(self, node_id: NodeID, epoch: Epoch | None = None) -> MetaEvent:
        """
        Block until a node is ready

        Args:
            node_id:
            epoch (Epoch, None): if `Epoch` , wait until the node is ready in the given epoch,
                otherwise wait until the node is ready in any epoch

        Returns:

        """
        with self._ready_condition:
            if not self.node_is_ready(node_id, epoch):
                self._ready_condition.wait_for(lambda: self.node_is_ready(node_id, epoch))

            # be FIFO-like and get the earliest epoch the node is ready in
            if epoch is None:
                for ep in self._epochs:
                    if self.node_is_ready(node_id, ep):
                        epoch = ep
                        break

            if epoch is None:
                raise RuntimeError(
                    "Could not find ready epoch even though node ready condition passed, "
                    "something is wrong with the way node status checking is "
                    "locked between threads."
                )

            # mark just one event as "out."
            # threadsafe because we are holding the lock that protects graph mutation
            self._epochs[epoch].mark_out(node_id)

        return MetaEvent(
            id=uuid4().int,
            timestamp=datetime.now(),
            node_id="meta",
            signal=MetaEventType.NodeReady,
            epoch=epoch,
            value=node_id,
        )

    def await_epoch(self, epoch: Epoch | None = None) -> Epoch:
        """
        Block until an epoch is completed.

        Args:
            epoch (Epoch, None): if `Epoch` , wait until the epoch is ready,
                otherwise wait until the next epoch is finished, in whatever order.

        Returns:
            Epoch: the epoch that was completed.
        """
        with self._epoch_condition:
            # check if we have already completed this epoch
            if epoch is not None and self.epoch_completed(epoch):
                return epoch

            if epoch is None:
                self._epoch_condition.wait()
                return self._epoch_log[-1]
            else:
                self._epoch_condition.wait_for(lambda: self.epoch_completed(epoch))
                return epoch

    def epoch_completed(self, epoch: Epoch) -> bool:
        """
        Check if the epoch has been completed.
        """
        with self._epoch_condition:
            previously_completed = (
                len(self._epoch_log) > 0 and epoch not in self._epochs and epoch in self._epoch_log
            )
            active_completed = epoch in self._epochs and not self._epochs[epoch].is_active()
            return previously_completed or active_completed

    def end_epoch(self, epoch: Epoch | None = None) -> MetaEvent | None:
        if epoch is None:
            if len(self._epochs) == 0:
                return None
            epoch = list(self._epochs)[-1]

        with self._epoch_condition:
            self._epoch_condition.notify_all()
            self._epoch_log.append(epoch)
            if epoch in self._epochs:
                del self._epochs[epoch]

            # Check if this was a sub-epoch and all siblings are now complete
            self._check_subepoch_completion(epoch)

        return MetaEvent(
            id=uuid4().int,
            timestamp=datetime.now(UTC),
            node_id="meta",
            signal=MetaEventType.EpochEnded,
            epoch=epoch,
            value=epoch,
        )

    def _check_subepoch_completion(self, ended_epoch: Epoch) -> None:
        """
        When a sub-epoch ends, check if all sibling sub-epochs are complete.
        If so, mark the map node as done in the parent epoch so downstream
        nodes (like Return) become ready.
        """
        if len(ended_epoch) <= 1:
            # Not a sub-epoch
            return

        parent_epoch = ended_epoch[:-1]
        map_node_id = ended_epoch[-1][1]  # The node that created this sub-epoch

        # Check if we have tracking info for this map
        if parent_epoch not in self._subepoch_counts:
            return
        if map_node_id not in self._subepoch_counts[parent_epoch]:
            return

        expected_count = self._subepoch_counts[parent_epoch][map_node_id]

        # Count how many sub-epochs for this map have completed
        completed_count = sum(
            1 for ep in self._epoch_log
            if len(ep) > len(parent_epoch)
            and ep[:len(parent_epoch)] == parent_epoch
            and ep[len(parent_epoch)][1] == map_node_id
        )

        if completed_count >= expected_count:
            # All sub-epochs complete - unblock ONLY sink successors in parent epoch
            # Other successors already ran in sub-epochs
            if parent_epoch in self._epochs:
                self.logger.debug(
                    "All %d sub-epochs for map %s complete, unblocking sink successors in parent %s",
                    expected_count, map_node_id, parent_epoch
                )
                # Get nodes that were in sub-epochs (already processed)
                # Stop at Gather nodes - their successors run in parent after gather
                subepoch_nodes = self._get_subepoch_nodes(map_node_id)

                graph = self[parent_epoch]

                # Mark ALL non-map nodes that ran in sub-epochs as expired in parent
                # This prevents them from running again in the parent epoch
                # (the map node itself was already expired when it created sub-epochs)
                # Note: Gather nodes will be marked done by their GatherResult handling,
                # so we skip them here to avoid double-marking
                for node in subepoch_nodes:
                    if node != map_node_id and node not in graph.done_nodes:
                        # Skip gather nodes - they're marked done by GatherResult handling
                        if self.nodes[node].type_ != "gather":
                            graph.mark_expired(node)

                # Now unblock successors for ALL nodes that ran in sub-epochs
                # This is needed for nested maps: when b's sub-epochs complete,
                # Return depends on both b AND c. We need to unblock for both.
                for node in subepoch_nodes:
                    graph._unblock_successors(node)

                self._ready_condition.notify_all()
            else:
                self.logger.debug(
                    "All sub-epochs complete but parent epoch %s no longer active",
                    parent_epoch
                )

            # Clean up tracking
            self._subepoch_counts[parent_epoch].pop(map_node_id, None)
            if not self._subepoch_counts[parent_epoch]:
                del self._subepoch_counts[parent_epoch]

    def collapse_subepochs(self, parent_epoch: Epoch, map_node_id: str) -> None:
        """
        End all sub-epochs created by a map node, returning to the parent epoch.

        Called when a gather node collapses mapped events.
        """
        with self._ready_condition, self._epoch_condition:
            to_end = [
                ep
                for ep in list(self._epochs.keys())
                if len(ep) > len(parent_epoch)
                and ep[: len(parent_epoch)] == parent_epoch
                and ep[len(parent_epoch)][1] == map_node_id
            ]
            for ep in to_end:
                self.end_epoch(ep)

            # Clean up tracking
            if parent_epoch in self._subepoch_counts:
                self._subepoch_counts[parent_epoch].pop(map_node_id, None)
                if not self._subepoch_counts[parent_epoch]:
                    del self._subepoch_counts[parent_epoch]

    def enable_node(self, node_id: str) -> None:
        """
        Enable edges attached to the node and the
        NodeSpecification enable switches to True

        """
        self.nodes[node_id].enabled = True

    def disable_node(self, node_id: str) -> None:
        """
        Disable edges attached to the node and the
        NodeSpecification enable switches to False

        """
        self.nodes[node_id].enabled = False
        for graph in self._epochs.values():
            graph.mark_expired(node_id)

    def clear(self) -> None:
        """
        Remove epoch records, restarting the scheduler
        """
        self._epochs = {}
        self._epoch_log = deque(maxlen=100)
        self._subepoch_counts = {}

    @staticmethod
    def _init_graph(nodes: dict[str, NodeSpecification], edges: list[Edge]) -> TopoSorter:
        """
        Produce a :class:`.TopoSorter` based on the graph induced by
        a set of :class:`.Node` and a set of :class:`.Edge` that yields node ids.

        .. note:: Optional params

            Dependency graph only includes edges where `required == True` -
            aka even if we declare some dependency that passes a value to an
            optional (type annotation is `type | None`), default == `None`
            param, we still call that node even if that optional param is absent.

            Additionally, nodes with `enabled == False` are excluded, even when
            other nodes declare dependency to the disabled node. This means the
            signal of the disabled node will not be emitted and thus will not reach
            the dependent nodes. Disable nodes at your own risk.

            This behavior will likely change,
            allowing explicit parameterization of how optional values are handled,
            see: https://github.com/miniscope/noob/issues/26,

        """
        return TopoSorter(nodes, edges)

    def has_cycle(self) -> bool:
        """
        Checks that the graph is acyclic.
        """
        graph = self._init_graph(nodes=self.nodes, edges=self.edges)
        cycle = graph.find_cycle()
        return bool(cycle)

    def generations(self) -> list[tuple[str, ...]]:
        """
        Get the topological generations of the graph:
        tuples for each set of nodes that can be run at the same time.

        Order within a generation is not guaranteed to be stable.
        """
        sorter = self._init_graph(self.nodes, self.edges)
        generations = []
        while sorter.is_active():
            ready = sorter.get_ready()
            generations.append(ready)
            sorter.done(*ready)
        return generations
