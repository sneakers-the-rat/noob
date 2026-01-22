"""
Tube runners for running tubes
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import count
from threading import Condition
from typing import Any, TypeAlias

from noob.const import META_SIGNAL
from noob.event import Event, MetaSignal
from noob.node import Edge
from noob.node.base import Signal
from noob.types import Epoch, NodeID, SignalName, epoch_is_descendant, epoch_parent

EventDict: TypeAlias = dict[Epoch, dict[NodeID, dict[SignalName, list[Event]]]]
"""
A nested dictionary to store events for rapid access
(vs. the old implementation which was just a big list to filter).

Stores by epoch, node_id, and signal, like
events = {epoch_tuple: {'node_id': {'signal': [...]}}}

Should be made with a `defaultdict` to avoid annoying nested indexing problems
"""


def _make_event_dict() -> EventDict:
    return defaultdict(lambda: defaultdict(lambda: defaultdict(list)))


@dataclass
class EventStore:
    """
    Container class for storing and retrieving events by node and slot
    """

    events: EventDict = field(default_factory=_make_event_dict)
    counter: count = field(default_factory=count)

    _event_condition: Condition = field(default_factory=Condition)

    @property
    def flat_events(self) -> list[Event]:
        """Flattened list of events in the store"""
        events = []
        for epoch_evts in self.events.values():
            for node_evts in epoch_evts.values():
                for signal_evts in node_evts.values():
                    events.extend(signal_evts)
        return events

    def add(self, event: Event) -> Event:
        """
        Add an existing event to the store, returning it.

        Mostly an abstraction layer to give ourselves room above the `events` list
        in cast we want to change the internal implementation of how events are stored
        """
        with self._event_condition:
            self.events[event["epoch"]][event["node_id"]][event["signal"]].append(event)
            self._event_condition.notify_all()
        return event

    def add_value(self, signals: list[Signal], value: Any, node_id: str, epoch: Epoch) -> list[Event]:
        """
        Add the result of a :meth:`.Node.process` call to the event store.

        Split the dictionary of values into separate :class:`.Event` s,
        store along with current timestamp

        Args:
            signals (list[Signal]): Signals from which the value was emitted by
                a :meth:`.Node.process` call
            value (Any): Value emitted by a :meth:`.Node.process` call. Gets wrapped
                with a list in case the length of signals is 1. Otherwise, it's zipped
                with :signals:
            node_id (str): ID of the node that emitted the events
            epoch (Epoch): Hierarchical epoch tuple that the signal was emitted in
        """
        timestamp = datetime.now(UTC)
        if value is MetaSignal.NoEvent or (isinstance(value, str) and value == MetaSignal.NoEvent):
            signals = [Signal(name=META_SIGNAL, type_=MetaSignal)]

        with self._event_condition:
            values = [value] if len(signals) == 1 else value

            new_events = []
            for signal, val in zip(signals, values):
                new_event = Event(
                    id=next(self.counter),
                    timestamp=timestamp,
                    node_id=node_id,
                    epoch=epoch,
                    signal=signal.name,
                    value=val,
                )
                self.add(new_event)
                new_events.append(new_event)

            self._event_condition.notify_all()
        return new_events

    def get(self, node_id: str, signal: str, epoch: Epoch) -> Event:
        """
        Get the event with the matching node_id and signal name from a given epoch.

        Raises:
            KeyError: if no event with the matching node_id and signal name exists
        """
        events = self.events[epoch][node_id][signal]
        event = events[-1] if events else None

        if event is None:
            raise KeyError(
                f"No event found for node_id: {node_id}, signal: {signal}, epoch: {epoch}"
            )
        return event

    def get_at_or_above(self, node_id: str, signal: str, epoch: Epoch) -> Event:
        """
        Get an event at the given epoch, or search up the epoch hierarchy.

        This is used when a node depends on a signal that was emitted at a higher
        (parent) epoch level - e.g., a mapped node depending on a non-mapped signal.

        Raises:
            KeyError: if no event found at this epoch or any ancestor epoch
        """
        current: Epoch | None = epoch
        while current is not None:
            try:
                return self.get(node_id, signal, current)
            except KeyError:
                current = epoch_parent(current)

        raise KeyError(
            f"No event found for node_id: {node_id}, signal: {signal}, "
            f"at epoch {epoch} or any ancestor"
        )

    def collect_across_subepochs(
        self, node_id: str, signal: str, parent_epoch: Epoch
    ) -> list[Event]:
        """
        Collect all events from sub-epochs that are descendants of the given parent epoch.

        For nested maps, this searches all depth levels to find events.

        Returns:
            List of events sorted by their full epoch path
        """
        events = []
        for epoch, epoch_events in self.events.items():
            # Check if this epoch is ANY descendant of parent_epoch (not just direct child)
            if len(epoch) > len(parent_epoch) and epoch_is_descendant(epoch, parent_epoch):
                if node_id in epoch_events and signal in epoch_events[node_id]:
                    events.extend(epoch_events[node_id][signal])

        # Sort by the full epoch path to maintain proper ordering
        events.sort(key=lambda e: e["epoch"])
        return events

    def get_subepoch_count(self, parent_epoch: Epoch, node_id: str) -> int:
        """
        Count the number of sub-epochs created by a specific node under a parent epoch.

        Used to determine when all mapped events have been received.
        """
        count = 0
        for epoch in self.events.keys():
            if (
                len(epoch) == len(parent_epoch) + 1
                and epoch_is_descendant(epoch, parent_epoch)
                and epoch[-1][1] == node_id
            ):
                count += 1
        return count

    def collect(
        self, edges: list[Edge], epoch: Epoch, hierarchical: bool = True
    ) -> dict | None:
        """
        Gather events into a form that can be consumed by a :meth:`.Node.process` method,
        given the collection of inbound edges (usually from :meth:`.Tube.in_edges` ).

        If none of the requested events have been emitted, return ``None``.

        If all of the requested events have been emitted, return a kwarg-like dict

        If some of the requested events are missing but others are present,
        exclude the keys from the returned dict
        (`None` is a valid value for an event, so if a key is present and the value is `None`,
        the event was emitted with a value of `None`)

        Args:
            edges: List of edges from which to collect events
            epoch: The epoch to collect events from
            hierarchical: If True, search up the epoch hierarchy for missing events.
                This allows nodes in sub-epochs to access events from parent epochs.

        .. todo::

            Add an example

        """
        events = self.collect_events(edges, epoch, hierarchical)
        if events is None:
            return None

        return self.transform_events(edges=edges, events=events)

    def collect_events(
        self, edges: list[Edge], epoch: Epoch, hierarchical: bool = True
    ) -> list[Event] | None:
        """
        Collect the event objects from a set of dependencies indicated by edges in a given epoch.

        If none of the requested events are present, return None

        If some of the requested events but others are present,
        return an incomplete list.

        Args:
            edges (list[Edge]): List of edges from which to collect events
            epoch (Epoch): Epoch to select from
            hierarchical: If True, use hierarchical lookup (search ancestors and descendants)
        """
        events = []
        for edge in edges:
            try:
                if hierarchical:
                    event = self.get_at_or_above(edge.source_node, edge.source_signal, epoch)
                else:
                    event = self.get(edge.source_node, edge.source_signal, epoch)
                events.append(event)
            except KeyError:
                if hierarchical:
                    # Try collecting from sub-epochs (for mapped signals)
                    sub_events = self.collect_across_subepochs(
                        edge.source_node, edge.source_signal, epoch
                    )
                    if sub_events:
                        # Wrap multiple events as a list value in a synthetic event
                        values = [e["value"] for e in sub_events]
                        synthetic_event = Event(
                            id=sub_events[0]["id"],
                            timestamp=sub_events[0]["timestamp"],
                            node_id=edge.source_node,
                            epoch=epoch,
                            signal=edge.source_signal,
                            value=values,
                        )
                        events.append(synthetic_event)
                        continue
                # no matching event
                continue

        return events if events else None

    def clear(self, epoch: Epoch | None = None) -> None:
        """
        Clear events for a specific or all epochs.

        Does not reset the counter (to continue giving unique ids to the next round's events)
        """

        if epoch is None:
            self.events = _make_event_dict()
        else:
            del self.events[epoch]

    def clear_subepochs(self, parent_epoch: Epoch) -> None:
        """
        Clear all events from sub-epochs of the given parent epoch.

        Used after a gather operation collapses sub-epochs.
        """
        to_delete = [
            ep
            for ep in self.events.keys()
            if len(ep) > len(parent_epoch) and epoch_is_descendant(ep, parent_epoch)
        ]
        for ep in to_delete:
            del self.events[ep]

    @staticmethod
    def transform_events(edges: list[Edge], events: list[Event]) -> dict:
        """
        Transform the values of a set of events to a dict that can be consumed by the
        target node's process method.

        i.e.: return a dictionary whose keys are the ``target_signal`` s of the edges
           using the ``value`` of the matching event.
        """
        args = {}
        for edge in edges:
            evts = [
                e
                for e in events
                if e["node_id"] == edge.source_node and e["signal"] == edge.source_signal
            ]
            if not evts:
                continue
            args[edge.target_slot] = evts[-1]["value"]
        return args

    @staticmethod
    def split_args_kwargs(inputs: dict) -> tuple[tuple, dict]:
        # TODO: integrate this with `collect`
        args = []
        kwargs = {}
        for k, v in inputs.items():
            if isinstance(k, int):
                args.append((k, v))
            elif k is None:
                args.append((0, v))
            else:
                kwargs[k] = v

        # cast to tuple since `*args` is a tuple
        args_tuple = tuple(item[1] for item in sorted(args, key=lambda x: x[0]))
        return args_tuple, kwargs
