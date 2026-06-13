from datetime import UTC, datetime
from multiprocessing import Queue
from queue import Empty
from uuid import uuid4

import pytest

from noob import NodeSpecification, SynchronousRunner, Tube
from noob.edge import Edge
from noob.event import Event, MetaEventType, MetaSignal
from noob.exceptions import EpochCompletedError
from noob.scheduler import Scheduler
from noob.toposort import TopoSorter
from noob.types import Epoch, NodeSignal


def test_epoch_increment():
    """
    Must be able to increment epoch on a Scheduler, which generates a graph
    each time.
    """
    tube = Tube.from_specification("testing-basic")
    scheduler = tube.scheduler

    # accessing makes the epoch
    assert isinstance(scheduler[0], TopoSorter)

    for i in range(5):
        assert scheduler.add_epoch() == i + 1
        assert isinstance(scheduler[i], TopoSorter)

    # if we create an epoch out of order, the next call without epoch specified increments
    assert scheduler.add_epoch(10) == 10
    assert scheduler.add_epoch() == 11


def test_tube_increments_epoch(basic_tubes):
    """
    Multiple runs of a tube should increment the epoch
    """
    tube = Tube.from_specification(basic_tubes)
    runner = SynchronousRunner(tube)

    for i in range(5):
        _ = runner.process()
        events = runner.store.events
        # we haven't cleared events
        assert len(events) == 1
        assert list(events.keys())[0] == i


def test_event_store_filter():
    """
    Must be able to filter events in EventStore by epoch.

    """
    tube = Tube.from_specification("testing-basic")
    runner = SynchronousRunner(tube)
    gen = runner.iter(5)
    for i, out in enumerate(gen):
        assert runner.store.get(node_id="b", signal="value", epoch=Epoch(i))["value"] == out


def test_epoch_completion(basic_tubes):
    """
    When all nodes within an epoch are complete, the Scheduler
    must emit an end-of-epoch event.

    """
    tube = Tube.from_specification(basic_tubes)
    scheduler = tube.scheduler
    scheduler.add_epoch()

    eoe = None
    while scheduler.is_active(Epoch(0)):
        ready_nodes = scheduler.get_ready()
        for ready in ready_nodes:
            # until we're done, eoe should have been None in the last iteration
            assert eoe is None
            # marking the very last one as done should emit the end of epoch event
            # (we should also break out of the while loop after the last assignment)
            eoe = scheduler.done(epoch=Epoch(0), node_id=ready["value"])

    assert (
        isinstance(eoe["id"], int)
        and isinstance(eoe["timestamp"], datetime)
        and eoe["node_id"] == "meta"
        and eoe["signal"] == MetaEventType("EpochEnded")
        and eoe["epoch"] == 0
    )


def test_tube_disable_node():
    """
    When a node gets disabled in a Tube, it takes effect
    for all the SUBSEQUENT epochs.

    """
    tube = Tube.from_specification("testing-basic")
    runner = SynchronousRunner(tube)
    assert runner.process() == 0
    runner.tube.disable_node("c")
    assert runner.process() is None


def test_scheduler_disable_node():
    """
    When a node gets disabled in a Scheduler, it takes effect
    for all the SUBSEQUENT epochs.

    """
    tube = Tube.from_specification("testing-basic")
    scheduler = tube.scheduler
    scheduler.add_epoch()
    scheduler.disable_node("c")
    scheduler.add_epoch()
    assert list(scheduler.nodes.keys()) == ["a", "b", "c"]
    assert list(scheduler[0]._node2info.keys()) == [("a", "index"), "a", "b", ("b", "value"), "c"]
    assert list(scheduler[1]._node2info.keys()) == [("a", "index"), "a", "b"]


@pytest.mark.parametrize(
    "spec, input, expected",
    [
        ("testing-basic", None, ["a"]),
        ("testing-merge", None, ["a", "b"]),
        ("testing-input-mixed", {"start": 7, "multiply_tube": 13}, ["a"]),
    ],
)
def test_identify_source_node(spec, input, expected):
    """
    Scheduler can correctly identify the source nodes of a tube.

    """
    tube = Tube.from_specification(spec, input)
    assert set(tube.scheduler.source_nodes) == set(expected)


@pytest.mark.parametrize(
    "spec, input, sources",
    [
        ("testing-basic", None, ["a"]),
        ("testing-merge", None, ["a", "b"]),
        ("testing-input-mixed", {"start": 7, "multiply_tube": 13}, ["a"]),
    ],
)
def test_source_node_completion(spec, input, sources):
    """
    Scheduler checks whether all source nodes of a given epoch is complete.

    """
    tube = Tube.from_specification(spec, input)
    scheduler = tube.scheduler
    # Epoch 0 won't be finished
    scheduler.add_epoch()
    # We will finish the latest epoch
    epoch_1 = scheduler.add_epoch()
    assert not scheduler.sources_finished()
    for node_id in sources:
        scheduler.get_ready()
        # Default checks the latest epoch
        assert not scheduler.sources_finished()
        scheduler.done(epoch=epoch_1, node_id=node_id)
    assert scheduler.sources_finished()


def test_clear_ended_epochs():
    """
    Scheduler must clear the cache of any epochs
    whose nodes have all completed.

    """
    tube = Tube.from_specification("testing-basic")
    scheduler = tube.scheduler
    # We will finish epoch 0
    scheduler.add_epoch()
    # Epoch 1 will not be finished
    scheduler.add_epoch()
    for node in scheduler.nodes:
        scheduler.get_ready()
        scheduler.done(epoch=Epoch(0), node_id=node)
        if node != "c":
            assert len(scheduler._epochs) == 2
        else:
            assert scheduler[1]
            with pytest.raises(EpochCompletedError):
                _ = scheduler[0]
            assert len(scheduler._epochs) == 1


@pytest.mark.xfail(raises=Empty)
def test_metaevents():
    """
    Meta events are emitted in callbacks, but not given to nodes or added to the store.

    marked `xfail` until Return node starts returning NoEvent instead of None.

    """
    queue = Queue()

    def callback(event: Event) -> bool:
        nonlocal queue

        if event["node_id"] == "meta":
            queue.put_nowait(event)

    tube = Tube.from_specification("testing-basic")
    runner = SynchronousRunner(tube)
    runner.add_callback(callback)
    runner.run(5)
    event = queue.get_nowait()
    assert event["node_id"] == "meta"
    assert len(runner.store.events) > 0
    assert all(event["node_id"] != "meta" for event in runner.store.flat_events)


@pytest.mark.xfail(raises=NotImplementedError)
def test_noevent_ends_epoch():
    """If a node in the middle of a tube emits a noevent, the epoch should no longer be active"""
    raise NotImplementedError("Write this test!")


def test_disable_nodes():
    """
    For A->B->C graph, when B is disabled,
    the scheduler only yields A and then becomes inactive,
    rather than yielding C.
    """
    tube = Tube.from_specification("testing-basic")
    scheduler = tube.scheduler
    scheduler.disable_node("b")
    epoch = scheduler.add_epoch()
    ready_nodes = scheduler.get_ready()

    # only "a" is returned, even though "b" also has no dependencies,
    # since "b" is disabled.
    assert {node["value"] for node in ready_nodes} == {"a"}
    scheduler.done(epoch=epoch, node_id="a")

    # nothing ready anymore
    assert not scheduler.get_ready()


@pytest.mark.map
def test_map_creating_subepochs_expires_parent_epoch():
    """
    When a node induces a map by creating subepochs,
    that node should be marked exhausted in the parent epoch
    """
    tube = Tube.from_specification("testing-map-basic")
    scheduler = tube.scheduler
    ep = scheduler.add_epoch()
    scheduler.done(ep, node_id="a")
    scheduler.update([_make_event(ep / ("b", i), "b") for i in range(3)])
    assert "b" in scheduler._epochs[ep].done_nodes
    assert "b" not in scheduler._epochs[ep].ran_nodes


@pytest.mark.map
def test_get_ready_yields_all_mapped_subepochs():
    """
    get_ready should yield all mapped subepochs at once when accessed with the parent epoch
    """
    tube = Tube.from_specification("testing-map-basic")
    scheduler = tube.scheduler
    ep = scheduler.add_epoch()
    scheduler.done(ep, node_id="a")
    scheduler.update([_make_event(ep / ("b", i), "b") for i in range(3)])
    ready = scheduler.get_ready(ep)
    assert len(ready) == 3
    assert {r["epoch"] for r in ready} == {ep / ("b", i) for i in range(3)}


@pytest.mark.map
def test_map_gather_only_parent_epoch():
    """
    When a gather node collapses subepochs, nodes that are exclusively downstream of the gather node
    only run in the parent epoch
    """
    tube = Tube.from_specification("testing-map-gather")
    scheduler = tube.scheduler
    ep = scheduler.add_epoch()
    scheduler.done(ep, node_id="a")
    scheduler.update([_make_event(ep / ("b", i), "b") for i in range(3)])
    for i in range(3):
        scheduler.done(ep / ("b", i), node_id="c")
    scheduler.done(ep, node_id="d")
    ready = scheduler.get_ready(ep)
    assert len(ready) == 1
    assert ready[0]["epoch"] == ep


@pytest.mark.map
def test_map_gather_mixed_epochs():
    """
    When a gather node collapses subepochs,
    it yields nodes that are exclusively downstream of the gather node in the parent epoch,
    and nodes that are in the mapped subepoch in the subepochs.
    """
    tube = Tube.from_specification("testing-map-gather")
    scheduler = tube.scheduler
    ep = scheduler.add_epoch()
    scheduler.done(ep, node_id="a")
    scheduler.update([_make_event(ep / ("b", i), "b") for i in range(3)])
    for i in range(3):
        scheduler.done(ep / ("b", i), node_id="c", signal="value")
    for i in range(3):
        scheduler.expire(ep / ("b", i), node_id="d", signal="value", unlock_optionals=False)
    scheduler.done(ep, node_id="d", signal="value")
    scheduler.done(ep, node_id="e")
    ready = scheduler.get_ready(ep)
    assert len(ready) == 3
    assert {r["epoch"] for r in ready} == {ep / ("b", i) for i in range(3)}


@pytest.mark.map
def test_is_active_when_subepochs_active():
    """Scheduler stays active for an epoch as long as there are active subepochs"""
    tube = Tube.from_specification("testing-map-basic")
    scheduler = tube.scheduler
    ep = scheduler.add_epoch()
    scheduler.done(ep, node_id="a")
    scheduler.update([_make_event(ep / ("b", i), "b") for i in range(3)])
    assert not scheduler._epochs[ep].is_active()
    assert scheduler._epochs[ep / ("b", 0)].is_active()
    assert scheduler.is_active(ep)
    scheduler.update([_make_event(ep / ("b", i), "c") for i in range(3)])
    assert not scheduler._epochs[ep].is_active()
    assert scheduler._epochs[ep / ("b", 0)].is_active()
    assert scheduler.is_active(ep)
    for i in range(3):
        scheduler.expire(ep / ("b", i), node_id="return")
        assert not scheduler._epochs[ep / ("b", i)].is_active()
    assert not scheduler.is_active(ep)


def _make_event(epoch: Epoch, node_id: str, signal: str = "value") -> Event:
    return Event(
        id=0, epoch=epoch, node_id=node_id, timestamp=datetime.now(), signal=signal, value=0
    )


def test_asset_generations():
    """Topological generations with respect to an asset"""
    tube = Tube.from_specification("testing-asset-generations")
    scheduler = tube.scheduler
    generations = scheduler.asset_generations()
    for asset in ("count_process", "count_runner_depends", "count_runner"):
        if asset == "count_runner_depends":
            assert len(generations[asset]) == 4
            assert set(generations[asset][3]) == {"d1"}
        else:
            assert len(generations[asset]) == 3
        assert set(generations[asset][0]) == {"a1", "a2"}
        assert set(generations[asset][1]) == {"b1", "b2"}
        assert set(generations[asset][2]) == {"c1", "c2"}


def test_upstream_nodes(optional_graph):
    """
    Upstream nodes detects immediate dependencies as well as nodes who can affect our running
    by indirect optional dependencies
    (i.e., if they emitted NoEvent, we would be eligible for running)
    """
    sched = Scheduler(edges=optional_graph, nodes={})
    # d should implicitly have b upstream but not a, because a->b is optional,
    # despite only directly depending on c
    # the rest are just direct dependencies and that's the `Node.edges` method, so not tested here.
    assert not any(e.source_node == "b" and e.target_node == "d" for e in optional_graph)
    assert sched.upstream_nodes("d") == {"b", "c"}


def test_subepoch_generation_race_condition():
    """
    Nodes in a parent epoch are not incorrectly run in subepochs
    when a race condition exists between the `map` node and the parent epoch nodes.

    References:
        https://github.com/miniscope/noob/issues/193

    """
    tube = Tube.from_specification("testing-map-depends")
    scheduler = tube.scheduler
    ep = scheduler.add_epoch()
    ready = scheduler.get_ready(ep)
    assert set([r["value"] for r in ready]) == {"word", "count"}

    # word completes first and is then mapped
    scheduler.done(ep, node_id="word")
    ready = scheduler.get_ready()
    assert set([r["value"] for r in ready]) == {"map"}

    # simulate the map events creating subepochs
    subepochs = ep.make_subepochs("map", 3)
    events = [
        Event(
            id=i, timestamp=datetime.now(UTC), node_id="map", signal="value", epoch=subep, value=i
        )
        for i, subep in enumerate(subepochs)
    ] + [Event(id=3, timestamp=datetime.now(UTC), node_id="map", signal="n", epoch=ep, value=3)]
    scheduler.update(events)

    # next node in subepoch "exclaim" depends on count, which is not done yet
    # we should not have accidentally yielded count in the subepochs here
    # because it wasn't done in the parent.
    ready = scheduler.get_ready(ep)
    assert len(ready) == 0

    # then when count is done, all the exclaim nodes in the subepoch should be ready.
    scheduler.update(
        [
            Event(
                id=4,
                timestamp=datetime.now(UTC),
                node_id="count",
                signal="index",
                epoch=ep,
                value=4,
            )
        ]
    )
    ready = scheduler.get_ready(ep)
    assert len(ready) == 3
    assert all(len(r["epoch"]) > 1 for r in ready)
    assert set(r["value"] for r in ready) == {"exclaim"}


def test_non_equivalent_event(non_equivalent_event):
    """
    Regression - we can handle events that have a value that can't use ==

    References:
        https://github.com/miniscope/noob/issues/192
        https://github.com/miniscope/noob/issues/201
    """
    scheduler = Scheduler(
        edges=[
            Edge(
                source_node="default",
                source_signal="value",
                target_node="target",
                target_slot="value",
            )
        ],
        nodes={
            "default": NodeSpecification(id="default", type="noob.testing.notreal"),
            "target": NodeSpecification(id="target", type="noob.testing.notreal"),
        },
    )
    scheduler.update([non_equivalent_event])


# --------------------------------------------------
# statefulness, cancellation, run control & iteration
# --------------------------------------------------


def _event(node_id: str, signal: str, epoch: int | Epoch, value=1) -> Event:
    return Event(
        id=uuid4().int,
        timestamp=datetime.now(UTC),
        node_id=node_id,
        signal=signal,
        epoch=Epoch(epoch) if isinstance(epoch, int) else epoch,
        value=value,
    )


def _node_scheduler(stateful: bool) -> Scheduler:
    """
    A scheduler as the zmq NodeRunner holds one: a single node's spec,
    plus the edges of the graph around it
    """
    return Scheduler(
        nodes={
            "b": NodeSpecification(
                id="b",
                type="noob.testing.multiply",
                stateful=stateful,
                depends=[{"left": "a.value"}],
            )
        },
        edges=[
            Edge(source_node="a", source_signal="value", target_node="b", target_slot="left")
        ],
    )


def test_scheduler_reads_edge_and_node_fields():
    """
    Drift canary for the fields the rust core extracts across the barrier
    (``EdgeRec``: source_node, source_signal, required;
    ``NodeFlags``: enabled, stateful) and the python scheduler's equivalent
    reads. Renaming a python field/key breaks one of these assertions before
    it can silently change scheduling behavior.
    """
    scheduler = Scheduler(
        nodes={
            "a": NodeSpecification(id="a", type="noob.testing.notreal"),
            "b": NodeSpecification(id="b", type="noob.testing.notreal"),
            "c": NodeSpecification(
                id="c",
                type="noob.testing.notreal",
                depends=[{"left": "a.value"}, {"right": "b.value"}],
            ),
            "off": NodeSpecification(id="off", type="noob.testing.notreal", enabled=False),
            "s": NodeSpecification(
                id="s",
                type="noob.testing.notreal",
                stateful=True,
                depends=[{"left": "a.value"}],
            ),
        },
        edges=[
            Edge(source_node="a", source_signal="value", target_node="c", target_slot="left"),
            Edge(
                source_node="b",
                source_signal="value",
                target_node="c",
                target_slot="right",
                required=False,
            ),
            Edge(source_node="a", source_signal="value", target_node="s", target_slot="left"),
        ],
    )

    # EdgeRec.source_node / source_signal -> graph_signals
    assert ("a", "value") in scheduler.graph_signals
    assert ("b", "value") in scheduler.graph_signals

    epoch = scheduler.add_epoch()

    # EdgeRec.required -> the required edge is a hard predecessor, the optional one isn't
    optional = scheduler[epoch].node_info["c"].optional_predecessors
    assert NodeSignal("b", "value") in optional
    assert NodeSignal("a", "value") not in optional

    # NodeFlags.enabled=False -> disabled node never becomes ready
    ready_values = {r["value"] for r in scheduler.get_ready(epoch)}
    assert "a" in ready_values
    assert "off" not in ready_values

    # NodeFlags.stateful=True -> withheld in a later epoch until earlier ones complete
    scheduler.add_epoch(1)
    scheduler.update([_event("a", "value", 1)])
    assert scheduler.get_ready(Epoch(1), node_id="s") == []


def test_iter_epoch():
    """
    iter_epoch yields ready nodes for a single epoch in dependency order,
    and ends when the epoch completes
    """
    tube = Tube.from_specification("testing-basic")
    scheduler = tube.scheduler
    epoch = scheduler.add_epoch()

    ran = []
    for item in scheduler.iter_epoch():
        assert item is not None, "nothing should stall when nodes complete synchronously"
        ran.append(item["value"])
        scheduler.done(epoch=item["epoch"], node_id=item["value"])

    assert not scheduler.is_active(epoch)
    assert scheduler.epoch_completed(epoch)
    assert ran.index("a") < ran.index("b")


def test_get_ready_withholds_stateful():
    """
    Stateful nodes are only ready in an epoch once they have completed
    all earlier epochs; stateless nodes run in any epoch as soon as
    their inputs are satisfied.
    """
    stateful = _node_scheduler(True)
    stateless = _node_scheduler(False)
    for scheduler in (stateful, stateless):
        scheduler.add_epoch(0)
        scheduler.add_epoch(1)
        # the node's input arrives in epoch 1 before epoch 0
        scheduler.update([_event("a", "value", 1)])

    # stateless: run wherever ready
    ready = stateless.get_ready(node_id="b")
    assert [r["epoch"] for r in ready] == [Epoch(1)]

    # stateful: withheld until epoch 0 completes
    assert stateful.get_ready(node_id="b") == []
    stateful.update([_event("a", "value", 0)])
    ready = stateful.get_ready(node_id="b")
    assert [r["epoch"] for r in ready] == [Epoch(0)]
    stateful.done(Epoch(0), "b")
    ready = stateful.get_ready(node_id="b")
    assert [r["epoch"] for r in ready] == [Epoch(1)]


def test_update_emits_node_canceled():
    """
    A NoEvent expires everything that requires it for the rest of the epoch:
    update returns NodeCanceled MetaEvents for the canceled nodes
    and the epoch can complete.
    """
    scheduler = _node_scheduler(False)
    epoch = scheduler.add_epoch()

    events = scheduler.update([_event("a", "value", 0, value=MetaSignal.NoEvent)])

    canceled = [
        e
        for e in events
        if e["node_id"] == "meta" and e["signal"] == MetaEventType.NodeCanceled
    ]
    assert [(c["value"], c["epoch"]) for c in canceled] == [("b", Epoch(0))]
    assert scheduler.node_is_done("b", epoch)
    assert not scheduler.is_active(epoch)


@pytest.mark.parametrize("stateful", [True, False])
def test_iter_events_grants(stateful):
    """
    iter_events runs granted epochs: stateful nodes wait for strict epoch
    order even when grants arrive out of order, while stateless nodes run
    granted epochs as soon as their inputs arrive.
    """
    scheduler = _node_scheduler(stateful)
    items = scheduler.iter_events("b")

    # no permission, nothing to do
    assert next(items) is None

    # epoch 1 granted and its inputs available, but 0 has not happened yet
    scheduler.queue_epoch(Epoch(1))
    scheduler.update([_event("a", "value", 1)])
    item = next(items)

    if stateful:
        assert item is None
        scheduler.queue_epoch(Epoch(0))
        scheduler.update([_event("a", "value", 0)])
        item = next(items)
        assert item["signal"] == MetaEventType.NodeReady
        assert item["epoch"] == Epoch(0)
        scheduler.done(Epoch(0), "b")
        # the epoch ends (we only observe our own node), then epoch 1 runs
        item = next(items)
        assert item["signal"] == MetaEventType.EpochEnded
        assert item["epoch"] == Epoch(0)
        item = next(items)

    assert item["signal"] == MetaEventType.NodeReady
    assert item["epoch"] == Epoch(1)


def test_iter_events_freerun():
    """
    In freerun, iter_events creates new epochs as the node completes old ones
    """
    scheduler = Scheduler(
        nodes={"a": NodeSpecification(id="a", type="noob.testing.count_source", stateful=True)},
        edges=[Edge(source_node="a", source_signal="index", target_node="b", target_slot="left")],
    )
    items = scheduler.iter_events("a")
    assert next(items) is None

    scheduler.set_freerun(True)
    for i in range(3):
        item = next(items)
        assert item["signal"] == MetaEventType.NodeReady
        assert item["epoch"] == Epoch(i)
        scheduler.done(Epoch(i), "a")
        ended = next(items)
        assert ended["signal"] == MetaEventType.EpochEnded
        assert ended["epoch"] == Epoch(i)

    scheduler.set_freerun(False)
    assert next(items) is None
