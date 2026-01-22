import pytest
from pydantic import ValidationError
from typing import Any

from noob import SynchronousRunner, Tube
from noob.runner import TubeRunner
from noob.types import epoch_depth
from noob.utils import iscoroutinefunction_partial

pytestmark = pytest.mark.map


async def _call_process(runner: TubeRunner) -> Any:
    if iscoroutinefunction_partial(runner.process):
        return await runner.process()
    else:
        return runner.process()


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded_tube", ["testing-map-basic"], indirect=True)
async def test_map_basic(loaded_tube: Tube, all_runners: TubeRunner):
    """
    Map splits up an iterator and processes its elements individually
    """
    runner = all_runners
    for _ in range(5):
        val = await _call_process(runner)
        assert isinstance(val["letter"], list)
        assert val["letter"] == [letter + "!" for letter in val["word"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded_tube", ["testing-map-depends"], indirect=True)
async def test_map_depends(loaded_tube: Tube, all_runners: TubeRunner):
    """
    A node that depends on a normal event and a mapped one has the normal event repeated
    """
    runner = all_runners
    for _ in range(5):
        val = await _call_process(runner)
        assert isinstance(val["letters"], list)
        assert val["letters"] == [letter + ("!" * val["count"]) for letter in val["word"]]


def test_map_double_depends():
    """
    A node cannot be downstream of multiple, unrelated maps
    """
    with pytest.raises(ValidationError):
        Tube.from_specification("testing-map-depends-double")


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded_tube", ["testing-map-gather"], indirect=True)
async def test_map_gather(loaded_tube: Tube, all_runners: TubeRunner):
    """
    Gathering after a map collapses the sub-epoch
    """
    runner = all_runners
    for _ in range(5):
        val = await _call_process(runner)
        assert val["letter"] == [letter + "!" for letter in val["word"]]
        assert val["reconstructed"] == "".join(val["letter"])

        # Verify epoch structure:
        # After gather, the epoch should be collapsed back to root level (depth 1)
        # The gathered events should have been at depth 2 (sub-epochs from map)


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded_tube", ["testing-map-nested"], indirect=True)
async def test_map_nested(loaded_tube: Tube, all_runners: TubeRunner):
    """
    Maps can map mappings.

    Without intermediate Gather nodes, nested map outputs are collected as
    a flat list of all events from all descendant sub-epochs.
    """
    runner = all_runners
    for _ in range(5):
        val = await _call_process(runner)
        # mapping a list and then returning it reconstructs the list
        assert val["word"] == val["words"]
        # letter is a flat list of all characters from all words
        # (no intermediate gather, so no grouping)
        expected_letters = list("".join(val["words"]))
        assert val["letter"] == expected_letters


# Edge case tests


def test_map_store_events():
    """
    Verify that map node stores individual events in sub-epochs,
    and that downstream nodes collect those individual values.
    """
    from noob.types import make_root_epoch, make_sub_epoch

    tube = Tube.from_specification("testing-map-basic")
    runner = SynchronousRunner(tube=tube)
    runner.init()

    # Clear store and create a fresh epoch
    runner.store.clear()
    parent_epoch = make_root_epoch(0)
    runner.tube.scheduler.add_epoch(parent_epoch)

    # Simulate word_source emitting "test"
    word_events = runner.store.add_value(
        tube.nodes["a"].signals, "test", "a", parent_epoch
    )

    # Now simulate map processing
    map_node = tube.nodes["b"]
    map_node.set_epoch_context(parent_epoch)
    result = map_node.process("test")

    # MapResult should have 4 events, one for each character
    assert len(result.events) == 4
    assert [e["value"] for e in result.events] == ["t", "e", "s", "t"]

    # Each event should be in a different sub-epoch
    for i, event in enumerate(result.events):
        expected_epoch = make_sub_epoch(parent_epoch, i, "b")
        assert event["epoch"] == expected_epoch, f"Event {i} has wrong epoch"

    # Store the events
    for event in result.events:
        runner.store.add(event)

    # Verify we can retrieve individual values in each sub-epoch
    for i, expected_char in enumerate(["t", "e", "s", "t"]):
        sub_epoch = make_sub_epoch(parent_epoch, i, "b")
        event = runner.store.get("b", "value", sub_epoch)
        assert event["value"] == expected_char, (
            f"Sub-epoch {i} should have value '{expected_char}', got '{event['value']}'"
        )


def test_map_input_collection():
    """
    Verify that nodes downstream of map collect individual values
    when running in sub-epochs.
    """
    from noob.types import make_root_epoch, make_sub_epoch

    tube = Tube.from_specification("testing-map-basic")
    runner = SynchronousRunner(tube=tube)
    runner.init()

    # Clear store and create epochs
    runner.store.clear()
    parent_epoch = make_root_epoch(0)
    runner.tube.scheduler.add_epoch(parent_epoch)

    # Store word in parent epoch
    runner.store.add_value(tube.nodes["a"].signals, "test", "a", parent_epoch)

    # Create and store map events in sub-epochs
    map_node = tube.nodes["b"]
    map_node.set_epoch_context(parent_epoch)
    result = map_node.process("test")
    for event in result.events:
        runner.store.add(event)

    # Now test input collection for exclaim (node c) in each sub-epoch
    exclaim_node = tube.nodes["c"]
    exclaim_edges = tube.in_edges(exclaim_node)

    for i, expected_char in enumerate(["t", "e", "s", "t"]):
        sub_epoch = make_sub_epoch(parent_epoch, i, "b")

        # Collect input for exclaim in this sub-epoch
        collected = runner.store.collect(exclaim_edges, sub_epoch, hierarchical=True)

        assert collected is not None, f"No input collected for sub-epoch {i}"
        assert "string" in collected, f"'string' key missing in collected input"
        assert collected["string"] == expected_char, (
            f"Sub-epoch {i}: exclaim should get '{expected_char}', got '{collected['string']}'"
        )


def test_subepoch_completion_timing():
    """
    Verify that sub-epochs complete in the expected order
    and completion check fires at the right time.
    """
    tube = Tube.from_specification("testing-map-basic")
    runner = SynchronousRunner(tube=tube)

    # Track events
    events_order = []

    def track(event):
        events_order.append({
            "node": event.get("node_id"),
            "epoch": event.get("epoch"),
            "signal": event.get("signal"),
            "value": event.get("value"),
        })

    runner.add_callback(track)
    runner.init()

    # First run
    result = runner.process()
    print(f"Result: {result}")
    print(f"Events order:")
    for e in events_order:
        print(f"  {e}")

    # Verify we got results
    assert result is not None, "Should get a result"
    assert "letter" in result, "Result should have 'letter'"
    assert isinstance(result["letter"], list), f"letter should be a list, got {type(result['letter'])}"


def test_nested_map_store_collection():
    """
    Test that events from nested maps are stored and can be collected by Return.
    """
    from noob.types import make_root_epoch

    tube = Tube.from_specification("testing-map-nested")
    runner = SynchronousRunner(tube=tube)

    # Track what's stored
    stored_events = []
    original_add = runner.store.add
    def track_add(event):
        stored_events.append(event)
        return original_add(event)
    runner.store.add = track_add

    runner.init()
    result = runner.process()

    # Check what was stored
    print(f"Total events stored: {len(stored_events)}")
    for e in stored_events[:10]:
        print(f"  {e['node_id']}.{e['signal']} = {e['value']} in {e['epoch']}")

    # Check the return node's edges
    return_node = tube.nodes["return"]
    return_edges = tube.in_edges(return_node)
    print(f"Return edges: {[(e.source_node, e.source_signal) for e in return_edges]}")

    # Try to collect for return in parent epoch
    parent_epoch = ((0, 'root'),)
    collected = runner.store.collect(return_edges, parent_epoch, hierarchical=True)
    print(f"Collected for return: {collected}")


def test_nested_map_completion_flow():
    """
    Test that nested map sub-epochs complete correctly and trigger parent completion.

    Note: Sub-epochs with no active nodes end immediately in add_sub_epochs.
    """
    from noob.types import make_root_epoch, make_sub_epoch

    tube = Tube.from_specification("testing-map-nested")
    runner = SynchronousRunner(tube=tube)
    runner.init()

    parent_epoch = make_root_epoch(0)
    runner.tube.scheduler.add_epoch(parent_epoch)

    # Create b's sub-epochs
    b_sub_epochs = runner.tube.scheduler.add_sub_epochs(parent_epoch, 2, "b")

    # Process first b sub-epoch: c creates sub-sub-epochs
    first_b_sub = b_sub_epochs[0]
    graph = runner.tube.scheduler._epochs[first_b_sub]
    graph.get_ready()  # marks c as out

    # Create c's sub-sub-epochs FIRST, then expire
    c_sub_epochs_1 = runner.tube.scheduler.add_sub_epochs(first_b_sub, 2, "c")

    # The c sub-sub-epochs should have ended immediately (inactive)
    for c_sub in c_sub_epochs_1:
        assert c_sub not in runner.tube.scheduler._epochs, "c sub-epoch should have ended"
        assert c_sub in runner.tube.scheduler._epoch_log, "c sub-epoch should be in log"

    # Now expire c in b sub-epoch
    runner.tube.scheduler.expire(first_b_sub, "c")

    # After all c sub-epochs complete, first_b_sub should have ended
    assert first_b_sub in runner.tube.scheduler._epoch_log, \
        f"First b sub-epoch should be in epoch log"

    # Check that parent epoch is still active (waiting for second b sub-epoch)
    assert runner.tube.scheduler.is_active(parent_epoch), "Parent epoch should still be active"

    # Process second b sub-epoch similarly
    second_b_sub = b_sub_epochs[1]
    graph = runner.tube.scheduler._epochs[second_b_sub]
    graph.get_ready()
    c_sub_epochs_2 = runner.tube.scheduler.add_sub_epochs(second_b_sub, 2, "c")

    for c_sub in c_sub_epochs_2:
        assert c_sub in runner.tube.scheduler._epoch_log

    runner.tube.scheduler.expire(second_b_sub, "c")

    # Now both b sub-epochs should be done
    for b_sub in b_sub_epochs:
        assert b_sub in runner.tube.scheduler._epoch_log


def test_nested_map_subepoch_state():
    """
    For nested maps, verify that sub-epochs end correctly when inner map creates sub-sub-epochs.

    Note: Sub-epochs with no active nodes end immediately in add_sub_epochs.
    """
    from noob.types import make_root_epoch, make_sub_epoch

    tube = Tube.from_specification("testing-map-nested")
    runner = SynchronousRunner(tube=tube)
    runner.init()

    parent_epoch = make_root_epoch(0)
    runner.tube.scheduler.add_epoch(parent_epoch)

    # Create b's sub-epochs (simulating what happens when b.process returns MapResult)
    b_sub_epochs = runner.tube.scheduler.add_sub_epochs(parent_epoch, 3, "b")
    assert len(b_sub_epochs) == 3

    # Check sub-epoch graph state
    for sub_epoch in b_sub_epochs:
        graph = runner.tube.scheduler._epochs[sub_epoch]
        # b should be done, c should be ready
        assert "b" in graph.done_nodes
        assert "c" in graph.ready_nodes

    # Simulate c processing and creating sub-sub-epochs in first b sub-epoch
    first_sub = b_sub_epochs[0]

    # Get c ready
    graph = runner.tube.scheduler._epochs[first_sub]
    graph.get_ready()  # marks c as out

    # Create c's sub-sub-epochs (these will end immediately since c has no downstream)
    c_sub_epochs = runner.tube.scheduler.add_sub_epochs(first_sub, 2, "c")
    assert len(c_sub_epochs) == 2

    # The c sub-sub-epochs should have ended immediately (no downstream except return)
    for sub_sub_epoch in c_sub_epochs:
        # Sub-epoch is already ended (not in _epochs, but in _epoch_log)
        assert sub_sub_epoch not in runner.tube.scheduler._epochs, \
            f"c sub-epoch {sub_sub_epoch} should have ended immediately"
        assert sub_sub_epoch in runner.tube.scheduler._epoch_log, \
            f"c sub-epoch {sub_sub_epoch} should be in epoch log"


def test_nested_map_downstream_nodes():
    """
    For nested maps, verify the downstream node sets are correct.
    """
    tube = Tube.from_specification("testing-map-nested")
    sched = tube.scheduler

    # From b (first map), downstream should include b and c (maps), but NOT return (sink)
    downstream_b = sched._get_downstream_nodes("b")
    assert "b" in downstream_b
    assert "c" in downstream_b
    assert "return" not in downstream_b

    # With sinks included
    downstream_b_sinks = sched._get_downstream_nodes("b", exclude_sinks=False)
    assert "return" in downstream_b_sinks

    # From c (second map), downstream should include c only, NOT return
    downstream_c = sched._get_downstream_nodes("c")
    assert "c" in downstream_c
    assert "return" not in downstream_c

    # Sinks from c should be {return}
    downstream_c_sinks = sched._get_downstream_nodes("c", exclude_sinks=False)
    sinks_c = downstream_c_sinks - downstream_c
    assert sinks_c == {"return"}


def test_nested_map_subepoch_stays_active():
    """
    When b creates sub-epochs, each sub-epoch should remain active until c runs.
    The sub-epoch should NOT end immediately when created.
    """
    from noob.types import make_root_epoch

    tube = Tube.from_specification("testing-map-nested")
    sched = tube.scheduler

    parent = make_root_epoch(0)
    sched.add_epoch(parent)

    # Create sub-epochs for b
    b_sub_epochs = sched.add_sub_epochs(parent, 2, "b")
    assert len(b_sub_epochs) == 2

    # Each b sub-epoch should still be active (c is ready)
    for sub in b_sub_epochs:
        assert sub in sched._epochs, f"Sub-epoch {sub} should still exist in _epochs"
        graph = sched._epochs[sub]
        assert graph.is_active(), f"Sub-epoch {sub} graph should be active (c is ready)"
        assert "c" in graph.ready_nodes, f"Node 'c' should be ready in sub-epoch {sub}"
        assert "b" in graph.done_nodes, f"Node 'b' should be done in sub-epoch {sub}"

    # Parent should have tracking for b's sub-epochs
    assert parent in sched._subepoch_counts
    assert "b" in sched._subepoch_counts[parent]
    assert sched._subepoch_counts[parent]["b"] == 2


def test_nested_map_subepoch_tracks_inner_maps():
    """
    When c creates sub-sub-epochs from a b sub-epoch, the b sub-epoch should
    track these sub-epochs and stay active until they all complete.

    Since c's sub-epochs are inactive (c has no downstream nodes), they end
    immediately during creation, which triggers completion. The sequence is:
    1. add_sub_epochs for c creates and immediately ends all sub-epochs
    2. When last c sub-epoch ends, completion check fires
    3. Tracking for c is cleaned up
    4. When we call expire(first_b_sub, "c"), the graph is inactive
    5. first_b_sub ends
    """
    from noob.types import make_root_epoch

    tube = Tube.from_specification("testing-map-nested")
    sched = tube.scheduler

    parent = make_root_epoch(0)
    sched.add_epoch(parent)

    # Create sub-epochs for b
    b_sub_epochs = sched.add_sub_epochs(parent, 2, "b")
    first_b_sub = b_sub_epochs[0]
    second_b_sub = b_sub_epochs[1]

    # Both b sub-epochs should be active (c is ready in each)
    for sub in b_sub_epochs:
        assert sub in sched._epochs
        assert sched._epochs[sub].is_active()

    # Now simulate c processing in the first b sub-epoch
    graph = sched._epochs[first_b_sub]
    graph.get_ready()  # marks c as out

    # Create c's sub-epochs - they will ALL end immediately (inactive)
    # because c has no downstream nodes (return is excluded as sink)
    c_sub_epochs = sched.add_sub_epochs(first_b_sub, 3, "c")
    assert len(c_sub_epochs) == 3

    # All c sub-epochs should have ended immediately
    for c_sub in c_sub_epochs:
        assert c_sub not in sched._epochs, f"c sub-epoch {c_sub} should have ended"
        assert c_sub in sched._epoch_log, f"c sub-epoch {c_sub} should be in epoch log"

    # Tracking for c should be cleaned up already
    assert first_b_sub not in sched._subepoch_counts or "c" not in sched._subepoch_counts.get(first_b_sub, {})

    # Now expire c in the b sub-epoch
    sched.expire(first_b_sub, "c")

    # b sub-epoch should have ended (c expired, no sub-epoch tracking)
    assert first_b_sub not in sched._epochs, "first_b_sub should have ended"
    assert first_b_sub in sched._epoch_log, "first_b_sub should be in epoch log"

    # But the second b sub-epoch should still be active
    assert second_b_sub in sched._epochs, "second_b_sub should still be active"

    # And the parent (root) should still be active (waiting for b sub-epochs)
    assert parent in sched._epochs, "parent should still exist"
    assert sched.is_active(parent), "parent should still be active"


def test_nested_map_return_becomes_ready():
    """
    When all b sub-epochs complete, the Return node should become ready
    in the parent (root) epoch.
    """
    from noob.types import make_root_epoch

    tube = Tube.from_specification("testing-map-nested")
    sched = tube.scheduler

    parent = make_root_epoch(0)
    sched.add_epoch(parent)

    # Mark 'a' as done (word source emitted)
    sched.done(parent, "a")

    # Create sub-epochs for b
    b_sub_epochs = sched.add_sub_epochs(parent, 2, "b")

    # Expire b in parent (it ran and created sub-epochs)
    sched.expire(parent, "b")

    # Process each b sub-epoch
    for b_sub in b_sub_epochs:
        # Get c ready
        graph = sched._epochs[b_sub]
        graph.get_ready()

        # c creates and immediately ends its sub-epochs
        sched.add_sub_epochs(b_sub, 2, "c")

        # Expire c (it ran and created sub-epochs)
        sched.expire(b_sub, "c")

    # Now all b sub-epochs should have ended
    for b_sub in b_sub_epochs:
        assert b_sub not in sched._epochs, f"{b_sub} should have ended"
        assert b_sub in sched._epoch_log, f"{b_sub} should be in epoch log"

    # Check parent epoch state
    assert parent in sched._epochs, "parent should still exist"
    parent_graph = sched._epochs[parent]

    # Return should now be ready
    assert "return" in parent_graph.ready_nodes, (
        f"Return should be ready. ready_nodes={parent_graph.ready_nodes}, "
        f"done_nodes={parent_graph.done_nodes}, out_nodes={parent_graph.out_nodes}"
    )


def test_subepoch_graph_structure():
    """
    Verify that sub-epoch graphs contain the correct nodes.
    """
    from noob.types import make_root_epoch, make_sub_epoch

    tube = Tube.from_specification("testing-map-basic")
    runner = SynchronousRunner(tube=tube)
    runner.init()

    parent_epoch = make_root_epoch(0)
    runner.tube.scheduler.add_epoch(parent_epoch)

    # Get downstream nodes from map (what should be in sub-epoch graphs)
    downstream = runner.tube.scheduler._get_downstream_nodes("b")
    print(f"Downstream nodes from map 'b': {downstream}")

    # Should include map and exclaim, but NOT return (sink excluded)
    assert "b" in downstream, "Map node should be in its own downstream set"
    assert "c" in downstream, "Exclaim should be downstream of map"
    assert "return" not in downstream, "Return (sink) should be excluded from sub-epochs"

    # Create sub-epochs
    sub_epochs = runner.tube.scheduler.add_sub_epochs(parent_epoch, 4, "b")
    assert len(sub_epochs) == 4

    # Check each sub-epoch graph
    for i, sub_epoch in enumerate(sub_epochs):
        graph = runner.tube.scheduler._epochs[sub_epoch]
        print(f"Sub-epoch {i} graph: ready={graph.ready_nodes}, done={graph.done_nodes}")

        # Map should already be marked done (not ready)
        assert "b" in graph.done_nodes, f"Map should be done in sub-epoch {i}"
        assert "b" not in graph.ready_nodes, f"Map should not be ready in sub-epoch {i}"

        # Exclaim should be ready (its dependency b is done)
        assert "c" in graph.ready_nodes, f"Exclaim should be ready in sub-epoch {i}"


def test_map_multiple_runs():
    """
    Verify that map works correctly across multiple runs.
    The store should be cleared between runs.
    """
    from noob.types import make_root_epoch, make_sub_epoch

    tube = Tube.from_specification("testing-map-basic")
    runner = SynchronousRunner(tube=tube)
    runner.init()

    # First run
    runner.store.clear()
    parent_epoch_1 = make_root_epoch(0)
    runner.tube.scheduler.add_epoch(parent_epoch_1)
    runner.store.add_value(tube.nodes["a"].signals, "cat", "a", parent_epoch_1)

    map_node = tube.nodes["b"]
    map_node.set_epoch_context(parent_epoch_1)
    result1 = map_node.process("cat")
    for event in result1.events:
        runner.store.add(event)

    # Verify first run
    assert [e["value"] for e in result1.events] == ["c", "a", "t"]

    # Clear between runs (like _before_process does)
    runner.store.clear()

    # Second run
    parent_epoch_2 = make_root_epoch(1)
    runner.tube.scheduler.add_epoch(parent_epoch_2)
    runner.store.add_value(tube.nodes["a"].signals, "dog", "a", parent_epoch_2)

    map_node.set_epoch_context(parent_epoch_2)
    result2 = map_node.process("dog")
    for event in result2.events:
        runner.store.add(event)

    # Verify second run
    assert [e["value"] for e in result2.events] == ["d", "o", "g"]

    # Verify input collection in second run's sub-epochs
    exclaim_edges = tube.in_edges(tube.nodes["c"])
    for i, expected_char in enumerate(["d", "o", "g"]):
        sub_epoch = make_sub_epoch(parent_epoch_2, i, "b")
        collected = runner.store.collect(exclaim_edges, sub_epoch, hierarchical=True)
        assert collected is not None
        assert collected["string"] == expected_char, (
            f"Second run sub-epoch {i}: expected '{expected_char}', got '{collected['string']}'"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded_tube", ["testing-map-basic"], indirect=True)
async def test_epoch_structure_basic(loaded_tube: Tube, all_runners: TubeRunner):
    """
    Verify the epoch structure for basic map operations.
    """
    runner = all_runners

    # Add a callback to track events
    events_seen = []

    def track_events(event):
        if event.get("node_id") != "meta":
            events_seen.append(event)

    runner.add_callback(track_events)

    await _call_process(runner)

    # Find events from the map node (node 'b')
    map_events = [e for e in events_seen if e["node_id"] == "b"]

    # Each mapped event should be in a sub-epoch (depth 2)
    for event in map_events:
        assert epoch_depth(event["epoch"]) == 2, (
            f"Map event should be at epoch depth 2, got {epoch_depth(event['epoch'])}"
        )

    # Events from non-mapped nodes should be at depth 1
    source_events = [e for e in events_seen if e["node_id"] == "a"]
    for event in source_events:
        assert epoch_depth(event["epoch"]) == 1, (
            f"Source event should be at epoch depth 1, got {epoch_depth(event['epoch'])}"
        )
