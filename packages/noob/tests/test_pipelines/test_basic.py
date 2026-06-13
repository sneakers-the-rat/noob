from itertools import cycle

import pytest

from noob import Tube
from noob.event import MetaSignal
from noob.runner import TubeRunner
from noob.utils import iscoroutinefunction_partial


@pytest.mark.parametrize("loaded_tube", ["testing-basic"], indirect=True)
def test_basic_process(loaded_tube: Tube, runner: TubeRunner):
    """The most basic tube! We can process a fixed number of events"""
    outputs = []
    for _ in range(5):
        outputs.append(runner.process())
    assert len(outputs) == 5
    assert outputs == [0, 2, 4, 6, 8]


@pytest.mark.parametrize("loaded_tube", ["testing-basic"], indirect=True)
def test_basic_run(loaded_tube: Tube, runner: TubeRunner):
    """The most basic tube! We can process a fixed number of events"""
    outputs = runner.run(n=5)
    assert len(outputs) == 5
    assert outputs == [0, 2, 4, 6, 8]


@pytest.mark.parametrize("loaded_tube", ["testing-basic"], indirect=True)
def test_basic_iter(loaded_tube: Tube, runner: TubeRunner):
    """We should also be able to iterate over values"""
    expected = [0, 2, 4, 6, 8]
    for e, value in zip(expected, runner.iter(n=5)):
        assert value == e


@pytest.mark.parametrize("loaded_tube", ["testing-branch"], indirect=True)
def test_branch(loaded_tube: Tube, runner: TubeRunner):
    """A nodes output can be branched and received by multiple nodes!"""
    expected = [{"multiply": i * 2, "divide": i / 5} for i in range(5)]

    for e, value in zip(expected, runner.iter(n=5)):
        assert value == e


@pytest.mark.parametrize("loaded_tube", ["testing-branch-switching"], indirect=True)
def test_branch_switching(loaded_tube: Tube, runner: TubeRunner):
    """Nodes can have switching outputs - yielding only a subset of thier signals"""
    expected = cycle(["fruit", "vegetable", "mineral"])
    keys = cycle(["this", "that", "the_other"])
    for _ in range(5):
        value = runner.process()
        e = next(expected)
        key = next(keys)
        assert len(value) == 2
        assert e in value
        assert value[e].endswith("!")
        assert value["this_or_that"][key] == value[e]


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded_tube", ["testing-noevent-chain"], indirect=True)
async def test_noevent_cancels_chain(loaded_tube: Tube, all_runners: TubeRunner):
    """
    A NoEvent cancels everything downstream of it for the rest of the epoch,
    and the epoch still completes - including in the zmq runner, where nodes
    several hops downstream don't subscribe to the NoEvent emitter and
    cancellation must propagate hop-by-hop
    (the canceled nodes emit NoEvents to their own subscribers).
    """
    runner = all_runners
    results = []
    for _ in range(6):
        if iscoroutinefunction_partial(runner.process):
            results.append(await runner.process())
        else:
            results.append(runner.process())

    values = [r["result"] for r in results if r is not None and r is not MetaSignal.NoEvent]
    skipped = [r for r in results if r is None or r is MetaSignal.NoEvent]
    # count emits 0..5; odd values are skipped, even values are quadrupled
    assert values == [0, 8, 16]
    assert len(skipped) == 3


@pytest.mark.parametrize("loaded_tube", ["testing-merge"], indirect=True)
def test_merge(loaded_tube: Tube, runner: TubeRunner):
    """Multiple node outputs can be merged into one node!"""
    expected = [(i * 2) / j for i, j in zip(range(5), range(5, 10))]

    for e, value in zip(expected, runner.iter(n=5)):
        assert value == e


@pytest.mark.parametrize("loaded_tube", ["testing-gather-n"], indirect=True)
def test_gather_n(loaded_tube: Tube, runner: TubeRunner):
    """A node can gather n inputs into one call"""
    expected = ["abcde", "fghij", "klmno", "pqrst", "uvwxy"]

    for e, value in zip(expected, runner.iter(n=5)):
        assert value == {"word": e}


@pytest.mark.parametrize("loaded_tube", ["testing-gather-dependent"], indirect=True)
def test_gather_dependent(loaded_tube: Tube, runner: TubeRunner):
    """A node can gather inputs from one slot when another slot receives an event"""
    expected = [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8],
        [9, 10, 11],
        [12, 13, 14],
    ]

    for e, value in zip(expected, runner.iter(n=5)):
        assert isinstance(value, dict)
        assert len(value) == 1
        value = value["word"]
        inner = value[list(value.keys())[0]]
        assert inner == e


@pytest.mark.parametrize("loaded_tube", ["testing-multi-signal"], indirect=True)
def test_multi_signal(loaded_tube: Tube, sync_runner_cls):
    """
    Nodes that emit multiple signals can have each used independently
    """
    tube = Tube.from_specification("testing-multi-signal")
    runner = sync_runner_cls(tube)

    with runner:
        for value in runner.iter(n=5):
            assert isinstance(value, dict)
            assert isinstance(value["word"], str)
            assert value["count_sum"] == sum(value["counts"])
