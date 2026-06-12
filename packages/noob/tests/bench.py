import pytest
from pytest_codspeed.plugin import BenchmarkFixture

from noob import Tube
from noob.edge import Edge
from noob.runner.base import TubeRunner
from noob.scheduler import Scheduler


def test_load_tube(benchmark: BenchmarkFixture) -> None:
    benchmark(lambda: Tube.from_specification("testing-kitchen-sink"))


@pytest.mark.parametrize("loaded_tube", ["testing-kitchen-sink"], indirect=True)
def test_kitchen_sink_process(benchmark: BenchmarkFixture, runner: TubeRunner) -> None:
    benchmark(lambda: runner.process())


@pytest.mark.parametrize("loaded_tube", ["testing-kitchen-sink"], indirect=True)
def test_kitchen_sink_run(benchmark: BenchmarkFixture, runner: TubeRunner) -> None:
    benchmark(lambda: runner.run(n=10))


@pytest.mark.parametrize("loaded_tube", ["testing-long-add"], indirect=True)
def test_long_add(benchmark: BenchmarkFixture, runner: TubeRunner) -> None:
    """
    ZMQ runner should be faster for tubes where nodes take a long time
    and there's lots of concurrency possibilities
    """
    benchmark(lambda: runner.process())


@pytest.mark.parametrize("loaded_tube", ["testing-kitchen-sink"], indirect=True)
def test_topo_sorter(benchmark: BenchmarkFixture, loaded_tube: Tube) -> None:
    """
    Our TopoSorter should not get uh slower
    """
    benchmark(lambda: _run_sorter(loaded_tube))


def _run_sorter(tube: Tube) -> None:
    epoch = tube.scheduler.add_epoch()
    sorter = tube.scheduler._epochs[epoch]
    while sorter.is_active():
        ready_nodes = sorter.get_ready()
        for node in ready_nodes:
            sorter.done(node)


@pytest.fixture
def wide_scheduler() -> Scheduler:
    """
    A scheduler over a wide, layered synthetic graph (200 nodes, ~400 edges),
    larger than the testing tubes, to benchmark scheduling at a size where
    graph work dominates over per-call overhead.
    """
    width, depth = 20, 10
    edges = []
    for layer in range(1, depth):
        for i in range(width):
            edges.append(
                Edge(
                    source_node=f"n{layer - 1}_{i}",
                    source_signal="value",
                    target_node=f"n{layer}_{i}",
                    target_slot="value",
                )
            )
            edges.append(
                Edge(
                    source_node=f"n{layer - 1}_{(i + 1) % width}",
                    source_signal="other",
                    target_node=f"n{layer}_{i}",
                    target_slot="other",
                )
            )
    return Scheduler(nodes={}, edges=edges)


def test_scheduler_add_epoch(benchmark: BenchmarkFixture, wide_scheduler: Scheduler) -> None:
    """
    Creating and ending epochs (i.e. copying the frozen graph template)
    should be fast on graphs with hundreds of nodes
    """

    def _add_end() -> None:
        epoch = wide_scheduler.add_epoch()
        wide_scheduler.end_epoch(epoch)

    benchmark(_add_end)


def test_scheduler_epoch_drive(benchmark: BenchmarkFixture, wide_scheduler: Scheduler) -> None:
    """
    Driving a full epoch through its sorter should be fast
    on graphs with hundreds of nodes
    """

    def _drive() -> None:
        epoch = wide_scheduler.add_epoch()
        sorter = wide_scheduler._epochs[epoch]
        while sorter.is_active():
            sorter.done(*sorter.get_ready())

    benchmark(_drive)
