import pytest

from noob import Tube
from noob.exceptions import ExtraInputWarning, InputMissingError
from noob.runner import TubeRunner
from noob.runner.zmq import ZMQRunner
from noob.input import InputScope
from noob.event import Event
from noob.network.message import Message, MessageType


def test_tube_input_params(runner_cls: type[TubeRunner]):
    """tube-scoped input can be used as params"""
    tube = Tube.from_specification("testing-input-tube-params", input={"start": 7})
    with runner_cls(tube) as runner:
        outputs = runner.run(n=5)
    assert len(outputs) == 5
    assert outputs == [7, 8, 9, 10, 11]


def test_tube_input_depends(runner_cls: type[TubeRunner]):
    """tube-scoped input can be used as depends"""
    tube = Tube.from_specification("testing-input-tube-depends", input={"multiply_right": 7})
    with runner_cls(tube) as runner:
        outputs = runner.run(n=5)
    assert len(outputs) == 5
    assert outputs == [i * 7 for i in range(5)]


def test_process_input_params():
    """process-scoped input can NOT be used as params"""
    with (
        pytest.raises(InputMissingError),
        pytest.warns(ExtraInputWarning, match=r"Ignoring extra.*"),
    ):
        Tube.from_specification("testing-input-process-params", input={"start": 7})


def test_process_input_depends(runner_cls: type[TubeRunner]):
    """process-scoped input can be used as depends"""
    tube = Tube.from_specification("testing-input-process-depends")
    with runner_cls(tube) as runner:
        for left, right in zip(range(5), range(5, 10)):
            assert runner.process(multiply_right=right) == left * right


@pytest.mark.parametrize("tube", ("testing-input-tube-params", "testing-input-tube-depends"))
def test_tube_input_missing(tube):
    """Input scoped as tube input raises an error when missing"""
    with pytest.raises(InputMissingError):
        Tube.from_specification(tube)

    with (
        pytest.raises(InputMissingError),
        pytest.warns(ExtraInputWarning, match=r"Ignoring extra.*"),
    ):
        Tube.from_specification(tube, input={"irrelevant": 7})


@pytest.mark.parametrize("tube_name", ("testing-input-process-depends",))
def test_process_input_missing(tube_name: str, runner_cls: type[TubeRunner]):
    """Input scoped as process input raises an error when missing"""
    tube = Tube.from_specification(tube_name)
    with runner_cls(tube) as runner:
        with pytest.raises(InputMissingError):
            runner.process()

        with (
            pytest.raises(InputMissingError),
            pytest.warns(ExtraInputWarning, match=r"Ignoring extra.*"),
        ):
            runner.process(irrelevant=1)

        # nodes were not run
        assert runner.process(multiply_right=10) == 0


def test_input_integration(runner_cls: type[TubeRunner]):
    """
    All the different forms of input can be used together
    """
    start = 7
    multiply_tube = 13
    multiply_process = 17
    tube = Tube.from_specification(
        "testing-input-mixed", input={"start": start, "multiply_tube": multiply_tube}
    )

    with runner_cls(tube) as runner:
        for i in range(5):
            this_process = multiply_process * 2 * (i + 1)
            expected = (start + i) * multiply_tube * this_process
            assert runner.process(multiply_process=this_process) == expected

@pytest.mark.zmq_runner
def test_zmq_out_of_order():
    """
    When network latency or multithreading causes messages to arrive out of order,
    we still run the nodes in correct order
    """
    tube = Tube.from_specification("testing-input-process-depends")
    lefts = [i for i in range(5)]
    rights = [3, 5, 7, 11, 17]
    correct = [left * right for left, right in  zip(lefts, rights)]
    scrambled_epochs = [4, 2, 0, 3, 1]
    scrambled = [rights[epoch] for epoch in scrambled_epochs]

    messages: list[Message] = []
    returns = []
    def _event_cb(msg: Message):
        messages.append(msg)

    with ZMQRunner(tube) as runner:
        # simulate the process method having been called several times,
        # but the messages were delivered out of order
        runner.command.add_callback("inbox", _event_cb)

        # send the messages in a burst, e.g. like network latency
        for epoch, an_input in zip(scrambled_epochs, scrambled):
            validated = runner.tube.input_collection.validate_input(InputScope.process, {"multiply_right": an_input})
            # we explicitly set epoch here, but in the actual case the epoch would have incremented every call
            runner._current_epoch = runner.tube.scheduler.add_epoch(epoch)
            runner._mark_pseudo_nodes_done(epoch)
            runner.command.process(epoch, validated)

        # brief delay to ensure all ProcessMsg arrive at NodeRunners before processing starts
        # this allows epochs to be processed in numerical order (via sorted scheduler)
        import time
        time.sleep(0.1)

        # wait until all the epochs have completed
        for epoch in range(5):
            runner.tube.scheduler.await_epoch(epoch)

        # the store is persistent across epochs, so we can collect it all at the end
        for epoch in range(5):
            returns.append(runner.collect_return(epoch))

    # we should have gotten the expected order of the return values:
    # the inputs were run in order, epochs 0 through 5 with the "rights" values
    # so the outputs should go in the same order, even if there was network latency or threading probs
    assert correct == returns
    # the events should match
    b_messages = [msg for msg in messages if msg.type_ == MessageType.event and msg.node_id == "b"]
    b_events: list[Event] = []
    for msg in b_messages:
        b_events.extend(msg.value)
    b_events = sorted(b_events, key=lambda e: e['epoch'])
    assert [evt['value'] for evt in b_events] == correct








