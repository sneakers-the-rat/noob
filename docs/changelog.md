# Changelog

## v1000.*

### Unreleased

**Added**

- [`#144`](https://github.com/miniscope/noob/issues/144),
  [`#140`](https://github.com/miniscope/noob/issues/140) -
  Scheduling policy moved out of the runners and into the graph model of the
  scheduler and topological sorter (implemented in both the python scheduler
  and the rust core):

  - *Cancellation is graph behavior:* `TopoSorter.mark_expired(cascade=True)`
    transitively expires everything that requires an item that expired without
    running, so a `NoEvent` cancels the rest of the epoch downstream of it
    instead of leaving nodes in limbo. `Scheduler.update` returns
    `NodeCanceled` MetaEvents (a new `MetaEventType`) for the canceled nodes.
    The zmq `NodeRunner` reacts by publishing NoEvents for its own signals, so
    cancellation propagates hop-by-hop along the same edges as data - fixing
    node runners waiting forever on epochs canceled by a `NoEvent` emitted
    several hops upstream (which they don't subscribe to).
  - *Statefulness is scheduler state:* node statefulness is resolved onto
    `NodeSpecification` when nodes are instantiated, and `Scheduler.get_ready`
    withholds stateful nodes from epochs (and sibling subepochs) until they
    have completed all earlier ones - the `expected_epoch` bookkeeping in the
    zmq `NodeRunner` is gone, and the async runner no longer runs stateful
    nodes out of order across subepochs.
  - *Run control:* `Scheduler.queue_epoch` / `queue_epochs` / `set_freerun`
    grant permission to run epochs (`process` / bounded `start` / freerun),
    replacing the todo-queues, counters, and flags previously duplicated
    across runners.
  - *Two iterators drive all runners:*
    `Scheduler.iter_epoch(epoch=None, node_id=None)` yields scheduling events
    for a single epoch until it completes (the synchronous and asyncio
    runners' `process`), and `Scheduler.iter_events(node_id=None)` yields
    events until stopped, running granted epochs and managing epoch lifecycle
    from a single node's point of view (the zmq `NodeRunner` loop, replacing
    `await_node` and most of `await_inputs`).
- `noob-core` - an optional rust implementation of the scheduler and topo sorter
  (`packages/noob-core`, pyO3 bindings).
  When installed, `noob.scheduler.Scheduler` *is* the rust-backed
  `noob_core.RustScheduler`, a drop-in replacement for the public
  scheduler API - no code changes needed. The pure-python scheduler remains in
  place and is used when `noob-core` is not installed,
  or when `NOOB_SCHEDULER=python` is set.
  The rust `CoreScheduler` owns one `CoreTopoSorter` per epoch; all scheduler
  logic runs in rust, and `Epoch`/`NodeSignal`/`MetaEvent` objects are
  constructed at the barrier, so the python adapters are pure delegation.
  `noob-core` is a mixed rust/python package: the compiled extension is the
  inner module `noob_core._core`, and the `noob_core` package adds the
  `RustScheduler` / `RustTopoSorter` adapters, so the rust-backed scheduler
  lives entirely in `noob-core` (it depends on `noob` for the domain types).
  ~3x faster epoch scheduling on medium-sized (hundreds of nodes) graphs.

**Changed**

- NoEvent-valued events are no longer stored by the zmq runners' event stores:
  they are scheduling information, not collectible values
  (downstream collectors like the `Return` node now see an absence
  rather than a `NoEvent` value).

### v1000.1.0 - 26-05-18

**Added**

- [`#195`](https://github.com/miniscope/noob/pull/195),
  [`#196`](https://github.com/miniscope/noob/pull/196) -
  `extends` keyword - allow a tube to extend other tubes!
  reuse tubes, add on new nodes, override them, and so on.
  build bigger tubes out of smaller tube fragments.
- [`#196`](https://github.com/miniscope/noob/pull/196) -
  `NoEventable[]` convenience generic that indicates that a return type can also be NoEvent.
- [`#207`](https://github.com/miniscope/noob/pull/207),
  [`#209`](https://github.com/miniscope/noob/pull/209),
  [`#214`](https://github.com/miniscope/noob/pull/214),
  [`#222`](https://github.com/miniscope/noob/pull/222) - 
  Big improvements to the display of tubes. 
  A `noob view` cli command to show a live-updating display of a tube as it is edited on disk.
  Better representations of tube specs: nested tubes, correct signals and slots from inspecting nodes,
  better use of the ELK layout engine.

**Changed**

- [`#211`](https://github.com/miniscope/noob/pull/211) - 
  Allow accessing a node's signals and slots from a classmethod,
  avoids needing to instantiate a node in order to inspect its edge properties
- [`#212`](https://github.com/miniscope/noob/pull/212) - 
  Make the `signals` and `slots` accessors have the same type: dicts rather than dicts and lists
- [`#213`](https://github.com/miniscope/noob/pull/213) - 
  An additional `NodeInfo` dictionary contains metadata about signals and slots
  derived from the combination of the node specification and the node class.

### v1000.0.1 - 26-03-15

**Fix**
- [#192](https://github.com/miniscope/noob/issues/192), 
  [#201](https://github.com/miniscope/noob/issues/201),
  [#202](https://github.com/miniscope/noob/pull/202) - 
  Support event values that can't use the `==` operator by using `is` to check for NoEvents

### v1000.0.0 - 26-03-13

First "official" beta release with all target features working :).

ok NOW the changelog officials starts since we're now releasing versions regularly.

## v0.1.*

### v0.1.0 - 25-12-09

- Start actually publishing versions.
- Begin changelog

Recent changes

- [#54](https://github.com/miniscope/noob/pull/54) - ZMQ Runner
- [#72](https://github.com/miniscope/noob/pull/72) - Make `NoEvent` a `MetaSignal` enum
- [#51](https://github.com/miniscope/noob/pull/51) - Recursive Tubes

## v0.0.*

### v0.0.9999999

```{raw} html
:file: assets/important.html
```