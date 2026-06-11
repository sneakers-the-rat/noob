# noob-core

Optimized rust implementation of the [noob](https://github.com/miniscope/noob)
scheduler and topological sorter, exposed via pyO3 bindings.

`noob-core` is an optional accelerator: when it is installed, `noob` transparently
uses the rust scheduler instead of the pure-python one. No code changes are needed -
`noob.scheduler.Scheduler` *is* the rust-backed scheduler when `noob-core` is importable.

```bash
pip install noob-core
```

To force the pure-python scheduler even when `noob-core` is installed,
set the environment variable `NOOB_SCHEDULER=python` before importing `noob`.

## Design

All scheduler state - the per-epoch topological sorters, epoch bookkeeping,
subepoch relationships, and frozen graph templates - lives in rust
(`CoreScheduler` in this crate). Only native types (strings, ints, bools,
tuples, lists) cross the python:rust barrier:

- a graph item is a node id `str` or a `(node_id, signal)` tuple
- an epoch is a tuple of `(node_id, int)` segments
- events are decomposed to `(epoch, node_id, signal, is_noevent, is_meta)` tuples

The python-side wrapper (`noob.rust_scheduler.RustScheduler`) converts
`Edge` / `NodeSpecification` / `Epoch` / `Event` objects at the boundary and
reconstructs `Epoch`s, `MetaEvent`s, and `TopoSorter`-compatible views on the
way back, so it is a drop-in replacement for `noob.scheduler.Scheduler`.

## Building

```bash
# from packages/noob-core
pip install maturin
maturin develop --release
# or build a wheel
maturin build --release
```
