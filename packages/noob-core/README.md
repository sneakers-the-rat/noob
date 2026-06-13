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

The scheduler is a container of topo sorters:

- `CoreTopoSorter` is a standalone topological sorter over interned graph
  items. It knows nothing about the scheduler.
- `CoreScheduler` owns one sorter per epoch (plus the frozen graph templates
  epochs are cloned from) and hands out shared `CoreTopoSorter` handles, so
  driving a handle with `get_ready()`/`done()` advances the real epoch.

All scheduler state and logic live in rust. Conversion happens once, at the
boundary: the core accepts and returns real `noob` objects ( `Epoch` ,
`NodeSignal` , `MetaEvent` dicts) and raises `noob.exceptions` types directly,
constructing them through cached class references rather than calling back
into python logic.

The package is a mixed rust/python layout: the compiled extension is the inner
module `noob_core._core`, and `noob_core/__init__.py` re-exports it and adds
the python adapters (`noob_core.RustScheduler` / `RustTopoSorter`). The
adapters are near-pure delegation, existing only to carry the `nodes` /
`edges` python attributes, to subclass `TopoSorter` for `isinstance`
compatibility, and to provide the `iter_epoch` / `iter_events` generators.
`noob.scheduler` imports `RustScheduler` from here when the package is
installed.

`noob-core` depends on `noob` and only works alongside it: the rust core and
the adapters are both defined in terms of noob's domain types. (The inner
`noob_core._core` extension's `CoreTopoSorter` can be exercised without `noob`
loaded - returning plain tuples for `(node, signal)` items and raising its own
exception types - but the package as a whole, and the scheduler's
event-facing APIs, require `noob`.)

## Building

Installed editable as part of the noob dev environment:

```bash
pip install -e packages/noob-core
```

Re-run after changing rust sources to recompile (builds are incremental).
