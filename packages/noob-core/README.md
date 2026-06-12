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
into python logic. The python wrappers
(`noob.rust_scheduler.RustScheduler` / `RustTopoSorter`) are pure delegation,
existing only to carry the `nodes` / `edges` python attributes and to subclass
`TopoSorter` for `isinstance` compatibility.

When used standalone (without `noob` installed), `CoreTopoSorter` still works,
returning plain tuples for `(node, signal)` items and raising its own
exception types; the scheduler's event-facing APIs require `noob`.

## Building

Installed editable as part of the noob dev environment:

```bash
pip install -e packages/noob-core
```

Re-run after changing rust sources to recompile (builds are incremental).
