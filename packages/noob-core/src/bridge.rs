//! Cached references to the python classes that cross the python:rust
//! barrier, so rust can construct real `Epoch` / `NodeSignal` / `MetaEvent`
//! objects (and raise real `noob.exceptions` types) at the boundary instead
//! of leaving conversion shims on the python side.
//!
//! Imports are resolved lazily on first use and cached for the lifetime of
//! the interpreter: by the time any scheduler method runs, `noob` is fully
//! imported (it is what imported `noob_core`).

use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyDict, PyTuple};

use crate::epoch::EpochKey;

pub struct Bridge {
    /// noob.types.Epoch
    pub epoch: Py<PyAny>,
    /// noob.types.EpochSegment
    pub epoch_segment: Py<PyAny>,
    /// noob.types.NodeSignal
    pub node_signal: Py<PyAny>,
    /// noob.toposort._NodeInfo
    pub node_info: Py<PyAny>,
    /// noob.event.MetaEventType.NodeReady
    pub node_ready: Py<PyAny>,
    /// noob.event.MetaEventType.EpochEnded
    pub epoch_ended: Py<PyAny>,
    /// noob.event.MetaEventType.NodeCanceled
    pub node_canceled: Py<PyAny>,
    /// noob.event.MetaSignal.NoEvent
    pub noevent: Py<PyAny>,
    /// datetime.datetime
    pub datetime: Py<PyAny>,
    /// datetime.timezone.utc
    pub utc: Py<PyAny>,
    pub exc_already_done: Py<PyAny>,
    pub exc_not_added: Py<PyAny>,
    pub exc_epoch_exists: Py<PyAny>,
    pub exc_epoch_completed: Py<PyAny>,
}

static BRIDGE: GILOnceCell<Bridge> = GILOnceCell::new();

impl Bridge {
    pub fn get(py: Python<'_>) -> PyResult<&'static Bridge> {
        BRIDGE.get_or_try_init(py, || {
            let types = py.import("noob.types")?;
            let event = py.import("noob.event")?;
            let toposort = py.import("noob.toposort")?;
            let exceptions = py.import("noob.exceptions")?;
            let datetime = py.import("datetime")?;
            let meta_event_type = event.getattr("MetaEventType")?;
            Ok(Bridge {
                epoch: types.getattr("Epoch")?.unbind(),
                epoch_segment: types.getattr("EpochSegment")?.unbind(),
                node_signal: types.getattr("NodeSignal")?.unbind(),
                node_info: toposort.getattr("_NodeInfo")?.unbind(),
                node_ready: meta_event_type.getattr("NodeReady")?.unbind(),
                epoch_ended: meta_event_type.getattr("EpochEnded")?.unbind(),
                node_canceled: meta_event_type.getattr("NodeCanceled")?.unbind(),
                noevent: event.getattr("MetaSignal")?.getattr("NoEvent")?.unbind(),
                datetime: datetime.getattr("datetime")?.unbind(),
                utc: datetime.getattr("timezone")?.getattr("utc")?.unbind(),
                exc_already_done: exceptions.getattr("AlreadyDoneError")?.unbind(),
                exc_not_added: exceptions.getattr("NotAddedError")?.unbind(),
                exc_epoch_exists: exceptions.getattr("EpochExistsError")?.unbind(),
                exc_epoch_completed: exceptions.getattr("EpochCompletedError")?.unbind(),
            })
        })
    }
}

/// Construct a `noob.types.Epoch` from a native epoch key
pub fn epoch_to_py<'py>(py: Python<'py>, key: &EpochKey) -> PyResult<Bound<'py, PyAny>> {
    let bridge = Bridge::get(py)?;
    let segment_cls = bridge.epoch_segment.bind(py);
    let segments = key
        .iter()
        .map(|(node_id, number)| segment_cls.call1((node_id, *number)))
        .collect::<PyResult<Vec<_>>>()?;
    bridge.epoch.bind(py).call1((PyTuple::new(py, segments)?,))
}

/// `datetime.now()` (naive) or `datetime.now(UTC)` (aware)
pub fn now<'py>(py: Python<'py>, aware: bool) -> PyResult<Bound<'py, PyAny>> {
    let bridge = Bridge::get(py)?;
    let datetime = bridge.datetime.bind(py);
    if aware {
        datetime.call_method1("now", (bridge.utc.bind(py),))
    } else {
        datetime.call_method0("now")
    }
}

/// Construct a `noob.event.MetaEvent` dict.
/// `signal` is one of the cached `MetaEventType` members.
pub fn meta_event<'py>(
    py: Python<'py>,
    signal: &Py<PyAny>,
    epoch: &EpochKey,
    value: Bound<'py, PyAny>,
    aware_timestamp: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let event = PyDict::new(py);
    // matches uuid4().int: a random 128-bit int with uuid4 version bits
    event.set_item("id", uuid::Uuid::new_v4().as_u128())?;
    event.set_item("timestamp", now(py, aware_timestamp)?)?;
    event.set_item("node_id", "meta")?;
    event.set_item("signal", signal.bind(py))?;
    event.set_item("epoch", epoch_to_py(py, epoch)?)?;
    event.set_item("value", value)?;
    Ok(event)
}

/// An `EpochEnded` MetaEvent: value is the epoch, timestamp is UTC-aware
pub fn epoch_ended_event<'py>(py: Python<'py>, epoch: &EpochKey) -> PyResult<Bound<'py, PyDict>> {
    let bridge = Bridge::get(py)?;
    meta_event(
        py,
        &bridge.epoch_ended,
        epoch,
        epoch_to_py(py, epoch)?,
        true,
    )
}

/// A `NodeReady` MetaEvent: value is the node id, timestamp is naive
pub fn node_ready_event<'py>(
    py: Python<'py>,
    epoch: &EpochKey,
    node_id: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let bridge = Bridge::get(py)?;
    meta_event(
        py,
        &bridge.node_ready,
        epoch,
        node_id.into_pyobject(py)?.into_any(),
        false,
    )
}

/// A `NodeCanceled` MetaEvent: value is the node id, timestamp is UTC-aware
pub fn node_canceled_event<'py>(
    py: Python<'py>,
    epoch: &EpochKey,
    node_id: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let bridge = Bridge::get(py)?;
    meta_event(
        py,
        &bridge.node_canceled,
        epoch,
        node_id.into_pyobject(py)?.into_any(),
        true,
    )
}
