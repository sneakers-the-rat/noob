//! Optimized rust core for the noob scheduler.
//!
//! Exposes [`scheduler::CoreScheduler`], a faithful port of
//! `noob.scheduler.Scheduler` + `noob.toposort.TopoSorter` that holds all
//! state in rust and only passes native types (strings, ints, bools, tuples,
//! lists) across the python boundary.

use pyo3::prelude::*;

pub mod epoch;
pub mod errors;
pub mod item;
pub mod pysorter;
pub mod scheduler;
pub mod sorter;

#[pymodule]
fn noob_core(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<scheduler::CoreScheduler>()?;
    m.add_class::<pysorter::CoreTopoSorter>()?;
    m.add("SchedulerError", py.get_type::<errors::SchedulerError>())?;
    m.add(
        "AlreadyDoneError",
        py.get_type::<errors::AlreadyDoneError>(),
    )?;
    m.add("NotAddedError", py.get_type::<errors::NotAddedError>())?;
    m.add(
        "EpochExistsError",
        py.get_type::<errors::EpochExistsError>(),
    )?;
    m.add(
        "EpochCompletedError",
        py.get_type::<errors::EpochCompletedError>(),
    )?;
    Ok(())
}
