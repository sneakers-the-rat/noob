use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::bridge::Bridge;

// Fallback exception types, used only when the `noob` package is not
// importable (i.e. when noob_core is used standalone). Inside noob, errors
// are raised as the corresponding `noob.exceptions` types directly.
create_exception!(
    noob_core,
    SchedulerError,
    PyException,
    "Base error for noob-core scheduler errors"
);
create_exception!(
    noob_core,
    AlreadyDoneError,
    SchedulerError,
    "Node was marked done, but it was already done!"
);
create_exception!(
    noob_core,
    NotAddedError,
    SchedulerError,
    "Node was marked done but wasn't added!"
);
create_exception!(
    noob_core,
    EpochExistsError,
    SchedulerError,
    "Epoch already exists and is active, but attempted to create it."
);
create_exception!(
    noob_core,
    EpochCompletedError,
    SchedulerError,
    "An epoch was already completed, but some attempt was made to update it or use it."
);

/// Internal error type so core logic can branch on error kinds
/// (e.g. `update` suppresses AlreadyDone/NotAdded for the first
/// done-marking of each node) without touching python exception objects.
#[derive(Clone, Debug)]
pub enum CoreError {
    AlreadyDone(String),
    NotAdded(String),
    EpochExists(String),
    EpochCompleted(String),
    Value(String),
    Key(String),
    Type(String),
}

pub type CoreResult<T> = Result<T, CoreError>;

fn noob_exception(py: Python<'_>, err: &CoreError) -> Option<PyErr> {
    let bridge = Bridge::get(py).ok()?;
    let (cls, msg) = match err {
        CoreError::AlreadyDone(msg) => (&bridge.exc_already_done, msg),
        CoreError::NotAdded(msg) => (&bridge.exc_not_added, msg),
        CoreError::EpochExists(msg) => (&bridge.exc_epoch_exists, msg),
        CoreError::EpochCompleted(msg) => (&bridge.exc_epoch_completed, msg),
        _ => return None,
    };
    let instance = cls.bind(py).call1((msg,)).ok()?;
    Some(PyErr::from_value(instance))
}

impl From<CoreError> for PyErr {
    fn from(err: CoreError) -> PyErr {
        Python::with_gil(|py| {
            if let Some(e) = noob_exception(py, &err) {
                return e;
            }
            match err {
                CoreError::AlreadyDone(msg) => AlreadyDoneError::new_err(msg),
                CoreError::NotAdded(msg) => NotAddedError::new_err(msg),
                CoreError::EpochExists(msg) => EpochExistsError::new_err(msg),
                CoreError::EpochCompleted(msg) => EpochCompletedError::new_err(msg),
                CoreError::Value(msg) => PyValueError::new_err(msg),
                CoreError::Key(msg) => PyKeyError::new_err(msg),
                CoreError::Type(msg) => PyTypeError::new_err(msg),
            }
        })
    }
}
