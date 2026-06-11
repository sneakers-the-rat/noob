use std::fmt;

use indexmap::IndexSet;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// A graph item: either a node id, or a (node id, signal name) pair.
///
/// The native representation of `noob.types.NodeID | noob.types.NodeSignal`:
/// a `str` or a 2-tuple of `str` on the python side.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Item {
    Node(String),
    Signal(String, String),
}

impl Item {
    pub fn is_signal(&self) -> bool {
        matches!(self, Item::Signal(..))
    }

    /// The node id: itself for nodes, the node part for signals
    pub fn node_id(&self) -> &str {
        match self {
            Item::Node(n) => n,
            Item::Signal(n, _) => n,
        }
    }
}

impl fmt::Display for Item {
    /// Match the python repr: `'node'` for node ids (str repr),
    /// `('node', 'signal')` for signals (NodeSignal repr)
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Item::Node(n) => write!(f, "'{n}'"),
            Item::Signal(n, s) => write!(f, "('{n}', '{s}')"),
        }
    }
}

impl<'py> FromPyObject<'py> for Item {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            return Ok(Item::Node(s));
        }
        if let Ok((node, signal)) = ob.extract::<(String, String)>() {
            return Ok(Item::Signal(node, signal));
        }
        Err(PyTypeError::new_err(
            "graph items must be a node id string or a (node_id, signal) tuple",
        ))
    }
}

impl<'py> IntoPyObject<'py> for Item {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            Item::Node(n) => n.into_pyobject(py)?.into_any(),
            // construct a real noob.types.NodeSignal when noob is importable,
            // a plain tuple (equal to it) when used standalone
            Item::Signal(n, s) => match crate::bridge::Bridge::get(py) {
                Ok(bridge) => bridge.node_signal.bind(py).call1((n, s))?,
                Err(_) => (n, s).into_pyobject(py)?.into_any(),
            },
        })
    }
}

/// Interns [`Item`]s to dense `u32` ids shared by all sorters in a scheduler,
/// so that all graph algorithms operate on integers rather than strings.
#[derive(Clone, Debug, Default)]
pub struct Interner {
    items: IndexSet<Item>,
}

impl Interner {
    pub fn intern(&mut self, item: Item) -> u32 {
        self.items.insert_full(item).0 as u32
    }

    pub fn intern_node(&mut self, id: &str) -> u32 {
        self.intern(Item::Node(id.to_owned()))
    }

    pub fn intern_signal(&mut self, node: &str, signal: &str) -> u32 {
        self.intern(Item::Signal(node.to_owned(), signal.to_owned()))
    }

    pub fn get(&self, item: &Item) -> Option<u32> {
        self.items.get_index_of(item).map(|i| i as u32)
    }

    pub fn resolve(&self, id: u32) -> &Item {
        self.items
            .get_index(id as usize)
            .expect("interner ids are never removed")
    }

    pub fn is_signal(&self, id: u32) -> bool {
        self.resolve(id).is_signal()
    }

    /// For a signal item, the interned id of its node part.
    /// For a node item, its own id.
    pub fn node_part(&mut self, id: u32) -> u32 {
        let node = self.resolve(id).node_id().to_owned();
        self.intern_node(&node)
    }
}
