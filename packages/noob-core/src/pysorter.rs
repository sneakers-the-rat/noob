use std::sync::{Arc, Mutex, MutexGuard};

use indexmap::IndexMap;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PySet, PyTuple};

use crate::bridge::Bridge;
use crate::item::{Interner, Item};
use crate::sorter::{extract_nodes, EdgeRec, NodeFlags, Sorter};

/// Shared handle to a [`Sorter`] and the [`Interner`] that resolves its item
/// ids. Locks are uncontended in practice (everything runs under the GIL);
/// they exist so the pyclass is `Send + Sync` and so a scheduler and the
/// sorter handles it gives out can share state soundly.
pub type SharedSorter = Arc<Mutex<Sorter>>;
pub type SharedInterner = Arc<Mutex<Interner>>;

/// Rust counterpart of `noob.toposort.TopoSorter`.
///
/// A standalone topological sorter over graph items - it knows nothing about
/// the scheduler. `CoreScheduler` *contains* one of these per epoch and hands
/// out shared handles via its `sorter()` / `getitem()` methods, so mutating a
/// handle mutates the epoch it came from.
#[pyclass(module = "noob_core")]
pub struct CoreTopoSorter {
    pub(crate) sorter: SharedSorter,
    pub(crate) interner: SharedInterner,
}

impl CoreTopoSorter {
    /// Wrap scheduler-owned state in a sorter handle
    pub fn from_shared(sorter: SharedSorter, interner: SharedInterner) -> Self {
        CoreTopoSorter { sorter, interner }
    }

    fn lock(&self) -> (MutexGuard<'_, Sorter>, MutexGuard<'_, Interner>) {
        (
            self.sorter.lock().expect("sorter lock"),
            self.interner.lock().expect("interner lock"),
        )
    }

    fn item_set<'py>(
        py: Python<'py>,
        interner: &Interner,
        ids: impl IntoIterator<Item = u32>,
    ) -> PyResult<Bound<'py, PySet>> {
        let items = ids
            .into_iter()
            .map(|id| interner.resolve(id).clone().into_pyobject(py))
            .collect::<PyResult<Vec<_>>>()?;
        PySet::new(py, &items)
    }
}

#[pymethods]
impl CoreTopoSorter {
    /// Accepts the same arguments as the python TopoSorter:
    /// `dict[str, NodeSpecification]` and `list[Edge]`
    #[new]
    #[pyo3(signature = (nodes=None, edges=None))]
    fn new(nodes: Option<Bound<'_, PyDict>>, edges: Option<Vec<EdgeRec>>) -> PyResult<Self> {
        let nodes: IndexMap<String, NodeFlags> = match &nodes {
            Some(nodes) => extract_nodes(nodes)?,
            None => IndexMap::new(),
        };
        let edges = edges.unwrap_or_default();
        let mut interner = Interner::default();
        let sorter = Sorter::from_graph(&mut interner, &nodes, &edges)?;
        Ok(CoreTopoSorter {
            sorter: Arc::new(Mutex::new(sorter)),
            interner: Arc::new(Mutex::new(interner)),
        })
    }

    #[pyo3(signature = (node, predecessors, required=true))]
    fn add(&self, node: Item, predecessors: Vec<Item>, required: bool) -> PyResult<()> {
        let (mut sorter, mut interner) = self.lock();
        let node_id = interner.intern(node);
        let pred_ids: Vec<u32> = predecessors
            .into_iter()
            .map(|i| interner.intern(i))
            .collect();
        Ok(sorter.add(&mut interner, node_id, &pred_ids, required)?)
    }

    #[pyo3(signature = (node_id=None))]
    fn get_ready<'py>(
        &self,
        py: Python<'py>,
        node_id: Option<String>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let (mut sorter, mut interner) = self.lock();
        let filter_id = node_id.map(|n| interner.intern_node(&n));
        let ready = sorter.get_ready(filter_id);
        PyTuple::new(py, ready.iter().map(|id| interner.resolve(*id).clone()))
    }

    fn is_active(&self) -> bool {
        self.sorter.lock().expect("sorter lock").is_active()
    }

    fn done(&self, nodes: Vec<Item>) -> PyResult<()> {
        let (mut sorter, mut interner) = self.lock();
        let ids: Vec<u32> = nodes.into_iter().map(|i| interner.intern(i)).collect();
        Ok(sorter.done(&interner, &ids)?)
    }

    fn mark_ready(&self, nodes: Vec<Item>) {
        let (mut sorter, mut interner) = self.lock();
        let ids: Vec<u32> = nodes.into_iter().map(|i| interner.intern(i)).collect();
        sorter.mark_ready(&ids);
    }

    fn mark_out(&self, nodes: Vec<Item>) {
        let (mut sorter, mut interner) = self.lock();
        let ids: Vec<u32> = nodes.into_iter().map(|i| interner.intern(i)).collect();
        sorter.mark_out(&ids);
    }

    #[pyo3(signature = (nodes, unlock_optionals=true, cascade=false))]
    fn mark_expired<'py>(
        &self,
        py: Python<'py>,
        nodes: Vec<Item>,
        unlock_optionals: bool,
        cascade: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let (mut sorter, mut interner) = self.lock();
        let ids: Vec<u32> = nodes.into_iter().map(|i| interner.intern(i)).collect();
        let expired = sorter.mark_expired(&ids, unlock_optionals, cascade);
        PyTuple::new(py, expired.iter().map(|id| interner.resolve(*id).clone()))
    }

    fn resurrect(&self, nodes: Vec<Item>) -> PyResult<()> {
        let (mut sorter, mut interner) = self.lock();
        let ids: Vec<u32> = nodes.into_iter().map(|i| interner.intern(i)).collect();
        Ok(sorter.resurrect(&interner, &ids)?)
    }

    fn find_cycle(&self) -> Option<Vec<Item>> {
        let (sorter, interner) = self.lock();
        sorter.find_cycle().map(|cycle| {
            cycle
                .into_iter()
                .map(|id| interner.resolve(id).clone())
                .collect()
        })
    }

    fn ready_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let (sorter, interner) = self.lock();
        Self::item_set(py, &interner, sorter.ready.iter().copied())
    }

    fn out_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let (sorter, interner) = self.lock();
        Self::item_set(py, &interner, sorter.out.iter().copied())
    }

    fn done_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let (sorter, interner) = self.lock();
        Self::item_set(py, &interner, sorter.done.iter().copied())
    }

    fn ran_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let (sorter, interner) = self.lock();
        Self::item_set(py, &interner, sorter.ran.iter().copied())
    }

    fn disabled_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let (sorter, interner) = self.lock();
        Self::item_set(py, &interner, sorter.disabled.iter().copied())
    }

    /// (npassedout, nfinished)
    fn counters(&self) -> (i64, i64) {
        let sorter = self.sorter.lock().expect("sorter lock");
        (sorter.npassedout, sorter.nfinished)
    }

    /// Insertion-ordered `dict[GraphItem, _NodeInfo]`,
    /// the same shape as `TopoSorter._node2info`
    fn node_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let bridge = Bridge::get(py)?;
        let info_cls = bridge.node_info.bind(py);
        let (sorter, interner) = self.lock();
        let out = PyDict::new(py);
        for (id, rec) in &sorter.info {
            let item = interner.resolve(*id).clone().into_pyobject(py)?;
            let info = info_cls.call1((&item,))?;
            info.setattr("nqueue", rec.nqueue)?;
            info.setattr(
                "successors",
                Self::item_set(py, &interner, rec.successors.iter().copied())?,
            )?;
            info.setattr(
                "predecessors",
                Self::item_set(py, &interner, rec.predecessors.iter().copied())?,
            )?;
            info.setattr(
                "optional_predecessors",
                Self::item_set(py, &interner, rec.optional_predecessors.iter().copied())?,
            )?;
            info.setattr(
                "optional_successors",
                Self::item_set(py, &interner, rec.optional_successors.iter().copied())?,
            )?;
            out.set_item(item, info)?;
        }
        Ok(out)
    }

    /// `dict[NodeID, set[NodeSignal]]`, the same shape as `TopoSorter.signals`
    fn signals<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let (sorter, interner) = self.lock();
        let out = PyDict::new(py);
        for (node, sigs) in &sorter.signals {
            out.set_item(
                interner.resolve(*node).node_id(),
                Self::item_set(py, &interner, sigs.iter().copied())?,
            )?;
        }
        Ok(out)
    }

    /// Whether two handles share the same underlying sorter state
    fn shares_state_with(&self, other: &CoreTopoSorter) -> bool {
        Arc::ptr_eq(&self.sorter, &other.sorter)
    }

    fn __eq__(&self, other: &CoreTopoSorter) -> bool {
        if Arc::ptr_eq(&self.sorter, &other.sorter) {
            return true;
        }
        // state comparison is only meaningful against the same interner
        Arc::ptr_eq(&self.interner, &other.interner)
            && *self.sorter.lock().expect("sorter lock")
                == *other.sorter.lock().expect("sorter lock")
    }

    /// An independent copy of this sorter's state.
    /// The interner is shared: it is append-only, so ids stay consistent.
    fn __deepcopy__(&self, _memo: Bound<'_, PyAny>) -> Self {
        let sorter = self.sorter.lock().expect("sorter lock").clone();
        CoreTopoSorter {
            sorter: Arc::new(Mutex::new(sorter)),
            interner: self.interner.clone(),
        }
    }

    fn __copy__(&self) -> Self {
        CoreTopoSorter {
            sorter: self.sorter.clone(),
            interner: self.interner.clone(),
        }
    }
}
