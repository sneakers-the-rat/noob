use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use indexmap::{IndexMap, IndexSet};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySet, PyTuple};

use crate::bridge::{epoch_ended_event, epoch_to_py, node_ready_event, Bridge};
use crate::epoch::{fmt_epoch, get_ready_order, parent, parents, root_int, Ep, EpochArg, EpochKey};
use crate::errors::{CoreError, CoreResult};
use crate::item::{Interner, Item};
use crate::pysorter::{CoreTopoSorter, SharedInterner, SharedSorter};
use crate::sorter::{extract_nodes, EdgeRec, Sorter};

/// Virtual nodes that don't actually exist as nodes but can be depended on
const VIRTUAL_NODES: [&str; 2] = ["input", "assets"];

const EPOCH_LOG_MAXLEN: usize = 100;

const SIGNAL_READY_WARNING: &str = "Scheduler attempted to return signal tuple %s in %s - \
something is wrong with how the graph is instantiated or run, \
or a node is emitting incorrect events manually, \
all signals should be marked done/expired by events passed in `update`. \
Ignoring - nodes downstream of this signal will not run.";

/// Port of `noob.tube.downstream_nodes`: BFS over edges from `node_id`,
/// including `node_id` itself
fn downstream_nodes(edges: &[EdgeRec], node_id: &str, exclude: Option<&str>) -> IndexSet<String> {
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in edges {
        adjacency
            .entry(edge.source_node.as_str())
            .or_default()
            .push(edge.target_node.as_str());
    }
    let mut downstream: IndexSet<String> = IndexSet::new();
    downstream.insert(node_id.to_owned());
    let mut queue: VecDeque<&str> = VecDeque::new();
    queue.push_back(node_id);
    while let Some(current) = queue.pop_front() {
        if let Some(successors) = adjacency.get(current) {
            for successor in successors {
                if !downstream.contains(*successor) && Some(*successor) != exclude {
                    downstream.insert((*successor).to_owned());
                    queue.push_back(successor);
                }
            }
        }
    }
    downstream
}

/// Rust implementation of `noob.scheduler.Scheduler`.
///
/// The scheduler is a *container of topo sorters*: it owns one [`Sorter`] per
/// epoch (plus the frozen templates they are cloned from) and a shared
/// [`Interner`]. [`CoreScheduler::sorter`] / `getitem` hand out
/// [`CoreTopoSorter`] handles that share state with the epoch they came from -
/// the sorters themselves know nothing about the scheduler.
///
/// All conversion happens at the boundary via [`crate::bridge`]: methods
/// accept and return real `noob` objects (`Epoch`, `NodeSignal`, `MetaEvent`
/// dicts) and raise `noob.exceptions` types, so the python wrapper
/// (`noob.rust_scheduler.RustScheduler`) is pure delegation.
#[pyclass(module = "noob_core")]
pub struct CoreScheduler {
    interner: SharedInterner,
    /// node id -> enabled
    nodes: IndexMap<String, bool>,
    edges: Vec<EdgeRec>,
    source_nodes: Vec<String>,
    /// (node id, signal) pairs depended on in the graph
    graph_signals: HashSet<(String, String)>,
    clock: i64,
    epochs: IndexMap<EpochKey, SharedSorter>,
    subepochs: HashMap<EpochKey, IndexSet<EpochKey>>,
    epoch_log: VecDeque<i64>,
    /// node id -> (subgraph nodes, subgraph edges), mirrors `_subgraphs`
    subgraphs: HashMap<String, (IndexMap<String, bool>, Vec<EdgeRec>)>,
    /// epoch node-id path -> template sorter, mirrors `_frozen_sorters`
    frozen: HashMap<Vec<String>, Sorter>,
    /// python logger, used on the debug/warning paths the python
    /// scheduler logs on
    logger: Option<Py<PyAny>>,
}

impl Clone for CoreScheduler {
    /// Deep clone: copies the interner and every epoch's sorter state
    fn clone(&self) -> Self {
        CoreScheduler {
            interner: Arc::new(Mutex::new(self.lock_interner().clone())),
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            source_nodes: self.source_nodes.clone(),
            graph_signals: self.graph_signals.clone(),
            clock: self.clock,
            epochs: self
                .epochs
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        Arc::new(Mutex::new(v.lock().expect("sorter lock").clone())),
                    )
                })
                .collect(),
            subepochs: self.subepochs.clone(),
            epoch_log: self.epoch_log.clone(),
            subgraphs: self.subgraphs.clone(),
            frozen: self.frozen.clone(),
            logger: Python::with_gil(|py| self.logger.as_ref().map(|l| l.clone_ref(py))),
        }
    }
}

impl CoreScheduler {
    fn lock_interner(&self) -> MutexGuard<'_, Interner> {
        self.interner.lock().expect("interner lock")
    }

    /// Arc handle to an epoch's sorter, or KeyError
    fn epoch_sorter(&self, epoch: &EpochKey) -> CoreResult<SharedSorter> {
        self.epochs
            .get(epoch)
            .cloned()
            .ok_or_else(|| CoreError::Key(fmt_epoch(epoch)))
    }

    fn debug_log(&self, message: String) {
        if let Some(logger) = &self.logger {
            Python::with_gil(|py| {
                let _ = logger.bind(py).call_method1("debug", (message,));
            });
        }
    }

    /// Port of `_init_graph`: build-or-fetch the frozen template for the
    /// epoch's node-id path and return a copy of it
    fn init_graph(&mut self, epoch: Option<&EpochKey>) -> CoreResult<Sorter> {
        let frozen_key: Vec<String> = match epoch {
            None => vec!["tube".to_owned()],
            Some(ep) => ep.iter().map(|(n, _)| n.clone()).collect(),
        };
        if !self.frozen.contains_key(&frozen_key) {
            let sorter = match epoch {
                Some(ep) if ep.len() > 1 => {
                    let last_node = ep[ep.len() - 1].0.clone();
                    self.ensure_subgraph(&last_node);
                    let (sub_nodes, sub_edges) = self.subgraphs[&last_node].clone();
                    Sorter::from_graph(&mut self.lock_interner(), &sub_nodes, &sub_edges)?
                }
                _ => Sorter::from_graph(&mut self.lock_interner(), &self.nodes, &self.edges)?,
            };
            self.frozen.insert(frozen_key.clone(), sorter);
        }
        Ok(self.frozen[&frozen_key].clone())
    }

    /// Port of `_subgraph`: cache the subgraph downstream of a node
    fn ensure_subgraph(&mut self, node_id: &str) {
        if self.subgraphs.contains_key(node_id) {
            return;
        }
        let downstream = downstream_nodes(&self.edges, node_id, None);
        let sub_nodes: IndexMap<String, bool> = downstream
            .iter()
            .filter_map(|id| self.nodes.get(id).map(|enabled| (id.clone(), *enabled)))
            .collect();
        let sub_edges: Vec<EdgeRec> = self
            .edges
            .iter()
            .filter(|e| downstream.contains(&e.target_node))
            .cloned()
            .collect();
        self.subgraphs
            .insert(node_id.to_owned(), (sub_nodes, sub_edges));
    }

    /// Port of `__getitem__`'s auto-creation: make sure the epoch exists
    fn ensure_epoch(&mut self, epoch: &EpochKey) -> CoreResult<()> {
        if !self.epochs.contains_key(epoch) {
            if epoch.len() == 1 {
                self.add_epoch_impl(Some(epoch.clone()))?;
            } else {
                self.add_subepoch_impl(epoch)?;
            }
        }
        Ok(())
    }

    fn add_epoch_impl(&mut self, epoch: Option<EpochKey>) -> CoreResult<EpochKey> {
        let this_epoch = match epoch {
            Some(ep) => {
                // ensure that the next tick of the clock returns the next
                // number if we create epochs out of order
                let max = std::iter::once(root_int(&ep))
                    .chain(self.epochs.keys().map(root_int))
                    .chain(self.epoch_log.iter().copied())
                    .max()
                    .expect("iterator is non-empty");
                self.clock = max + 1;
                ep
            }
            None => {
                let ep = vec![("tube".to_owned(), self.clock)];
                self.clock += 1;
                ep
            }
        };

        if self.epochs.contains_key(&this_epoch) {
            return Err(CoreError::EpochExists(format!(
                "Epoch {} is already scheduled",
                fmt_epoch(&this_epoch)
            )));
        }
        // only root epochs can match the (integer) epoch log
        if this_epoch.len() == 1 && self.epoch_log.contains(&root_int(&this_epoch)) {
            return Err(CoreError::EpochCompleted(format!(
                "Epoch {} has already been completed!",
                fmt_epoch(&this_epoch)
            )));
        }

        let graph = self.init_graph(Some(&this_epoch))?;
        self.epochs
            .insert(this_epoch.clone(), Arc::new(Mutex::new(graph)));
        Ok(this_epoch)
    }

    fn add_subepoch_impl(&mut self, epoch: &EpochKey) -> CoreResult<()> {
        let Some(parent_key) = parent(epoch) else {
            return Err(CoreError::Value(format!(
                "Cannot create a subepoch for root epoch {}",
                fmt_epoch(epoch)
            )));
        };
        self.ensure_epoch(&parent_key)?;
        let mut sorter = self.init_graph(Some(epoch))?;

        // mark any nodes completed in the parent as completed in the subepoch,
        // EXCEPT the node that induced the subepoch or its signals
        let inducing_node = self.lock_interner().intern_node(&epoch[epoch.len() - 1].0);
        let mut exclude_current: IndexSet<u32> = sorter
            .signals
            .get(&inducing_node)
            .cloned()
            .unwrap_or_default();
        exclude_current.insert(inducing_node);

        let parent_deps: Vec<u32> = sorter.info.keys().copied().collect();
        let parent_arc = self.epoch_sorter(&parent_key)?;
        {
            let parent_sorter = parent_arc.lock().expect("sorter lock");
            let interner = self.lock_interner();
            for parent_dep in parent_deps {
                if parent_sorter.ran.contains(&parent_dep) {
                    sorter.done(&interner, &[parent_dep])?;
                } else if parent_sorter.done.contains(&parent_dep)
                    && !exclude_current.contains(&parent_dep)
                {
                    sorter.mark_expired(&[parent_dep], false);
                } else if parent_sorter.out.contains(&parent_dep) {
                    sorter.mark_out(&[parent_dep]);
                }
            }
        }

        self.epochs
            .insert(epoch.clone(), Arc::new(Mutex::new(sorter)));
        for parent_epoch in parents(epoch) {
            self.subepochs
                .entry(parent_epoch)
                .or_default()
                .insert(epoch.clone());
        }

        // a node inducing subepochs expires the node in the immediate parent
        let parent_done = parent_arc
            .lock()
            .expect("sorter lock")
            .done
            .contains(&inducing_node);
        if !parent_done {
            let node_id = epoch[epoch.len() - 1].0.clone();
            self.expire_impl(&parent_key, &node_id, None, false, false)?;
        }
        Ok(())
    }

    fn is_active_impl(&self, epoch: Option<&EpochKey>) -> CoreResult<bool> {
        match epoch {
            Some(ep) => {
                if !self.epochs.contains_key(ep) {
                    // completed-and-cleared or not-started epochs are inactive
                    return Ok(false);
                }
                let mut keys: IndexSet<&EpochKey> = IndexSet::new();
                if let Some(subs) = self.subepochs.get(ep) {
                    keys.extend(subs.iter());
                }
                keys.insert(ep);
                for key in keys {
                    let sorter = self
                        .epochs
                        .get(key)
                        .ok_or_else(|| CoreError::Key(fmt_epoch(key)))?;
                    if sorter.lock().expect("sorter lock").is_active() {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            None => Ok(self
                .epochs
                .values()
                .any(|s| s.lock().expect("sorter lock").is_active())),
        }
    }

    /// Mark a node done; if that completes the epoch, end it and return it
    fn done_impl(
        &mut self,
        epoch: &EpochKey,
        node_id: &str,
        signal: Option<&str>,
        with_signals: bool,
    ) -> CoreResult<Option<EpochKey>> {
        if self.epoch_log.contains(&root_int(epoch)) {
            self.debug_log(format!(
                "Marking node {} as done in epoch {}, but epoch was already completed. ignoring",
                node_id,
                fmt_epoch(epoch)
            ));
            return Ok(None);
        }

        let to_mark = {
            let mut interner = self.lock_interner();
            match signal {
                Some(sig) => interner.intern_signal(node_id, sig),
                None => interner.intern_node(node_id),
            }
        };

        self.ensure_epoch(epoch)?;
        let result = {
            let sorter_arc = self.epoch_sorter(epoch)?;
            let mut sorter = sorter_arc.lock().expect("sorter lock");
            let interner = self.lock_interner();
            sorter.done(&interner, &[to_mark])
        };
        match result {
            Ok(()) => {}
            Err(CoreError::AlreadyDone(_)) => {
                let has_subepochs = self
                    .subepochs
                    .get(epoch)
                    .map(|s| !s.is_empty())
                    .unwrap_or(false);
                if !has_subepochs {
                    return Err(CoreError::AlreadyDone(format!(
                        "Node {} already done in {}",
                        node_id,
                        fmt_epoch(epoch)
                    )));
                }
            }
            Err(e) => return Err(e),
        }

        self.done_subepochs(epoch, node_id, signal)?;
        for parent_key in parents(epoch) {
            self.ensure_epoch(&parent_key)?;
            let sorter_arc = self.epoch_sorter(&parent_key)?;
            sorter_arc
                .lock()
                .expect("sorter lock")
                .mark_expired(&[to_mark], false);
        }

        if signal.is_none() && with_signals {
            self.ensure_epoch(epoch)?;
            let sorter_arc = self.epoch_sorter(epoch)?;
            let mut sorter = sorter_arc.lock().expect("sorter lock");
            let interner = self.lock_interner();
            let node_item = interner.get(&Item::Node(node_id.to_owned()));
            let remaining: Vec<u32> = match node_item {
                Some(nid) => sorter
                    .signals
                    .get(&nid)
                    .map(|sigs| {
                        sigs.iter()
                            .copied()
                            .filter(|s| !sorter.done.contains(s))
                            .collect()
                    })
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            if !remaining.is_empty() {
                sorter.done(&interner, &remaining)?;
            }
        }

        if !self.is_active_impl(Some(epoch))? {
            return self.end_epoch_impl(Some(epoch.clone()));
        }
        Ok(None)
    }

    /// Mark a node expired; if that completes the epoch, end it and return it
    fn expire_impl(
        &mut self,
        epoch: &EpochKey,
        node_id: &str,
        signal: Option<&str>,
        with_signals: bool,
        unlock_optionals: bool,
    ) -> CoreResult<Option<EpochKey>> {
        let to_mark = {
            let mut interner = self.lock_interner();
            match signal {
                Some(sig) => interner.intern_signal(node_id, sig),
                None => interner.intern_node(node_id),
            }
        };

        self.ensure_epoch(epoch)?;
        {
            let sorter_arc = self.epoch_sorter(epoch)?;
            let mut sorter = sorter_arc.lock().expect("sorter lock");
            sorter.mark_expired(&[to_mark], unlock_optionals);
            // if any immediate successors are already marked "ready",
            // we also want to cancel them
            if let Some(rec) = sorter.info.get(&to_mark) {
                let successors: Vec<u32> = rec.successors.iter().copied().collect();
                for successor in successors {
                    sorter.ready.swap_remove(&successor);
                }
            }
        }

        if signal.is_none() && with_signals {
            let signal_items: Vec<u32> = {
                let sorter_arc = self.epoch_sorter(epoch)?;
                let sorter = sorter_arc.lock().expect("sorter lock");
                let interner = self.lock_interner();
                match interner.get(&Item::Node(node_id.to_owned())) {
                    Some(nid) => sorter
                        .signals
                        .get(&nid)
                        .map(|sigs| sigs.iter().copied().collect())
                        .unwrap_or_default(),
                    None => Vec::new(),
                }
            };
            for sig_item in signal_items {
                let signal_name = match self.lock_interner().resolve(sig_item) {
                    Item::Signal(_, sig) => sig.clone(),
                    Item::Node(_) => continue,
                };
                self.expire_impl(epoch, node_id, Some(&signal_name), true, unlock_optionals)?;
            }
        }

        if !self.is_active_impl(Some(epoch))? {
            return self.end_epoch_impl(Some(epoch.clone()));
        }
        Ok(None)
    }

    fn end_epoch_impl(&mut self, epoch: Option<EpochKey>) -> CoreResult<Option<EpochKey>> {
        let ep = match epoch {
            None => match self.epochs.keys().last() {
                None => return Ok(None),
                Some(last) => last.clone(),
            },
            Some(ep) => ep,
        };
        self.debug_log(format!("Ending epoch {}", fmt_epoch(&ep)));
        if ep.len() == 1 {
            self.epoch_log.push_back(root_int(&ep));
            while self.epoch_log.len() > EPOCH_LOG_MAXLEN {
                self.epoch_log.pop_front();
            }
            let mut to_remove: IndexSet<EpochKey> = IndexSet::new();
            to_remove.insert(ep.clone());
            if let Some(subs) = self.subepochs.get(&ep) {
                to_remove.extend(subs.iter().cloned());
            }
            for key in to_remove {
                // shift_remove keeps insertion order for the remaining epochs
                self.epochs.shift_remove(&key);
            }
        }
        Ok(Some(ep))
    }

    /// Port of `_done_subepochs`
    fn done_subepochs(
        &mut self,
        epoch: &EpochKey,
        node_id: &str,
        signal: Option<&str>,
    ) -> CoreResult<()> {
        let subs: Vec<EpochKey> = match self.subepochs.get(epoch) {
            Some(subs) if !subs.is_empty() => subs.iter().cloned().collect(),
            _ => return Ok(()),
        };

        self.ensure_subgraph(node_id);
        let our_subgraph: IndexSet<String> = self.subgraphs[node_id].0.keys().cloned().collect();

        let to_mark = {
            let mut interner = self.lock_interner();
            match signal {
                Some(sig) => interner.intern_signal(node_id, sig),
                None => interner.intern_node(node_id),
            }
        };

        let mut exclusive_memo: HashMap<String, IndexSet<String>> = HashMap::new();
        for subepoch in subs {
            let sorter_arc = self.epoch_sorter(&subepoch)?;
            let mut sorter = sorter_arc.lock().expect("sorter lock");
            if sorter.ran.contains(&to_mark) || !sorter.info.contains_key(&to_mark) {
                continue;
            } else if sorter.done.contains(&to_mark) {
                // needs to be resurrected
                sorter.resurrect(&self.lock_interner(), &[to_mark])?;
            }
            sorter.done(&self.lock_interner(), &[to_mark])?;

            // mark all nodes that are exclusively downstream of this node expired
            let subep_node = subepoch[subepoch.len() - 1].0.clone();
            if !exclusive_memo.contains_key(&subep_node) {
                exclusive_memo.insert(
                    subep_node.clone(),
                    downstream_nodes(&self.edges, &subep_node, Some(node_id)),
                );
            }
            let downstream_of_subep = &exclusive_memo[&subep_node];
            let exclusive: Vec<String> = our_subgraph
                .iter()
                .filter(|n| !downstream_of_subep.contains(*n) && n.as_str() != node_id)
                .cloned()
                .collect();
            for node in exclusive {
                let item = self.lock_interner().intern_node(&node);
                sorter.mark_expired(&[item], true);
            }
        }
        Ok(())
    }

    /// Port of `__getitem__(-1)`: resolve which epoch "latest" refers to
    fn latest_epoch_key(&self) -> CoreResult<EpochKey> {
        if self.epochs.len() == 1 {
            return Ok(self.epochs.keys().next().expect("len is 1").clone());
        }
        let max = self
            .epochs
            .keys()
            .map(root_int)
            .max()
            .ok_or_else(|| CoreError::Value("max() arg is an empty sequence".to_owned()))?;
        let key: EpochKey = vec![("tube".to_owned(), max)];
        if !self.epochs.contains_key(&key) {
            return Err(CoreError::Key(fmt_epoch(&key)));
        }
        Ok(key)
    }

    fn sorter_handle(&self, epoch: &EpochKey) -> CoreResult<CoreTopoSorter> {
        Ok(CoreTopoSorter::from_shared(
            self.epoch_sorter(epoch)?,
            self.interner.clone(),
        ))
    }

    /// Run a fresh full-graph sorter to exhaustion, port of `generations`
    fn generations_native(&mut self) -> CoreResult<Vec<Vec<Item>>> {
        let mut sorter = self.init_graph(None)?;
        let mut generations: Vec<Vec<Item>> = Vec::new();
        while sorter.is_active() {
            let ready = sorter.get_ready(None);
            if ready.is_empty() {
                // a cycle would loop forever; the python version relies on
                // the no-cycle invariant, we bail out instead
                break;
            }
            let interner = self.lock_interner();
            generations.push(
                ready
                    .iter()
                    .map(|id| interner.resolve(*id).clone())
                    .collect(),
            );
            sorter.done(&interner, &ready)?;
        }
        Ok(generations)
    }
}

#[pymethods]
impl CoreScheduler {
    /// Accepts the same arguments as the python Scheduler:
    /// `dict[str, NodeSpecification]`, `list[Edge]`, optional source node ids,
    /// and the `noob.scheduler` logger
    #[new]
    #[pyo3(signature = (nodes, edges, source_nodes=None, logger=None))]
    fn new(
        nodes: Bound<'_, PyDict>,
        edges: Vec<EdgeRec>,
        source_nodes: Option<Vec<String>>,
        logger: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let nodes = extract_nodes(&nodes)?;
        let graph_signals: HashSet<(String, String)> = edges
            .iter()
            .map(|e| (e.source_node.clone(), e.source_signal.clone()))
            .collect();
        let mut scheduler = CoreScheduler {
            interner: Arc::new(Mutex::new(Interner::default())),
            nodes,
            edges,
            source_nodes: source_nodes.unwrap_or_default(),
            graph_signals,
            clock: 0,
            epochs: IndexMap::new(),
            subepochs: HashMap::new(),
            epoch_log: VecDeque::new(),
            subgraphs: HashMap::new(),
            frozen: HashMap::new(),
            logger: logger.map(|l| l.unbind()),
        };

        // port of `_get_sources`
        if scheduler.source_nodes.is_empty() {
            let graph = scheduler.init_graph(None)?;
            let interner = scheduler.lock_interner();
            let mut sources: Vec<String> = Vec::new();
            for id in &graph.ready {
                match interner.resolve(*id) {
                    Item::Node(n) if !VIRTUAL_NODES.contains(&n.as_str()) => {
                        sources.push(n.clone());
                    }
                    _ => {}
                }
            }
            drop(interner);
            scheduler.source_nodes = sources;
        }
        Ok(scheduler)
    }

    fn source_nodes(&self) -> Vec<String> {
        self.source_nodes.clone()
    }

    /// The set of (node id, signal) tuples depended on in the graph
    fn graph_signals(&self) -> HashSet<(String, String)> {
        self.graph_signals.clone()
    }

    /// Port of `Scheduler.__getitem__`: `-1` resolves to the latest epoch,
    /// other epochs are auto-created if missing
    fn getitem(&mut self, epoch: EpochArg) -> PyResult<CoreTopoSorter> {
        let key = if epoch.is_latest() {
            self.latest_epoch_key()?
        } else {
            let key = epoch.into_key();
            self.ensure_epoch(&key)?;
            key
        };
        Ok(self.sorter_handle(&key)?)
    }

    /// Shared handle to an existing epoch's sorter (KeyError if missing):
    /// mutations through the handle mutate the epoch
    fn sorter(&self, epoch: EpochKey) -> PyResult<CoreTopoSorter> {
        Ok(self.sorter_handle(&epoch)?)
    }

    #[pyo3(signature = (epoch=None))]
    fn add_epoch(&mut self, epoch: Option<EpochArg>) -> PyResult<Ep> {
        Ok(Ep(self.add_epoch_impl(epoch.map(EpochArg::into_key))?))
    }

    fn add_subepoch(&mut self, epoch: EpochKey) -> PyResult<Ep> {
        self.add_subepoch_impl(&epoch)?;
        Ok(Ep(epoch))
    }

    fn contains_epoch(&self, epoch: EpochKey) -> bool {
        self.epochs.contains_key(&epoch)
    }

    /// Keys of `_epochs` as Epochs, in insertion order
    fn epoch_keys(&self) -> Vec<Ep> {
        self.epochs.keys().cloned().map(Ep).collect()
    }

    fn epoch_log(&self) -> Vec<i64> {
        self.epoch_log.iter().copied().collect()
    }

    /// `dict[Epoch, set[Epoch]]`, the same shape as `Scheduler.subepochs`
    fn subepochs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        for (parent_key, subs) in &self.subepochs {
            let subs = subs
                .iter()
                .map(|s| epoch_to_py(py, s))
                .collect::<PyResult<Vec<_>>>()?;
            out.set_item(epoch_to_py(py, parent_key)?, PySet::new(py, &subs)?)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (epoch=None))]
    fn is_active(&self, epoch: Option<EpochKey>) -> PyResult<bool> {
        Ok(self.is_active_impl(epoch.as_ref())?)
    }

    /// `NodeReady` MetaEvents for everything ready across epochs
    #[pyo3(signature = (epoch=None, node_id=None))]
    fn get_ready<'py>(
        &mut self,
        py: Python<'py>,
        epoch: Option<EpochKey>,
        node_id: Option<String>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut keys: Vec<EpochKey> = match epoch {
            Some(ref ep) => {
                let mut set: IndexSet<EpochKey> = self
                    .subepochs
                    .get(ep)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                set.insert(ep.clone());
                set.into_iter()
                    .filter(|e| self.epochs.contains_key(e))
                    .collect()
            }
            None => self.epochs.keys().cloned().collect(),
        };
        keys.sort_by(get_ready_order);

        let filter_id = node_id.map(|n| self.lock_interner().intern_node(&n));

        let ready_events = PyList::empty(py);
        for key in keys {
            let sorter_arc = self.epoch_sorter(&key)?;
            let mut sorter = sorter_arc.lock().expect("sorter lock");
            let interner = self.lock_interner();
            for id in sorter.get_ready(filter_id) {
                match interner.resolve(id).clone() {
                    signal @ Item::Signal(..) => {
                        // signals should never be yielded as ready -
                        // expire and warn
                        sorter.mark_expired(&[id], true);
                        if let Some(logger) = &self.logger {
                            logger.bind(py).call_method1(
                                "warning",
                                (
                                    SIGNAL_READY_WARNING,
                                    signal.into_pyobject(py)?,
                                    epoch_to_py(py, &key)?,
                                ),
                            )?;
                        }
                    }
                    Item::Node(n) => {
                        let enabled = VIRTUAL_NODES.contains(&n.as_str())
                            || self.nodes.get(&n).copied().unwrap_or(true);
                        if enabled {
                            ready_events.append(node_ready_event(py, &key, &n)?)?;
                        }
                    }
                }
            }
        }
        Ok(ready_events)
    }

    #[pyo3(signature = (node, epoch=None))]
    fn node_is_ready(&mut self, node: String, epoch: Option<EpochKey>) -> PyResult<bool> {
        let node_id = self.lock_interner().intern_node(&node);
        match epoch {
            None => Ok(self
                .epochs
                .values()
                .any(|s| s.lock().expect("sorter lock").ready.contains(&node_id))),
            Some(ep) => {
                // if we've already run this epoch, the node is ready
                if ep.len() == 1 && self.epoch_log.contains(&root_int(&ep)) {
                    return Ok(true);
                }
                let mut keys: Vec<EpochKey> = vec![ep.clone()];
                if let Some(subs) = self.subepochs.get(&ep) {
                    keys.extend(subs.iter().cloned());
                }
                for key in keys {
                    // python uses self[ep], which auto-creates epochs
                    self.ensure_epoch(&key)?;
                    let sorter_arc = self.epoch_sorter(&key)?;
                    if sorter_arc
                        .lock()
                        .expect("sorter lock")
                        .ready
                        .contains(&node_id)
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    fn node_is_done(&mut self, node: String, epoch: EpochKey) -> PyResult<bool> {
        if epoch.len() == 1 && self.epoch_log.contains(&root_int(&epoch)) {
            return Ok(true);
        }
        let node_id = self.lock_interner().intern_node(&node);
        let subs: Vec<EpochKey> = self
            .subepochs
            .get(&epoch)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        if !subs.is_empty() {
            for key in subs.into_iter().chain(std::iter::once(epoch)) {
                let sorter_arc = self.epoch_sorter(&key)?;
                if !sorter_arc
                    .lock()
                    .expect("sorter lock")
                    .done
                    .contains(&node_id)
                {
                    return Ok(false);
                }
            }
            Ok(true)
        } else {
            let sorter_arc = self.epoch_sorter(&epoch)?;
            let contains = sorter_arc
                .lock()
                .expect("sorter lock")
                .done
                .contains(&node_id);
            Ok(contains)
        }
    }

    #[pyo3(signature = (epoch=None))]
    fn sources_finished(&mut self, epoch: Option<EpochKey>) -> PyResult<bool> {
        let key = match epoch {
            None => {
                if self.epochs.is_empty() {
                    return Ok(true);
                }
                self.latest_epoch_key()?
            }
            Some(ep) => ep,
        };
        let sorter_arc = self.epoch_sorter(&key)?;
        let sorter = sorter_arc.lock().expect("sorter lock");
        let interner = self.lock_interner();
        for source in &self.source_nodes {
            match interner.get(&Item::Node(source.clone())) {
                Some(id) if sorter.done.contains(&id) => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Port of `Scheduler.update`: takes the event dicts themselves and
    /// returns `[*sorted_events, *epoch_ended_events]`
    fn update<'py>(
        &mut self,
        py: Python<'py>,
        events: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        if events.is_empty() {
            return PyList::new(py, events);
        }
        let noevent = Bridge::get(py)?.noevent.bind(py);

        struct Rec<'py> {
            epoch: EpochKey,
            node_id: String,
            signal: String,
            is_noevent: bool,
            event: Bound<'py, PyAny>,
        }
        let mut recs: Vec<Rec<'py>> = Vec::with_capacity(events.len());
        for event in events {
            recs.push(Rec {
                epoch: event.get_item("epoch")?.extract()?,
                node_id: event.get_item("node_id")?.extract()?,
                signal: event.get_item("signal")?.extract()?,
                is_noevent: event.get_item("value")?.is(noevent),
                event,
            });
        }
        // process subepochs first so they're created when we handle
        // parent epochs (stable sort, like python's `sorted`)
        recs.sort_by_key(|r| std::cmp::Reverse(r.epoch.len()));

        let mut end_epochs: Vec<EpochKey> = Vec::new();
        let mut nodes_done: HashSet<(EpochKey, String)> = HashSet::new();
        for rec in &recs {
            if rec.node_id == "meta" {
                continue;
            }
            let node_done = (rec.epoch.clone(), rec.node_id.clone());
            if !nodes_done.contains(&node_done) {
                nodes_done.insert(node_done);
                // suppress AlreadyDone/NotAdded: the zmq runner has an
                // incomplete graph - see FIXME in the python implementation
                match self.done_impl(&rec.epoch, &rec.node_id, None, false) {
                    Ok(Some(ended)) => {
                        end_epochs.push(ended);
                        continue;
                    }
                    Ok(None) => {}
                    Err(CoreError::AlreadyDone(_)) | Err(CoreError::NotAdded(_)) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            if !self
                .graph_signals
                .contains(&(rec.node_id.clone(), rec.signal.clone()))
            {
                continue;
            }

            let ended = if rec.is_noevent {
                self.expire_impl(&rec.epoch, &rec.node_id, Some(&rec.signal), true, true)?
            } else {
                self.done_impl(&rec.epoch, &rec.node_id, Some(&rec.signal), true)?
            };
            if let Some(ended) = ended {
                end_epochs.push(ended);
            }
        }

        let result = PyList::new(py, recs.iter().map(|r| r.event.clone()))?;
        for ended in &end_epochs {
            result.append(epoch_ended_event(py, ended)?)?;
        }
        Ok(result)
    }

    /// Mark a node done; returns an `EpochEnded` MetaEvent if that
    /// completed the epoch
    #[pyo3(signature = (epoch, node_id, signal=None, with_signals=true))]
    fn done<'py>(
        &mut self,
        py: Python<'py>,
        epoch: EpochKey,
        node_id: String,
        signal: Option<String>,
        with_signals: bool,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        match self.done_impl(&epoch, &node_id, signal.as_deref(), with_signals)? {
            Some(ended) => Ok(Some(epoch_ended_event(py, &ended)?)),
            None => Ok(None),
        }
    }

    /// Mark a node expired; returns an `EpochEnded` MetaEvent if that
    /// completed the epoch
    #[pyo3(signature = (epoch, node_id, signal=None, with_signals=true, unlock_optionals=true))]
    fn expire<'py>(
        &mut self,
        py: Python<'py>,
        epoch: EpochKey,
        node_id: String,
        signal: Option<String>,
        with_signals: bool,
        unlock_optionals: bool,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let ended = self.expire_impl(
            &epoch,
            &node_id,
            signal.as_deref(),
            with_signals,
            unlock_optionals,
        )?;
        match ended {
            Some(ended) => Ok(Some(epoch_ended_event(py, &ended)?)),
            None => Ok(None),
        }
    }

    fn epoch_completed(&self, epoch: EpochKey) -> PyResult<bool> {
        let in_log = epoch.len() == 1 && self.epoch_log.contains(&root_int(&epoch));
        // `epoch < min(log)` via Epoch.__lt__ reduces to root_int <= min(log)
        let lt_min_log = self
            .epoch_log
            .iter()
            .min()
            .map(|min| root_int(&epoch) <= *min)
            .unwrap_or(false);
        let previously_completed = !self.epoch_log.is_empty()
            && !self.epochs.contains_key(&epoch)
            && (in_log || lt_min_log);

        let active_completed = if self.epochs.contains_key(&epoch) {
            let mut keys: Vec<EpochKey> = vec![epoch.clone()];
            if let Some(subs) = self.subepochs.get(&epoch) {
                keys.extend(subs.iter().cloned());
            }
            let mut any_active = false;
            for key in keys {
                let sorter_arc = self.epoch_sorter(&key)?;
                if sorter_arc.lock().expect("sorter lock").is_active() {
                    any_active = true;
                    break;
                }
            }
            !any_active
        } else {
            false
        };
        Ok(previously_completed || active_completed)
    }

    /// End an epoch (the latest for `None` / `-1`),
    /// returning the `EpochEnded` MetaEvent
    #[pyo3(signature = (epoch=None))]
    fn end_epoch<'py>(
        &mut self,
        py: Python<'py>,
        epoch: Option<EpochArg>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let resolved: Option<EpochKey> = match epoch {
            None => None,
            Some(arg) if arg.is_latest() => None,
            Some(arg) => Some(arg.into_key()),
        };
        match self.end_epoch_impl(resolved)? {
            Some(ended) => Ok(Some(epoch_ended_event(py, &ended)?)),
            None => Ok(None),
        }
    }

    fn enable_node(&mut self, node_id: String) {
        self.nodes.insert(node_id, true);
        self.frozen.clear();
        self.subgraphs.clear();
    }

    fn disable_node(&mut self, node_id: String) {
        self.nodes.insert(node_id.clone(), false);
        self.frozen.clear();
        self.subgraphs.clear();
        let item = self.lock_interner().intern_node(&node_id);
        for sorter in self.epochs.values() {
            sorter
                .lock()
                .expect("sorter lock")
                .mark_expired(&[item], true);
        }
    }

    fn clear(&mut self) {
        self.epochs.clear();
        self.epoch_log.clear();
    }

    fn has_cycle(&mut self) -> PyResult<bool> {
        let graph = self.init_graph(None)?;
        Ok(graph.find_cycle().is_some())
    }

    /// `list[tuple[GraphItem, ...]]` of topological generations
    fn generations<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let generations = self.generations_native()?;
        let out = PyList::empty(py);
        for generation in generations {
            out.append(PyTuple::new(py, generation)?)?;
        }
        Ok(out)
    }

    /// Port of `asset_generations`:
    /// `dict[asset id, list[tuple[node id, ...]]]`
    fn asset_generations<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let asset_ids: IndexSet<String> = self
            .edges
            .iter()
            .filter(|e| e.source_node == "assets")
            .map(|e| e.source_signal.clone())
            .collect();
        let generations = self.generations_native()?;

        let out = PyDict::new(py);
        for asset in &asset_ids {
            let mut asset_generations: Vec<Bound<PyTuple>> = Vec::new();
            for generation in &generations {
                let gen_deps: Vec<&str> = generation
                    .iter()
                    .filter_map(|item| match item {
                        Item::Node(node) => self
                            .edges
                            .iter()
                            .any(|e| {
                                e.source_node == "assets"
                                    && &e.source_signal == asset
                                    && &e.target_node == node
                            })
                            .then_some(node.as_str()),
                        Item::Signal(..) => None,
                    })
                    .collect();
                if !gen_deps.is_empty() {
                    asset_generations.push(PyTuple::new(py, gen_deps)?);
                }
            }
            if !asset_generations.is_empty() {
                out.set_item(asset, asset_generations)?;
            }
        }
        Ok(out)
    }

    /// All the nodes that have an effect on the given node
    fn upstream_nodes(&mut self, node: String) -> PyResult<HashSet<String>> {
        let mut upstream: HashSet<String> = self
            .edges
            .iter()
            .filter(|e| e.target_node == node)
            .map(|e| e.source_node.clone())
            .collect();
        let sorter = self.init_graph(None)?;
        let mut interner = self.lock_interner();
        let node_id = interner.intern_node(&node);
        for (item_id, rec) in &sorter.info {
            if rec.optional_successors.contains(&node_id) {
                upstream.insert(interner.resolve(*item_id).node_id().to_owned());
            }
        }
        Ok(upstream)
    }

    fn __deepcopy__(&self, _memo: Bound<'_, PyAny>) -> Self {
        self.clone()
    }

    fn __copy__(&self) -> Self {
        self.clone()
    }
}
