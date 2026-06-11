use std::collections::HashMap;

use indexmap::{IndexMap, IndexSet};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::{CoreError, CoreResult};
use crate::item::Interner;

/// The fields of `noob.edge.Edge` the scheduler cares about.
/// Extracted attribute-wise from `Edge` objects at the barrier.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeRec {
    pub source_node: String,
    pub source_signal: String,
    pub target_node: String,
    pub required: bool,
}

impl<'py> FromPyObject<'py> for EdgeRec {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(EdgeRec {
            source_node: ob.getattr("source_node")?.extract()?,
            source_signal: ob.getattr("source_signal")?.extract()?,
            target_node: ob.getattr("target_node")?.extract()?,
            required: ob.getattr("required")?.extract()?,
        })
    }
}

/// Extract `dict[str, NodeSpecification]` to a node id -> enabled map
pub fn extract_nodes(nodes: &Bound<'_, PyDict>) -> PyResult<IndexMap<String, bool>> {
    let mut out: IndexMap<String, bool> = IndexMap::with_capacity(nodes.len());
    for (node_id, spec) in nodes.iter() {
        out.insert(node_id.extract()?, spec.getattr("enabled")?.extract()?);
    }
    Ok(out)
}

/// Port of `noob.toposort._NodeInfo`
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeRec {
    pub nqueue: i64,
    pub successors: IndexSet<u32>,
    pub predecessors: IndexSet<u32>,
    pub optional_predecessors: IndexSet<u32>,
    pub optional_successors: IndexSet<u32>,
}

/// Port of `noob.toposort.TopoSorter` operating on interned item ids.
///
/// The semantics - including quirks like `_get_nodeinfo` creating entries on
/// read paths, and which operations do or don't touch the pass counters -
/// intentionally mirror the python implementation line by line, since the
/// scheduler's observable behavior (insertion order of `node_info`, error
/// types, ready-set contents) depends on them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sorter {
    /// node item id -> signal items emitted by that node that the graph depends on
    pub signals: HashMap<u32, IndexSet<u32>>,
    /// insertion-ordered, mirrors `TopoSorter._node2info`
    pub info: IndexMap<u32, NodeRec>,
    pub ready: IndexSet<u32>,
    pub out: IndexSet<u32>,
    pub done: IndexSet<u32>,
    pub disabled: IndexSet<u32>,
    pub ran: IndexSet<u32>,
    pub npassedout: i64,
    pub nfinished: i64,
}

impl Sorter {
    /// Port of `TopoSorter.__init__` from a node map and edge list
    pub fn from_graph(
        interner: &mut Interner,
        nodes: &IndexMap<String, bool>,
        edges: &[EdgeRec],
    ) -> CoreResult<Sorter> {
        let mut sorter = Sorter::default();
        // filter on disabled rather than enabled nodes, see python impl
        for (node_id, enabled) in nodes {
            if !enabled {
                let id = interner.intern_node(node_id);
                sorter.disabled.insert(id);
            }
        }
        for edge in edges {
            let target = interner.intern_node(&edge.target_node);
            if sorter.disabled.contains(&target) {
                continue;
            }
            let pred = interner.intern_signal(&edge.source_node, &edge.source_signal);
            sorter.add(interner, target, &[pred], edge.required)?;
        }
        // add enabled nodes that have no edges
        for (node_id, enabled) in nodes {
            if *enabled {
                let id = interner.intern_node(node_id);
                if !sorter.info.contains_key(&id) {
                    sorter.add(interner, id, &[], true)?;
                }
            }
        }
        Ok(sorter)
    }

    /// Port of `_get_nodeinfo`: get-or-create, preserving insertion order
    pub fn get_nodeinfo(&mut self, id: u32) -> &mut NodeRec {
        self.info.entry(id).or_default()
    }

    pub fn mark_ready(&mut self, nodes: &[u32]) {
        for n in nodes {
            self.ready.insert(*n);
        }
    }

    pub fn mark_out(&mut self, nodes: &[u32]) {
        for n in nodes {
            self.ready.swap_remove(n);
        }
        for n in nodes {
            self.out.insert(*n);
        }
        self.npassedout += nodes.len() as i64;
    }

    /// Port of `_expire_nodes`
    fn expire_nodes(&mut self, nodes: &[u32]) {
        let mut expired: IndexSet<u32> = IndexSet::new();
        for n in nodes {
            if !self.done.contains(n) {
                expired.insert(*n);
            }
        }
        for n in &expired {
            self.ready.swap_remove(n);
            if self.out.contains(n) {
                self.out.swap_remove(n);
            } else {
                self.npassedout += 1;
            }
        }
        for n in &expired {
            self.done.insert(*n);
        }
        self.nfinished += expired.len() as i64;
    }

    /// Port of `mark_expired`
    pub fn mark_expired(&mut self, nodes: &[u32], unlock_optionals: bool) {
        self.expire_nodes(nodes);
        if !unlock_optionals {
            return;
        }
        for node in nodes {
            // python uses _get_nodeinfo here, which creates missing entries
            let optional_successors: Vec<u32> = self
                .get_nodeinfo(*node)
                .optional_successors
                .iter()
                .copied()
                .collect();
            for successor in optional_successors {
                let rec = self.get_nodeinfo(successor);
                rec.nqueue -= 1;
                let nqueue = rec.nqueue;
                if nqueue == 0 && !self.done.contains(&successor) && !self.out.contains(&successor)
                {
                    if self.disabled.contains(&successor) {
                        self.mark_expired(&[successor], false);
                    } else {
                        self.mark_ready(&[successor]);
                    }
                }
            }
        }
    }

    /// Port of `add`
    pub fn add(
        &mut self,
        interner: &mut Interner,
        node: u32,
        predecessors: &[u32],
        required: bool,
    ) -> CoreResult<()> {
        let mut reasons: Vec<&str> = Vec::new();
        if self.out.contains(&node) {
            reasons.push("already out");
        }
        if self.done.contains(&node) {
            reasons.push("already done");
        }
        if !reasons.is_empty() {
            // python formats with str(): bare string for node ids,
            // tuple repr for signals
            let item = interner.resolve(node);
            let shown = match item {
                crate::item::Item::Node(n) => n.clone(),
                _ => format!("{item}"),
            };
            return Err(CoreError::Value(format!(
                "{} cannot be added: {}",
                shown,
                reasons.join(", ")
            )));
        }

        let mut new_predecessors: Vec<u32> = Vec::new();
        for &pred in predecessors {
            if self.get_nodeinfo(pred).successors.contains(&node) {
                continue;
            }
            new_predecessors.push(pred);
            self.get_nodeinfo(pred).successors.insert(node);

            if interner.is_signal(pred) {
                // (node, signal) predecessors must always depend on the node
                let pred_node = interner.node_part(pred);
                self.signals.entry(pred_node).or_default().insert(pred);
                self.add(interner, pred, &[pred_node], true)?;
            }

            // re-read after the recursive add, matching python which holds a
            // reference to the (possibly mutated) info object
            let nqueue = self.get_nodeinfo(pred).nqueue;
            if nqueue == 0
                && !self.out.contains(&pred)
                && !self.done.contains(&pred)
                && !self.disabled.contains(&pred)
            {
                self.mark_ready(&[pred]);
            }
        }

        {
            let rec = self.get_nodeinfo(node);
            for p in &new_predecessors {
                rec.predecessors.insert(*p);
            }
        }
        // note: python passes *all* given predecessors here, not just new ones
        self.update_optionals(interner, node, predecessors, required);

        let ndone_predecessors = new_predecessors
            .iter()
            .filter(|p| self.done.contains(*p))
            .count() as i64;
        let rec = self.get_nodeinfo(node);
        rec.nqueue += new_predecessors.len() as i64 - ndone_predecessors;
        let nqueue = rec.nqueue;
        if nqueue == 0 {
            self.mark_ready(&[node]);
        } else {
            // in case node is added multiple times
            self.ready.swap_remove(&node);
        }
        Ok(())
    }

    /// Port of `_update_optionals`.
    ///
    /// The traversals add a `processed` guard absent from python so that a
    /// malformed (cyclic) graph cannot loop forever; for DAGs the result is
    /// identical since all updates are idempotent set operations.
    fn update_optionals(
        &mut self,
        interner: &Interner,
        node: u32,
        predecessors: &[u32],
        required: bool,
    ) {
        if interner.is_signal(node) {
            return;
        }

        {
            let rec = self.get_nodeinfo(node);
            if required {
                for p in predecessors {
                    rec.optional_predecessors.swap_remove(p);
                }
            } else {
                for p in predecessors {
                    rec.optional_predecessors.insert(*p);
                }
            }
        }

        // downstream pass: find nodes hanging off the nearest optional edge
        let mut to_visit: IndexSet<u32> =
            self.get_nodeinfo(node).successors.iter().copied().collect();
        let mut seen: IndexSet<u32> = IndexSet::new();
        let mut processed: IndexSet<u32> = IndexSet::new();
        let mut new_optional_successors: Vec<u32> = Vec::new();
        while let Some(current) = to_visit.pop() {
            if !processed.insert(current) {
                continue;
            }
            let successors: Vec<u32> = self
                .get_nodeinfo(current)
                .successors
                .iter()
                .copied()
                .collect();
            for next_successor in successors {
                let next_is_optional_edge = !interner.is_signal(next_successor)
                    && self
                        .get_nodeinfo(next_successor)
                        .optional_predecessors
                        .contains(&current);
                if next_is_optional_edge {
                    // optional edge! terminate traversal of this branch,
                    // optionalness doesn't propagate
                    new_optional_successors.push(next_successor);
                } else {
                    let next_successors: Vec<u32> = self
                        .get_nodeinfo(next_successor)
                        .successors
                        .iter()
                        .copied()
                        .collect();
                    for s in next_successors {
                        if !seen.contains(&s) {
                            to_visit.insert(s);
                        }
                        seen.insert(s);
                    }
                }
            }
        }
        {
            let rec = self.get_nodeinfo(node);
            for s in new_optional_successors {
                rec.optional_successors.insert(s);
            }
        }

        // upstream, first pass - remove optionals
        let (start, optional_predecessors) = {
            let rec = self.get_nodeinfo(node);
            let start: IndexSet<u32> = rec
                .predecessors
                .difference(&rec.optional_predecessors)
                .copied()
                .collect();
            let opts: IndexSet<u32> = rec.optional_predecessors.iter().copied().collect();
            (start, opts)
        };
        let mut to_visit = start;
        let mut seen: IndexSet<u32> = IndexSet::new();
        let mut processed: IndexSet<u32> = IndexSet::new();
        while let Some(current) = to_visit.pop() {
            if !processed.insert(current) {
                continue;
            }
            let rec = self.get_nodeinfo(current);
            rec.optional_successors.swap_remove(&node);
            let required_predecessors: Vec<u32> = rec
                .predecessors
                .difference(&rec.optional_predecessors)
                .copied()
                .collect();
            let all_predecessors: Vec<u32> = rec.predecessors.iter().copied().collect();
            for p in required_predecessors {
                if !seen.contains(&p) {
                    to_visit.insert(p);
                }
            }
            for p in all_predecessors {
                seen.insert(p);
            }
        }

        // upstream, second pass - re-add optionals
        let mut to_visit = optional_predecessors;
        let mut seen: IndexSet<u32> = IndexSet::new();
        let mut processed: IndexSet<u32> = IndexSet::new();
        while let Some(current) = to_visit.pop() {
            if !processed.insert(current) {
                continue;
            }
            let current_is_signal = interner.is_signal(current);
            let rec = self.get_nodeinfo(current);
            if current_is_signal {
                rec.optional_successors.insert(node);
            }
            if rec.optional_predecessors.is_empty() {
                let predecessors: Vec<u32> = rec.predecessors.iter().copied().collect();
                for p in predecessors {
                    if !seen.contains(&p) {
                        to_visit.insert(p);
                    }
                    seen.insert(p);
                }
            }
        }
    }

    /// Port of `get_ready`
    pub fn get_ready(&mut self, filter_node: Option<u32>) -> Vec<u32> {
        let result: Vec<u32> = match filter_node {
            None => self.ready.iter().copied().collect(),
            // a NodeSignal never equals a node id string, so id equality matches
            Some(node) => self.ready.iter().copied().filter(|n| *n == node).collect(),
        };
        self.mark_out(&result);
        result
    }

    pub fn is_active(&self) -> bool {
        self.nfinished < self.npassedout || !self.ready.is_empty()
    }

    /// Port of `done`
    pub fn done(&mut self, interner: &Interner, nodes: &[u32]) -> CoreResult<()> {
        for &node in nodes {
            if !self.info.contains_key(&node) {
                return Err(CoreError::NotAdded(format!(
                    "node {} was not added using add()",
                    interner.resolve(node)
                )));
            }

            if !self.out.contains(&node) {
                if self.done.contains(&node) {
                    return Err(CoreError::AlreadyDone(format!(
                        "node {} was already marked done",
                        interner.resolve(node)
                    )));
                }
                // we do lots of forward-looking cancellation -
                // if we say it's done, it's done.
                self.mark_out(&[node]);
            }

            self.expire_nodes(&[node]);

            let successors: Vec<u32> = self.info[&node].successors.iter().copied().collect();
            for successor in successors {
                if self.done.contains(&successor) || self.out.contains(&successor) {
                    continue;
                }
                let rec = self.get_nodeinfo(successor);
                rec.nqueue -= 1;
                let nqueue = rec.nqueue;
                if nqueue == 0 {
                    if self.disabled.contains(&successor) {
                        self.mark_expired(&[successor], true);
                    } else {
                        self.mark_ready(&[successor]);
                    }
                }
            }
        }
        for &node in nodes {
            self.ran.insert(node);
        }
        Ok(())
    }

    /// Port of `resurrect`
    pub fn resurrect(&mut self, interner: &Interner, nodes: &[u32]) -> CoreResult<()> {
        for &node in nodes {
            if self.ran.contains(&node) {
                return Err(CoreError::AlreadyDone(format!(
                    "node {} was marked done, not expired! can only resurrect expired nodes.",
                    interner.resolve(node)
                )));
            }
            if !self.done.contains(&node) || self.disabled.contains(&node) {
                continue;
            }
            self.done.swap_remove(&node);
            self.nfinished -= 1;
            self.npassedout -= 1;
            let nqueue = match self.info.get(&node) {
                Some(rec) => rec.nqueue,
                // python indexes _node2info directly here and would KeyError
                None => {
                    return Err(CoreError::Key(format!("{}", interner.resolve(node))));
                }
            };
            if nqueue == 0 {
                self.mark_ready(&[node]);
            }
        }
        Ok(())
    }

    /// Port of `find_cycle`: iterative DFS over successors
    pub fn find_cycle(&self) -> Option<Vec<u32>> {
        let mut stack: Vec<u32> = Vec::new();
        let mut itstack: Vec<std::vec::IntoIter<u32>> = Vec::new();
        let mut seen: IndexSet<u32> = IndexSet::new();
        let mut node2stacki: HashMap<u32, usize> = HashMap::new();

        let starts: Vec<u32> = self.info.keys().copied().collect();
        for start in starts {
            if seen.contains(&start) {
                continue;
            }
            let mut node = start;
            loop {
                if seen.contains(&node) {
                    // if we have seen the node already and it is in the
                    // current stack we have found a cycle
                    if let Some(&i) = node2stacki.get(&node) {
                        let mut cycle: Vec<u32> = stack[i..].to_vec();
                        cycle.push(node);
                        return Some(cycle);
                    }
                } else {
                    seen.insert(node);
                    itstack.push(
                        self.info[&node]
                            .successors
                            .iter()
                            .copied()
                            .collect::<Vec<u32>>()
                            .into_iter(),
                    );
                    node2stacki.insert(node, stack.len());
                    stack.push(node);
                }

                // backtrack to the topmost stack entry with
                // at least another successor
                let mut found_next = false;
                while !stack.is_empty() {
                    if let Some(next) = itstack.last_mut().and_then(|it| it.next()) {
                        node = next;
                        found_next = true;
                        break;
                    } else {
                        let popped = stack.pop().expect("stack is non-empty");
                        node2stacki.remove(&popped);
                        itstack.pop();
                    }
                }
                if !found_next {
                    break;
                }
            }
        }
        None
    }
}
