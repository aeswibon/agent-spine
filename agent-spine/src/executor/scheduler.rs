use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::workflow::{NodeKind, WorkflowEdge};

/// Expected node latency weight in microseconds (static prior).
pub fn node_weight_us(kind: &NodeKind) -> u64 {
    match kind {
        NodeKind::Agent | NodeKind::Debate | NodeKind::Vote => 800_000,
        NodeKind::Sandbox => 200_000,
        NodeKind::Verify | NodeKind::Hydrate => 5_000,
        NodeKind::Router | NodeKind::Checkpoint => 100,
        NodeKind::ApprovalGate => 50_000,
        NodeKind::Fork | NodeKind::Join => 100,
    }
}

#[derive(Debug, Clone)]
pub struct SchedulerPlan {
    /// Critical-path priority score for each node in the workflow graph.
    pub cpp: HashMap<String, u64>,
}

impl SchedulerPlan {
    /// Compute critical-path priority (CPP) for the subgraph induced by `nodes`.
    ///
    /// CPP(n) = weight(n) + max(CPP(s) for successors s), with sinks = weight(sink).
    pub fn compute(nodes: &[(String, NodeKind)], edges: &[WorkflowEdge]) -> Self {
        let node_set: HashSet<&str> = nodes.iter().map(|(n, _)| n.as_str()).collect();

        let mut succ: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in edges {
            let from = e.from();
            let to = e.to();
            if node_set.contains(from) && node_set.contains(to) {
                succ.entry(from).or_default().push(to);
            }
        }

        // Memoized DFS (DAG expected).
        let mut memo: HashMap<&str, u64> = HashMap::new();
        fn dfs<'a>(
            n: &'a str,
            kinds: &HashMap<&'a str, &'a NodeKind>,
            succ: &HashMap<&'a str, Vec<&'a str>>,
            memo: &mut HashMap<&'a str, u64>,
            visiting: &mut HashSet<&'a str>,
        ) -> u64 {
            if let Some(v) = memo.get(n) {
                return *v;
            }
            // Cycle safety: treat cycles as zero successor benefit.
            if !visiting.insert(n) {
                let w = node_weight_us(kinds[n]);
                memo.insert(n, w);
                return w;
            }

            let w = node_weight_us(kinds[n]);
            let best_succ = succ
                .get(n)
                .map(|ss| {
                    ss.iter()
                        .map(|s| dfs(s, kinds, succ, memo, visiting))
                        .max()
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            visiting.remove(n);
            let cpp = w.saturating_add(best_succ);
            memo.insert(n, cpp);
            cpp
        }

        let kind_map: HashMap<&str, &NodeKind> =
            nodes.iter().map(|(n, k)| (n.as_str(), k)).collect();
        let mut visiting = HashSet::new();
        for (n, _) in nodes {
            let _ = dfs(n.as_str(), &kind_map, &succ, &mut memo, &mut visiting);
        }

        let cpp = memo.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Self { cpp }
    }
}

#[derive(Debug, Clone)]
pub struct Scheduled<T> {
    pub name: String,
    pub cpp: u64,
    pub payload: T,
}

impl<T> Eq for Scheduled<T> {}

impl<T> PartialEq for Scheduled<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cpp == other.cpp && self.name == other.name
    }
}

impl<T> Ord for Scheduled<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // max-heap on CPP; tie-break by name for determinism
        self.cpp
            .cmp(&other.cpp)
            .then_with(|| self.name.cmp(&other.name))
    }
}

impl<T> PartialOrd for Scheduled<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct ReadyQueue<T> {
    heap: BinaryHeap<Scheduled<T>>,
}

impl<T> ReadyQueue<T> {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, item: Scheduled<T>) {
        self.heap.push(item);
    }

    pub fn pop(&mut self) -> Option<Scheduled<T>> {
        self.heap.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::WorkflowEdge;

    #[test]
    fn cpp_prefers_longer_critical_path() {
        // A -> B (Agent) and A -> C (Verify). B should dominate A's successor max.
        let nodes = vec![
            ("A".to_string(), NodeKind::Router),
            ("B".to_string(), NodeKind::Agent),
            ("C".to_string(), NodeKind::Verify),
        ];
        let edges = vec![WorkflowEdge::new("A", "B"), WorkflowEdge::new("A", "C")];
        let plan = SchedulerPlan::compute(&nodes, &edges);
        assert!(plan.cpp["B"] > plan.cpp["C"]);
        assert!(plan.cpp["A"] > plan.cpp["B"]);
    }
}
