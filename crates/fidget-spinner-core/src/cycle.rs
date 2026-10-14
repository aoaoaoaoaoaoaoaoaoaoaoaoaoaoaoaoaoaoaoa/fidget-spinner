use crate::{Edge, Slug};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn closes_cycle(
    edges: &BTreeMap<(Slug, Slug), Edge>,
    from: &Slug,
    to: &Slug,
) -> Option<Vec<Slug>> {
    let mut pending = vec![(to.clone(), vec![from.clone(), to.clone()])];
    let mut visited = BTreeSet::new();
    while let Some((node, path)) = pending.pop() {
        if &node == from {
            return Some(path);
        }
        if !visited.insert(node.clone()) {
            continue;
        }
        for (source, target) in edges.keys().rev() {
            if source == &node {
                let mut next = path.clone();
                next.push(target.clone());
                pending.push((target.clone(), next));
            }
        }
    }
    None
}
