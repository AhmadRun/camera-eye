//! Port of `server/providers/cctv/cap.js`: the shared catalog cap.

use std::collections::HashMap;

use super::constants::{CCTV_MAX_SOURCES_CEILING, DEFAULT_CCTV_MAX_SOURCES};
use super::normalize::CctvSource;
use crate::common::js;

/// `resolveCatalogCap`: CCTV_MAX_SOURCES bounded to [8, ceiling].
pub fn resolve_catalog_cap(raw: Option<&str>) -> usize {
    let parsed = match raw.filter(|r| !r.is_empty()) {
        Some(text) => js::number(text),
        None => DEFAULT_CCTV_MAX_SOURCES,
    };
    if !parsed.is_finite() {
        return DEFAULT_CCTV_MAX_SOURCES as usize;
    }
    parsed.floor().clamp(8.0, CCTV_MAX_SOURCES_CEILING) as usize
}

/// One pack's share of the cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackAllocation {
    pub name: String,
    pub offered: usize,
    pub kept: usize,
}

/// A pack's normalized sources, in merge order.
#[derive(Debug, Clone)]
pub struct SourcePack {
    pub name: String,
    pub sources: Vec<CctvSource>,
}

/// `allocateSourceCap`: merge packs under one cap, round-robin when they
/// exceed it; duplicate ids resolve last-pack-wins.
pub fn allocate_source_cap(
    packs: &[SourcePack],
    max_count: usize,
) -> (Vec<CctvSource>, Vec<PackAllocation>) {
    let mut owner: HashMap<&str, (usize, usize)> = HashMap::new();
    for (p, pack) in packs.iter().enumerate() {
        for (i, source) in pack.sources.iter().enumerate() {
            if source.id.is_empty() {
                continue;
            }
            owner.insert(source.id.as_str(), (p, i));
        }
    }
    let queues: Vec<Vec<&CctvSource>> = packs
        .iter()
        .enumerate()
        .map(|(p, pack)| {
            pack.sources
                .iter()
                .enumerate()
                .filter(|(i, s)| !s.id.is_empty() && owner.get(s.id.as_str()) == Some(&(p, *i)))
                .map(|(_, s)| s)
                .collect()
        })
        .collect();
    let total: usize = queues.iter().map(Vec::len).sum();
    let limit = total.min(max_count);
    let mut next = vec![0usize; queues.len()];
    let mut sources: Vec<CctvSource> = Vec::new();
    if limit >= total {
        for (q, queue) in queues.iter().enumerate() {
            sources.extend(queue.iter().map(|s| (*s).clone()));
            next[q] = queue.len();
        }
    } else {
        while sources.len() < limit {
            for (q, queue) in queues.iter().enumerate() {
                if sources.len() >= limit {
                    break;
                }
                if next[q] >= queue.len() {
                    continue;
                }
                sources.push(queue[next[q]].clone());
                next[q] += 1;
            }
        }
    }
    let allocations = packs
        .iter()
        .enumerate()
        .map(|(q, pack)| PackAllocation {
            name: pack.name.clone(),
            offered: queues[q].len(),
            kept: next[q],
        })
        .collect();
    (sources, allocations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cctv::normalize::normalize_source_item;
    use serde_json::json;

    fn pack(name: &str, count: usize) -> SourcePack {
        SourcePack {
            name: name.into(),
            sources: (0..count)
                .map(|i| normalize_source_item(&json!({ "id": format!("{name}-{i}") })))
                .collect(),
        }
    }

    fn ids(sources: &[CctvSource]) -> Vec<&str> {
        sources.iter().map(|s| s.id.as_str()).collect()
    }

    #[test]
    fn under_the_cap_every_pack_is_served_whole() {
        let (sources, packs) = allocate_source_cap(&[pack("a", 3), pack("b", 2)], 10);
        assert_eq!(ids(&sources), ["a-0", "a-1", "a-2", "b-0", "b-1"]);
        assert_eq!(
            packs,
            vec![
                PackAllocation {
                    name: "a".into(),
                    offered: 3,
                    kept: 3
                },
                PackAllocation {
                    name: "b".into(),
                    offered: 2,
                    kept: 2
                },
            ]
        );
    }

    #[test]
    fn over_the_cap_packs_share_round_robin() {
        let (sources, packs) =
            allocate_source_cap(&[pack("a", 100), pack("b", 100), pack("c", 100)], 9);
        assert_eq!(sources.len(), 9);
        assert_eq!(packs.iter().map(|p| p.kept).collect::<Vec<_>>(), [3, 3, 3]);
        let c: Vec<&str> = ids(&sources)
            .into_iter()
            .filter(|id| id.starts_with("c-"))
            .collect();
        assert_eq!(c, ["c-0", "c-1", "c-2"]);
    }

    #[test]
    fn duplicates_take_one_slot_and_last_pack_wins() {
        let file = SourcePack {
            name: "file".into(),
            sources: vec![
                normalize_source_item(&json!({ "id": "x", "name": "old" })),
                normalize_source_item(&json!({ "id": "x", "name": "new" })),
                normalize_source_item(&json!({ "id": "y" })),
            ],
        };
        let (sources, _) = allocate_source_cap(&[file], 2);
        assert_eq!(
            sources
                .iter()
                .map(|s| (s.id.as_str(), s.name.as_str()))
                .collect::<Vec<_>>(),
            [("x", "new"), ("y", "y")]
        );
        let live = SourcePack {
            name: "live".into(),
            sources: vec![
                normalize_source_item(&json!({ "id": "cam-1", "name": "live" })),
                normalize_source_item(&json!({ "id": "cam-2" })),
            ],
        };
        let env = SourcePack {
            name: "env".into(),
            sources: vec![normalize_source_item(
                &json!({ "id": "cam-1", "name": "override" }),
            )],
        };
        let (sources, packs) = allocate_source_cap(&[live, env], 10);
        assert_eq!(
            sources
                .iter()
                .map(|s| (s.id.as_str(), s.name.as_str()))
                .collect::<Vec<_>>(),
            [("cam-2", "cam-2"), ("cam-1", "override")]
        );
        assert_eq!(
            packs
                .iter()
                .map(|p| (p.offered, p.kept))
                .collect::<Vec<_>>(),
            [(1, 1), (1, 1)]
        );
    }

    #[test]
    fn a_short_pack_releases_its_turns() {
        let (_, packs) = allocate_source_cap(&[pack("a", 2), pack("b", 50)], 10);
        assert_eq!(packs.iter().map(|p| p.kept).collect::<Vec<_>>(), [2, 8]);
        let (sources, packs) = allocate_source_cap(
            &[pack("austin", 250), pack("nsw", 217), pack("calgary", 215)],
            30,
        );
        assert_eq!(sources.len(), 30);
        assert_eq!(
            packs.iter().map(|p| p.kept).collect::<Vec<_>>(),
            [10, 10, 10]
        );
    }

    #[test]
    fn resolve_catalog_cap_bounds_the_env_value() {
        assert_eq!(resolve_catalog_cap(None), 4000);
        assert_eq!(resolve_catalog_cap(Some("nonsense")), 4000);
        assert_eq!(resolve_catalog_cap(Some("1")), 8);
        assert_eq!(resolve_catalog_cap(Some("999999")), 5000);
        assert_eq!(resolve_catalog_cap(Some("120.9")), 120);
    }
}
