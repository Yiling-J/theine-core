use crate::lru::{Lru, Slru};
use crate::sketch::CountMinSketch;
use ahash::{AHasher, RandomState};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

#[pyclass]
pub struct TinyLfu {
    lru: Lru,
    slru: Slru,
    sketch: CountMinSketch,
    key_mapping: HashMap<String, u8, BuildHasherDefault<AHasher>>,
    hasher: RandomState,
}

#[pymethods]
impl TinyLfu {
    #[new]
    fn new(size: usize) -> TinyLfu {
        let mut lru_size = (size as f64 * 0.01) as usize;
        if lru_size == 0 {
            lru_size = 1;
        }
        let slru_size = size - lru_size;
        TinyLfu {
            lru: Lru::new(lru_size),
            slru: Slru::new(slru_size),
            key_mapping: HashMap::default(),
            sketch: CountMinSketch::new(size),
            hasher: RandomState::new(),
        }
    }

    // add/update key
    fn set(&mut self, key: &str) -> Option<String> {
        let candidate = self.lru.set(key);
        if let Some(i) = candidate {
            let victim = self.slru.victim();
            if let Some(j) = victim {
                let candidate_count = self.sketch.estimate(self.hasher.hash_one(i.to_string()));
                let victim_count = self.sketch.estimate(self.hasher.hash_one(j));
                if candidate_count > victim_count {
                    return self.slru.set(&i);
                }
                return Some(i);
            }
            return self.slru.set(&i);
        }
        None
    }

    // remove key
    fn remove(&mut self, key: &str) {
        let e = self.key_mapping.remove(key);
        if let Some(i) = e {
            match i {
                0 => self.lru.remove(key),
                1 => self.slru.remove(key, 1),
                2 => self.slru.remove(key, 2),
                _ => unreachable!(),
            };
        }
    }

    // mark access, update sketch and lru/slru
    fn access(&mut self, key: &str) {
        self.sketch.add(self.hasher.hash_one(key.to_string()));
        let e = self.key_mapping.get(key);
        if let Some(i) = e {
            match i {
                0 => self.lru.access(key),
                1 => {
                    self.key_mapping.insert(key.to_string(), 2);
                    // move from protected to probation
                    let moved = self.slru.access(key, 1);
                    if let Some(i) = moved {
                        self.key_mapping.insert(i, 1);
                    };
                }
                2 => {
                    self.slru.access(key, 2);
                }
                _ => unreachable!(),
            };
        };
    }
}
