use crate::lru::{Lru, Slru};
use crate::sketch::CountMinSketch;
use ahash::{AHasher, RandomState};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

pub struct TinyLfu {
    lru: Lru,
    slru: Slru,
    sketch: CountMinSketch,
    key_mapping: HashMap<String, u8, BuildHasherDefault<AHasher>>,
    hasher: RandomState,
}

impl TinyLfu {
    pub fn new(size: usize) -> TinyLfu {
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
    pub fn set(&mut self, key: &str) -> Option<String> {
        self.key_mapping.insert(key.to_string(), 0);
        let candidate = self.lru.set(key);
        if let Some(i) = candidate {
            self.key_mapping.remove(&i.to_string());
            let victim = self.slru.victim();
            if let Some(j) = victim {
                let candidate_count = self.sketch.estimate(self.hasher.hash_one(i.to_string()));
                let victim_count = self.sketch.estimate(self.hasher.hash_one(j));
                // candicate is evicated
                if candidate_count <= victim_count {
                    return Some(i);
                }
            }
            // candicate is admitted, insert to slru
            self.key_mapping.insert(i.to_string(), 1);
            let e = self.slru.set(&i);
            // e is the evicated one from slru, is exists
            if let Some(j) = e {
                self.key_mapping.remove(&j);
                return Some(j);
            }
        }
        None
    }

    // remove key
    pub fn remove(&mut self, key: &str) {
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
    pub fn access(&mut self, key: &str) {
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

#[cfg(test)]
mod tests {
    use super::TinyLfu;

    #[test]
    fn test_tlfu() {
        let mut tlfu = TinyLfu::new(1000);
        assert_eq!(tlfu.lru.size(), 10);
        assert_eq!(tlfu.slru.probation_size(), 990);
        assert_eq!(tlfu.slru.protected_size(), 792);
        assert_eq!(tlfu.slru.probation_len(), 0);
        assert_eq!(tlfu.slru.protected_len(), 0);

        for i in 0..200 {
            let evicated = tlfu.set(&format!("key:{}", i));
            assert!(evicated.is_none());
        }

        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 190);
        assert_eq!(tlfu.slru.protected_len(), 0);

        // access same key will move the key from probation tp protected
        tlfu.access("key:10");
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 189);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // access again, length should be same
        tlfu.access("key:10");
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 189);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // fill rlfu
        for i in 200..1000 {
            tlfu.set(&format!("key:{}", i));
        }
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 989);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // set again, should evicate one
        let evicated = tlfu.set("key:0a");
        // lru size is 10, and last 10 is 990-1000, so evicate 990
        assert_eq!(evicated.unwrap(), "key:990");
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 989);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // test estimate
        let victim = tlfu.slru.victim();
        assert_eq!(victim.unwrap(), "key:0");
        tlfu.access("key:991");
        tlfu.access("key:991");
        tlfu.access("key:991");
        tlfu.access("key:991");
        let evicated = tlfu.set("key:1a");
        assert_eq!(evicated.unwrap(), "key:0");

        for i in 0..1000 {
            tlfu.set(&format!("key:{}:b", i));
        }
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 989);
        assert_eq!(tlfu.slru.protected_len(), 1);
    }

    #[test]
    fn test_tlfu_set_same() {
        let mut tlfu = TinyLfu::new(1000);

        for i in 0..200 {
            let evicated = tlfu.set(&format!("key:{}", i));
            assert!(evicated.is_none());
        }

        for i in 0..200 {
            let evicated = tlfu.set(&format!("key:{}", i));
            assert!(evicated.is_none());
        }
    }
}
