use crate::{metadata::Entry, timerwheel::TimerWheel, tlfu::TinyLfu};
use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;

#[pyclass]
pub struct TlfuCore {
    pub policy: TinyLfu,
    pub wheel: TimerWheel,
    pub entries: HashMap<u64, Entry>,
}

#[pymethods]
// None of the methods in this implementation are thread-safe. Please ensure that you use the appropriate mutex on the caller side.
impl TlfuCore {
    #[new]
    pub fn new(size: usize) -> Self {
        Self {
            policy: TinyLfu::new(size),
            wheel: TimerWheel::new(),
            entries: HashMap::with_capacity(size),
        }
    }

    fn set_entry(&mut self, key: u64, ttl: u64) -> Option<u64> {
        // update
        if let Some(exist) = self.entries.get_mut(&key) {
            exist.expire = self.wheel.clock.expire_ns(ttl);
            self.wheel.schedule(key, exist);
            return None;
        }

        // create
        let mut entry = Entry::new();
        entry.expire = self.wheel.clock.expire_ns(ttl);
        self.wheel.schedule(key, &mut entry);
        self.entries.insert(key, entry);

        if let Some(evicted_key) = self.policy.set(key, &mut self.entries) {
            if let Some(evicted) = self.entries.get_mut(&evicted_key) {
                self.wheel.deschedule(evicted);
                self.entries.remove(&evicted_key);
                Some(evicted_key)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn set(&mut self, entries: Vec<(u64, u64)>) -> Vec<u64> {
        let mut evicted = HashSet::new();
        for entry in entries.iter() {
            if evicted.contains(&entry.0) {
                evicted.remove(&entry.0);
            }
            let ev = self.set_entry(entry.0, entry.1);
            if let Some(key) = ev {
                evicted.insert(key);
            }
        }

        for ev in &evicted {
            self.entries.remove(ev);
        }

        Vec::from_iter(evicted)
    }

    pub fn remove(&mut self, key: u64) -> Option<u64> {
        if let Some(entry) = self.entries.get_mut(&key) {
            self.wheel.deschedule(entry);
            self.policy.remove(entry);
            self.entries.remove(&key);
            return Some(key);
        }
        None
    }

    pub fn access(&mut self, keys: Vec<u64>) {
        for key in keys {
            self.access_entry(key);
        }
    }

    fn access_entry(&mut self, key: u64) {
        self.policy
            .access(key, &self.wheel.clock, &mut self.entries);
    }

    pub fn advance(&mut self) {
        let removed = self
            .wheel
            .advance(self.wheel.clock.now_ns(), &mut self.entries);
        for key in removed.iter() {
            if let Some(entry) = self.entries.get_mut(key) {
                self.policy.remove(entry);
                self.entries.remove(key);
            }
        }
    }

    pub fn clear(&mut self) {
        self.wheel.clear();
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[pyfunction]
/// Applies a supplemental hash function to a given hash,
/// Python's hash function returns i64, which could be negative
pub fn spread(h: i64) -> u64 {
    let mut z = u64::from_ne_bytes(h.to_ne_bytes());
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
    z = z ^ (z >> 31);
    z
}

#[cfg(test)]
mod tests {
    use crate::core::TlfuCore;

    #[test]
    fn test_tlfu_core_size_small() {
        for size in [1, 2, 3] {
            let mut tlfu = TlfuCore::new(size);
            tlfu.set(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]);
            assert_eq!(size, tlfu.entries.len());
            tlfu.access(vec![1]);
            tlfu.set(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]);
            assert_eq!(size, tlfu.entries.len());
        }
    }
}
