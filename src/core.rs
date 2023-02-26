use std::time::SystemTime;

use crate::{
    lru::Lru,
    metadata::MetaData,
    policy::Policy,
    timerwheel::{Cache, TimerWheel},
    tlfu::TinyLfu,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};

struct PyCache<'a> {
    list: &'a PyList,
    kh: &'a PyDict,
    hk: &'a PyDict,
    sentinel: &'a PyAny,
}

impl<'a> Cache for PyCache<'a> {
    fn del_item(&mut self, key: &str, index: u32) {
        let _ = self.list.set_item(index as usize, self.sentinel);
        if let Some(nkey) = key.strip_prefix("_auto:") {
            let num: u64 = nkey.parse().unwrap();
            if let Some(keyh) = self.kh.get_item(num) {
                let _ = self.kh.del_item(num);
                let _ = self.hk.del_item(keyh);
            }
        }
    }
}

#[pyclass]
pub struct TlfuCore {
    pub policy: TinyLfu,
    pub wheel: TimerWheel,
    pub metadata: MetaData,
}

#[pyclass]
pub struct LruCore {
    policy: Lru,
    wheel: TimerWheel,
    metadata: MetaData,
}

#[pymethods]
impl TlfuCore {
    #[new]
    pub fn new(size: usize) -> Self {
        let mut metadata = MetaData::new(size);
        Self {
            policy: TinyLfu::new(size, &mut metadata),
            wheel: TimerWheel::new(size, &mut metadata),
            metadata,
        }
    }

    pub fn set(&mut self, key: &str, expire: u128) -> (u32, Option<u32>, Option<String>) {
        let entry = self.metadata.get_or_create(key);
        entry.expire = expire;
        let index = entry.index;
        let mut evicted_index = 0;
        self.wheel.schedule(index, &mut self.metadata);
        if let Some(evicted) = self.policy.set(index, &mut self.metadata) {
            self.wheel.deschedule(evicted, &mut self.metadata);
            self.metadata.remove(evicted);
            evicted_index = evicted;
        }
        if evicted_index > 0 {
            let evicted = &self.metadata.data[evicted_index as usize];
            return (index, Some(evicted.index), Some(evicted.key.to_string()));
        }
        (index, None, None)
    }

    pub fn remove(&mut self, key: &str) -> Option<u32> {
        if let Some(entry) = self.metadata.get(key) {
            self.wheel.deschedule(entry, &mut self.metadata);
            self.policy.remove(entry, &mut self.metadata);
            self.metadata.remove(entry);
            return Some(entry);
        }
        None
    }

    pub fn access(&mut self, key: &str) -> Option<u32> {
        self.policy.access(key, &mut self.metadata)
    }

    pub fn advance(
        &mut self,
        _py: Python,
        now: u128,
        cache: &PyList,
        sentinel: &PyAny,
        kh: &PyDict,
        hk: &PyDict,
    ) {
        let wrapper = &mut PyCache {
            list: cache,
            kh,
            hk,
            sentinel,
        };
        self.wheel
            .advance(now, wrapper, &mut self.policy, &mut self.metadata);
    }

    pub fn clear(&mut self) {
        self.wheel.clear(&mut self.metadata);
        self.metadata.clear();
    }
}

#[pymethods]
impl LruCore {
    #[new]
    fn new(size: usize) -> Self {
        let mut metadata = MetaData::new(size);
        Self {
            policy: Lru::new(size, &mut metadata),
            wheel: TimerWheel::new(size, &mut metadata),
            metadata,
        }
    }

    pub fn set(&mut self, key: &str, expire: u128) -> (u32, Option<u32>, Option<String>) {
        let entry = self.metadata.get_or_create(key);
        entry.expire = expire;
        let index = entry.index;
        let mut evicted_index = 0;
        self.wheel.schedule(index, &mut self.metadata);
        if let Some(evicted) = self.policy.insert(index, &mut self.metadata) {
            self.wheel.deschedule(evicted, &mut self.metadata);
            self.metadata.remove(evicted);
            evicted_index = evicted;
        }
        if evicted_index > 0 {
            let evicted = &self.metadata.data[evicted_index as usize];
            return (index, Some(evicted.index), Some(evicted.key.to_string()));
        }
        (index, None, None)
    }

    pub fn remove(&mut self, key: &str) -> Option<u32> {
        if let Some(index) = self.metadata.get(key) {
            self.wheel.deschedule(index, &mut self.metadata);
            self.policy.remove(index, &mut self.metadata);
            return Some(index);
        }
        None
    }

    pub fn access(&mut self, key: &str) -> Option<u32> {
        if let Some(index) = self.metadata.get(key) {
            let entry = &self.metadata.data[index as usize];
            if entry.expire != 0
                && entry.expire
                    <= SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
            {
                return None;
            }
            self.policy.access(index, &mut self.metadata);
            return Some(index);
        }
        None
    }

    pub fn advance(
        &mut self,
        _py: Python,
        now: u128,
        cache: &PyList,
        sentinel: &PyAny,
        kh: &PyDict,
        hk: &PyDict,
    ) {
        let wrapper = &mut PyCache {
            list: cache,
            kh,
            hk,
            sentinel,
        };
        self.wheel
            .advance(now, wrapper, &mut self.policy, &mut self.metadata);
    }

    pub fn clear(&mut self) {
        self.wheel.clear(&mut self.metadata);
        self.metadata.clear();
    }
}
