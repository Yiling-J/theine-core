use crate::{
    lru::Lru,
    metadata::MetaData,
    policy::Policy,
    timerwheel::{Cache, TimerWheel},
    tlfu::TinyLfu,
};
use pyo3::{prelude::*, types::PyDict};

struct PyDictCache<'a> {
    dict: &'a PyDict,
    kh: &'a PyDict,
    hk: &'a PyDict,
}

impl<'a> Cache for PyDictCache<'a> {
    fn del_item(&mut self, key: &str) {
        let _ = self.dict.del_item(key);
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

    pub fn set(&mut self, key: &str, expire: u128) -> Option<String> {
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
            return Some(self.metadata.data[evicted_index as usize].key.to_string());
        }
        None
    }

    pub fn remove(&mut self, key: &str) {
        if let Some(entry) = self.metadata.get(key) {
            self.wheel.deschedule(entry, &mut self.metadata);
            self.policy.remove(entry, &mut self.metadata);
            self.metadata.remove(entry);
        }
    }

    pub fn access(&mut self, key: &str) {
        self.policy.access(key, &mut self.metadata);
    }

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict, kh: &PyDict, hk: &PyDict) {
        let wrapper = &mut PyDictCache {
            dict: cache,
            kh,
            hk,
        };
        self.wheel
            .advance(now, wrapper, &mut self.policy, &mut self.metadata);
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

    pub fn set(&mut self, key: &str, expire: u128) -> Option<String> {
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
            let entry = &self.metadata.data[evicted_index as usize];
            return Some(entry.key.to_string());
        }
        None
    }

    pub fn remove(&mut self, key: &str) {
        if let Some(index) = self.metadata.get(key) {
            self.wheel.deschedule(index, &mut self.metadata);
            self.policy.remove(index, &mut self.metadata);
        }
    }

    pub fn access(&mut self, key: &str) {
        if let Some(index) = self.metadata.get(key) {
            self.policy.access(index, &mut self.metadata);
        }
    }

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict, kh: &PyDict, hk: &PyDict) {
        let wrapper = &mut PyDictCache {
            dict: cache,
            kh,
            hk,
        };
        self.wheel
            .advance(now, wrapper, &mut self.policy, &mut self.metadata);
    }
}
