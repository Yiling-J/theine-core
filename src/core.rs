use crate::{
    lru::Lru,
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
    policy: TinyLfu,
    wheel: TimerWheel,
}

#[pyclass]
pub struct LruCore {
    policy: Lru,
    wheel: TimerWheel,
}

#[pymethods]
impl TlfuCore {
    #[new]
    fn new(size: usize) -> Self {
        Self {
            policy: TinyLfu::new(size),
            wheel: TimerWheel::new(),
        }
    }

    pub fn schedule(&mut self, key: &str, expire: u128) {
        self.wheel.schedule(key, expire);
    }

    pub fn deschedule(&mut self, key: &str) {
        self.wheel.deschedule(key);
    }

    pub fn set_policy(&mut self, key: &str) -> Option<String> {
        if let Some(evicted) = self.policy.set(key) {
            self.wheel.deschedule(&evicted);
            return Some(evicted);
        }
        None
    }

    pub fn set(&mut self, key: &str, expire: u128) -> Option<String> {
        self.wheel.schedule(key, expire);
        if let Some(evicted) = self.policy.set(key) {
            self.wheel.deschedule(&evicted);
            return Some(evicted);
        }
        None
    }

    pub fn remove(&mut self, key: &str) {
        self.wheel.deschedule(key);
        self.policy.remove(key)
    }

    pub fn access(&mut self, key: &str) {
        self.policy.access(key);
    }

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict, kh: &PyDict, hk: &PyDict) {
        let wrapper = &mut PyDictCache {
            dict: cache,
            kh,
            hk,
        };
        self.wheel.advance(now, wrapper, &mut self.policy)
    }
}

#[pymethods]
impl LruCore {
    #[new]
    fn new(size: usize) -> Self {
        Self {
            policy: Lru::new(size),
            wheel: TimerWheel::new(),
        }
    }

    pub fn schedule(&mut self, key: &str, expire: u128) {
        self.wheel.schedule(key, expire);
    }

    pub fn deschedule(&mut self, key: &str) {
        self.wheel.deschedule(key);
    }

    pub fn set_policy(&mut self, key: &str) -> Option<String> {
        self.policy.set(key)
    }

    pub fn set(&mut self, key: &str, expire: u128) -> Option<String> {
        self.wheel.schedule(key, expire);
        self.policy.set(key)
    }

    pub fn remove(&mut self, key: &str) {
        self.wheel.deschedule(key);
        self.policy.remove(key)
    }

    pub fn access(&mut self, key: &str) {
        self.policy.access(key);
    }

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict, kh: &PyDict, hk: &PyDict) {
        let wrapper = &mut PyDictCache {
            dict: cache,
            kh,
            hk,
        };
        self.wheel.advance(now, wrapper, &mut self.policy)
    }
}
