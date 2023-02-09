use crate::{
    lru::Lru,
    timerwheel::{Cache, TimerWheel},
    tlfu::TinyLfu,
};
use pyo3::{prelude::*, types::PyDict};

struct PyDictCache<'a> {
    dict: &'a PyDict,
}

impl<'a> Cache for PyDictCache<'a> {
    fn del_item(&mut self, key: &str) {
        let _ = self.dict.del_item(key);
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

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict) {
        let wrapper = &mut PyDictCache { dict: cache };
        self.wheel.advance(now, wrapper)
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

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict) {
        let wrapper = &mut PyDictCache { dict: cache };
        self.wheel.advance(now, wrapper)
    }
}
