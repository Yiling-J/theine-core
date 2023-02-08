use crate::{
    lru::Lru,
    timerwheel::{Cache, TimerWheel},
    tlfu::TinyLfu,
};
use pyo3::{exceptions::PyException, prelude::*, types::PyDict};

struct PyDictCache<'a> {
    dict: &'a PyDict,
}

impl<'a> Cache for PyDictCache<'a> {
    fn del_item(&mut self, key: &str) -> Result<(), String> {
        match self.dict.del_item(key) {
            Ok(_v) => Ok(()),
            Err(e) => Err(e.to_string()),
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

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict) -> PyResult<()> {
        let wrapper = &mut PyDictCache { dict: cache };
        match self.wheel.advance(now, wrapper) {
            Ok(v) => Ok(v),
            Err(e) => Err(PyException::new_err(e)),
        }
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

    pub fn advance(&mut self, _py: Python, now: u128, cache: &PyDict) -> PyResult<()> {
        let wrapper = &mut PyDictCache { dict: cache };
        match self.wheel.advance(now, wrapper) {
            Ok(v) => Ok(v),
            Err(e) => Err(PyException::new_err(e)),
        }
    }
}
