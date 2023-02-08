use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyAny;
use std::time::SystemTime;

use crate::{lru::Lru, policy::Policy, tlfu::TinyLfu};

struct Cached {
    data: &PyAny,
    expire: f32,
}

pub struct Cache {
    policy: Box<dyn Policy + 'static>,
    cache: PyDict<String, Cached>,
    ttl: f32,
    wait_expire: f32,
}

impl Cache {
    fn new(policy: &str, size: usize, ttl: f32) -> Self {
        match policy {
            "tlfu" => Self {
                policy: Box::new(TinyLfu::new(size)),
                cache: IndexMap::new(),
                ttl,
                wait_expire: -1f32,
            },
            "lru" => Self {
                policy: Box::new(Lru::new(size)),
                cache: IndexMap::new(),
                ttl,
                wait_expire: -1f32,
            },
        }
    }

    fn get(&mut self, key: &str, default: &PyAny) -> PyResult<&PyAny> {
        self.policy.access(key);
        let cached = self.cache.get(key);
        match cached {
            Some(i) => match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => {
                    if i.expire < n.as_secs_f32() {
                        return Ok(default);
                    }
                    return Ok(&i.data);
                }
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            },
            None => Ok(default),
        }
    }

    fn set(&mut self, key: &str, value: &PyAny) {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => {
                let now = n.as_secs_f32();
                let expire = now + self.ttl;
                let exist = self.cache.contains_key(key);
                self.cache.insert(
                    key,
                    Cached {
                        data: value,
                        expire: expire,
                    },
                );
                if self.wait_expire == -1f32 {
                    self.wait_expire = now + self.ttl + 0.01
                }
                if now > self.wait_expire {
                    self.expire()
                }
            }
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        }
    }
    fn delete(&mut self, key: &str) -> bool {}
    fn expire(&mut self) {}
}
