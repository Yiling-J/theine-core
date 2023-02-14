use std::num::NonZeroUsize;

use lru::LruCache;

pub struct Lru {
    lru: LruCache<String, ()>, // id is 0
}

impl Lru {
    pub fn new(maxsize: usize) -> Lru {
        Lru {
            lru: LruCache::new(NonZeroUsize::new(maxsize).unwrap()),
        }
    }

    pub fn set(&mut self, key: &str) -> Option<String> {
        let evicted = self.lru.push(key.to_string(), ());
        if let Some(i) = evicted {
            if i.0 != key {
                return Some(i.0);
            }
        }
        None
    }

    pub fn remove(&mut self, key: &str) {
        self.lru.pop(key);
    }

    pub fn access(&mut self, key: &str) {
        self.lru.get(key);
    }

    pub fn size(&self) -> usize {
        self.lru.cap().get()
    }

    pub fn len(&self) -> usize {
        self.lru.len()
    }
}

pub struct Slru {
    protected: LruCache<String, ()>, // id is 1
    probation: LruCache<String, ()>, // id is 2
    maxsize: usize,
}

impl Slru {
    pub fn new(maxsize: usize) -> Slru {
        let protected_cap = (maxsize as f64 * 0.8) as usize;
        Slru {
            maxsize,
            protected: LruCache::new(NonZeroUsize::new(protected_cap).unwrap()),
            probation: LruCache::new(NonZeroUsize::new(maxsize).unwrap()),
        }
    }

    pub fn set(&mut self, key: &str) -> Option<String> {
        if self.protected.len() + self.probation.len() >= self.maxsize {
            let evicted = self.probation.pop_lru();
            self.probation.push(key.to_string(), ());
            if let Some(i) = evicted {
                return Some(i.0);
            }
        } else {
            let evicted = self.probation.push(key.to_string(), ());
            if let Some(i) = evicted {
                if i.0 != key {
                    return Some(i.0);
                }
            }
        }
        None
    }

    pub fn victim(&mut self) -> Option<String> {
        if self.probation.len() + self.protected.len() < self.maxsize {
            return None;
        }
        let evicted = self.probation.peek_lru();
        if let Some(i) = evicted {
            return Some(i.0.to_string());
        }
        None
    }

    pub fn access(&mut self, key: &str, id: u8) -> Option<String> {
        match id {
            1 => {
                self.probation.pop(key);
                let evicted = self.protected.push(key.to_string(), ());
                if let Some(i) = evicted {
                    // add back to probation
                    if i.0 != key {
                        self.probation.push(i.0.to_string(), ());
                        return Some(i.0);
                    }
                }
            }
            2 => {
                self.protected.get(key);
                return None;
            }
            _ => unreachable!(),
        };
        None
    }

    pub fn remove(&mut self, key: &str, id: u8) {
        match id {
            1 => self.probation.pop(key),
            2 => self.protected.pop(key),
            _ => unreachable!(),
        };
    }

    pub fn protected_size(&self) -> usize {
        self.protected.cap().get()
    }

    pub fn protected_len(&self) -> usize {
        self.protected.len()
    }

    pub fn probation_size(&self) -> usize {
        self.probation.cap().get()
    }

    pub fn probation_len(&self) -> usize {
        self.probation.len()
    }
}
