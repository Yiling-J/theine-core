use crate::metadata::{Entry, List};
use dlv_list::Index;
use std::collections::HashMap;

pub struct Lru {
    pub list: List<u64>, // id is 1
}

impl Lru {
    pub fn new(maxsize: usize) -> Lru {
        Lru {
            list: List::new(maxsize),
        }
    }

    pub fn insert(&mut self, key: u64, entry: &mut Entry) -> Option<u64> {
        let (index, evicted) = self.list.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 1;
        evicted
    }

    pub fn access(&mut self, index: Index<u64>) {
        self.list.touch(index)
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn remove(&mut self, entry: &Entry) {
        if let Some(index) = entry.policy_list_index {
            self.list.remove(index);
        }
    }
}

pub struct Slru {
    pub probation: List<u64>,
    pub protected: List<u64>,
    maxsize: usize,
}

impl Slru {
    pub fn new(maxsize: usize) -> Slru {
        let protected_cap = (maxsize as f64 * 0.8) as usize;
        Slru {
            maxsize,
            probation: List::new(maxsize),
            protected: List::new(protected_cap),
        }
    }

    pub fn insert(&mut self, key: u64, entry: &mut Entry) -> Option<u64> {
        if self.maxsize == 0 {
            return Some(key);
        }

        // probation list capacity is dynamic (max_size - protected_current_size),
        // if max_size reach, remove the tail entry from probation and insert new
        if self.protected.len() + self.probation.len() >= self.maxsize {
            if let Some(evicted) = self.probation.pop_tail() {
                let (index, _) = self.probation.insert_front(key);
                entry.policy_list_index = Some(index);
                entry.policy_list_id = 2;
                return Some(evicted);
            }
        }
        let (index, evicted) = self.probation.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 2;
        evicted
    }

    pub fn victim(&mut self) -> Option<&u64> {
        if self.maxsize == 0 {
            return None;
        }
        if self.probation.len() + self.protected.len() < self.maxsize {
            return None;
        }
        self.probation.tail()
    }

    pub fn access(&mut self, key: u64, entries: &mut HashMap<u64, Entry>) {
        if let Some(entry) = entries.get_mut(&key) {
            match entry.policy_list_id {
                2 => {
                    if self.protected.capacity > 0 {
                        self.probation.remove(entry.policy_list_index.unwrap());
                        let (index, evicted) = self.protected.insert_front(key);
                        entry.policy_list_index = Some(index);
                        entry.policy_list_id = 3;

                        if let Some(ek) = evicted {
                            if let Some(ev) = entries.get_mut(&ek) {
                                let (index, _) = self.probation.insert_front(ek);
                                ev.policy_list_index = Some(index);
                                ev.policy_list_id = 2;
                            }
                        }
                    }
                }
                3 => self.protected.touch(entry.policy_list_index.unwrap()),
                _ => unreachable!(),
            }
        }
    }

    pub fn remove(&mut self, entry: &Entry) {
        if let Some(list_index) = entry.policy_list_index {
            match entry.policy_list_id {
                2 => self.probation.remove(list_index),
                3 => self.protected.remove(list_index),
                _ => unreachable!(),
            };
        }
    }

    pub fn protected_len(&self) -> usize {
        self.protected.len()
    }

    pub fn probation_len(&self) -> usize {
        self.probation.len()
    }
}
