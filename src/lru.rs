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

    pub fn insert(&mut self, key: u64, entry: &mut Entry) {
        let index = self.list.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 1;
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
}

impl Slru {
    pub fn new(maxsize: usize) -> Slru {
        let protected_cap = (maxsize as f64 * 0.8) as usize;
        Slru {
            probation: List::new(maxsize),
            protected: List::new(protected_cap),
        }
    }

    pub fn insert(&mut self, key: u64, entry: &mut Entry) {
        let index = self.probation.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 2;
    }

    pub fn access(&mut self, key: u64, entries: &mut HashMap<u64, Entry>) {
        if let Some(entry) = entries.get_mut(&key) {
            match entry.policy_list_id {
                2 => {
                    self.probation.remove(entry.policy_list_index.unwrap());
                    let index = self.protected.insert_front(key);
                    entry.policy_list_index = Some(index);
                    entry.policy_list_id = 3;
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
}
