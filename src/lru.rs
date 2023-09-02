use crate::{
    metadata::{Link, MetaData},
    policy::Policy,
};

pub struct Lru {
    pub link: Link, // id is 1
}

impl Policy for Lru {
    fn remove(&mut self, index: u32, metadata: &mut MetaData) {
        self.link.remove(index, metadata);
    }
}

impl Lru {
    pub fn new(maxsize: usize, metadata: &mut MetaData) -> Lru {
        Lru {
            link: Link::new(1, maxsize as u32, metadata),
        }
    }

    pub fn insert(&mut self, index: u32, metadata: &mut MetaData) -> Option<u32> {
        self.link.insert_front(index, metadata)
    }

    pub fn access(&mut self, index: u32, metadata: &mut MetaData) {
        self.link.touch(index, metadata)
    }

    pub fn capacity(&self) -> usize {
        self.link.capacity as usize
    }

    pub fn len(&self) -> usize {
        self.link.len as usize
    }
}

pub struct Slru {
    pub probation: Link, // id is 2
    pub protected: Link, // id is 3
    maxsize: usize,
}

impl Slru {
    pub fn new(maxsize: usize, metadata: &mut MetaData) -> Slru {
        let protected_cap = (maxsize as f64 * 0.8) as usize;
        Slru {
            maxsize,
            probation: Link::new(2, maxsize as u32, metadata),
            protected: Link::new(3, protected_cap as u32, metadata),
        }
    }

    pub fn insert(&mut self, index: u32, metadata: &mut MetaData) -> Option<u32> {
        if self.maxsize == 0 {
            return Some(index);
        }
        if self.protected.len + self.probation.len >= self.maxsize as u32 {
            if let Some(evicted) = self.probation.pop_tail(metadata) {
                self.probation.insert_front(index, metadata);
                Some(evicted)
            } else {
                self.probation.insert_front(index, metadata)
            }
        } else {
            self.probation.insert_front(index, metadata)
        }
    }

    pub fn victim(&mut self, metadata: &mut MetaData) -> Option<u32> {
        if self.maxsize == 0 {
            return None;
        }
        if self.probation.len + self.protected.len < self.maxsize as u32 {
            return None;
        }
        self.probation.tail(metadata)
    }

    pub fn access(&mut self, index: u32, metadata: &mut MetaData) {
        let entry = &mut metadata.data[index as usize];
        match entry.link_id {
            2 => {
                self.probation.remove(index, metadata);
                if let Some(evicted) = self.protected.insert_front(index, metadata) {
                    self.probation.insert_front(evicted, metadata);
                }
            }
            3 => self.protected.touch(index, metadata),
            _ => unreachable!(),
        }
    }

    pub fn remove(&mut self, index: u32, metadata: &mut MetaData) {
        let entry = &mut metadata.data[index as usize];
        match entry.link_id {
            2 => self.probation.remove(index, metadata),
            3 => self.protected.remove(index, metadata),
            _ => unreachable!(),
        };
    }

    pub fn protected_capacity(&self) -> usize {
        self.protected.capacity as usize
    }

    pub fn protected_len(&self) -> usize {
        self.protected.len as usize
    }

    pub fn probation_capacity(&self) -> usize {
        self.probation.capacity as usize
    }

    pub fn probation_len(&self) -> usize {
        self.probation.len as usize
    }
}
