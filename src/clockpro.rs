use std::time::SystemTime;

use crate::{
    metadata::{Link, MetaData, COLD_PAGE, HOT_PAGE, TEST_PAGE},
    policy::Policy,
};

pub struct ClockPro {
    mem_max: usize,
    mem_cold: usize,
    hand_hot: u32,
    hand_cold: u32,
    hand_test: u32,
    count_hot: usize,
    count_cold: usize,
    count_test: usize,
    link: Link,
}

impl Policy for ClockPro {
    // remove key
    fn remove(&mut self, index: u32, metadata: &mut MetaData) {
        self._meta_del(index, metadata);
    }
}

impl ClockPro {
    pub fn new(size: usize, metadata: &mut MetaData) -> Self {
        let link = Link::new(1, size as u32 * 2, metadata);
        Self {
            mem_max: size,
            mem_cold: size,
            hand_hot: link.root,
            hand_cold: link.root,
            hand_test: link.root,
            count_cold: 0,
            count_hot: 0,
            count_test: 0,
            link,
        }
    }

    pub fn access(&mut self, key: &str, metadata: &mut MetaData) -> Option<u32> {
        if let Some(index) = metadata.get(key) {
            let entry = &mut metadata.data[index as usize];
            if entry.expire != 0
                && entry.expire
                    <= SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
            {
                return None;
            }
            // set reference bit to true
            entry.clock_info = (true, entry.clock_info.1);
            // test page has no value associated
            if entry.clock_info.1 != TEST_PAGE {
                return Some(index);
            }
        }
        None
    }

    pub fn set(&mut self, index: u32, metadata: &mut MetaData) -> (Option<u32>, Option<u32>) {
        let entry = &mut metadata.data[index as usize];
        let mut test = None;
        let mut removed = None;
        if entry.link_id == 0 {
            self._meta_add(index, metadata);
            self.count_cold += 1;
        } else {
            match entry.clock_info.1 {
                TEST_PAGE => {
                    if self.mem_cold < self.mem_max {
                        self.mem_cold += 1;
                    }
                    entry.clock_info = (false, HOT_PAGE);
                    self._meta_del(index, metadata);
                    self.count_test -= 1;
                    (test, removed) = self._meta_add(index, metadata);
                    self.count_hot += 1;
                }
                COLD_PAGE | HOT_PAGE => {
                    entry.clock_info = (true, entry.clock_info.1);
                }
                _ => unreachable!(),
            }
        }
        (test, removed)
    }

    fn _hand_cold(&mut self, metadata: &mut MetaData) -> (Option<u32>, Option<u32>) {
        let entry = &mut metadata.data[self.hand_cold as usize];
        let mut test = None;
        let mut removed = None;
        if entry.clock_info.1 == COLD_PAGE {
            match entry.clock_info.0 {
                true => {
                    entry.clock_info = (false, HOT_PAGE);
                    self.count_cold -= 1;
                    self.count_hot += 1;
                }
                false => {
                    // evict test page from data array(theine Python side)
                    // but still keeping this entry in metadata
                    test = Some(entry.index);
                    entry.clock_info = (false, TEST_PAGE);
                    self.count_cold -= 1;
                    self.count_test += 1;
                    while self.mem_max < self.count_test {
                        removed = self._hand_test(metadata);
                    }
                }
            }
        }

        // get cold hand entry again because hand test may change cold hand already
        let entry = &mut metadata.data[self.hand_cold as usize];
        let mut next = entry.next;
        if next == self.link.root {
            next = metadata.data[next as usize].next;
        }
        self.hand_cold = next;
        while self.mem_max - self.mem_cold < self.count_hot {
            self._hand_hot(metadata);
        }
        (test, removed)
    }

    fn _hand_hot(&mut self, metadata: &mut MetaData) {
        if self.hand_hot == self.hand_test {
            self._hand_test(metadata);
        }
        let entry = &mut metadata.data[self.hand_hot as usize];
        let mut next = entry.next;
        if entry.clock_info.1 == HOT_PAGE {
            match entry.clock_info.0 {
                true => {
                    entry.clock_info = (false, HOT_PAGE);
                }
                false => {
                    entry.clock_info = (false, COLD_PAGE);
                    self.count_hot -= 1;
                    self.count_cold += 1;
                }
            }
        }

        if next == self.link.root {
            next = metadata.data[next as usize].next;
        }
        self.hand_hot = next;
    }

    fn _hand_test(&mut self, metadata: &mut MetaData) -> Option<u32> {
        if self.hand_test == self.hand_cold {
            self._hand_cold(metadata);
        }
        let mut removed = None;
        let entry = &mut metadata.data[self.hand_test as usize];
        let info = entry.clock_info;
        if info.1 == TEST_PAGE {
            // remove from metadata
            // data on Python side already removed because this is a test page
            removed = Some(self.hand_test);
            metadata.remove(self.hand_test);
            self._meta_del(self.hand_test, metadata);
            self.count_test -= 1;
            if self.mem_cold > 1 {
                self.mem_cold -= 1;
            }
        }

        let mut next = metadata.data[self.hand_test as usize].next;
        if next == self.link.root {
            next = metadata.data[next as usize].next;
        }
        self.hand_test = next;
        removed
    }

    fn _meta_add(&mut self, index: u32, metadata: &mut MetaData) -> (Option<u32>, Option<u32>) {
        let data = self._evict(metadata);
        self.link.insert_before(index, self.hand_hot, metadata);
        // first element
        if self.hand_hot == self.link.root {
            self.hand_cold = index;
            self.hand_hot = index;
            self.hand_test = index
        }
        if self.hand_cold == self.hand_hot {
            let mut prev = metadata.data[self.hand_cold as usize].prev;
            if prev == self.link.root {
                prev = metadata.data[prev as usize].prev;
            }
            self.hand_cold = prev;
        }

        data
    }

    fn _meta_del(&mut self, index: u32, metadata: &mut MetaData) {
        if self.hand_cold == index {
            let mut prev = metadata.data[self.hand_cold as usize].prev;
            if prev == self.link.root {
                prev = metadata.data[prev as usize].prev;
            }
            self.hand_cold = prev;
        }
        if self.hand_hot == index {
            let mut prev = metadata.data[self.hand_hot as usize].prev;
            if prev == self.link.root {
                prev = metadata.data[prev as usize].prev;
            }
            self.hand_hot = prev;
        }
        if self.hand_test == index {
            let mut prev = metadata.data[self.hand_test as usize].prev;
            if prev == self.link.root {
                prev = metadata.data[prev as usize].prev;
            }
            self.hand_test = prev;
        }
        self.link.remove(index, metadata);
    }

    fn _evict(&mut self, metadata: &mut MetaData) -> (Option<u32>, Option<u32>) {
        let mut test = None;
        let mut removed = None;
        while self.mem_max <= self.count_hot + self.count_cold {
            (test, removed) = self._hand_cold(metadata);
        }
        (test, removed)
    }

    pub fn len(&self) -> usize {
        self.count_cold + self.count_hot
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::MetaData;

    use super::ClockPro;
    use std::fs::File;
    use std::io::{prelude::*, BufReader};

    #[test]
    fn test_clock_pro() {
        let mut metadata = MetaData::new(500);
        let mut policy = ClockPro::new(200, &mut metadata);
        let testdata = File::open("src/testdata/domains.txt");
        assert!(testdata.is_ok());
        let reader = BufReader::new(testdata.ok().unwrap());
        for line in reader.lines() {
            let tmp = line.ok().unwrap();
            let mut iter = tmp.split_whitespace();
            let key = iter.next().unwrap();
            let want_hit = iter.next().unwrap() == "h";
            let index = policy.access(key, &mut metadata);
            let mut hit = false;
            if index.is_none() {
                // create entry and add to policy
                let (_, removed) = policy.set(metadata.get_or_create(key).index, &mut metadata);
                if let Some(r) = removed {
                    metadata.remove(r);
                }
            } else {
                hit = true;
            }
            assert_eq!(hit, want_hit);
        }
    }
}
