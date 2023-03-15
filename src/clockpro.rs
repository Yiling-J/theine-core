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
    pub count_hot: usize,
    pub count_cold: usize,
    pub count_test: usize,
    link: Link,
}

impl Policy for ClockPro {
    // remove key
    fn remove(&mut self, index: u32, metadata: &mut MetaData) {
        match metadata.data[index as usize].clock_info.1 {
            COLD_PAGE => self.count_cold -= 1,
            HOT_PAGE => self.count_hot -= 1,
            TEST_PAGE => self.count_test -= 1,
            _ => unreachable!(),
        }
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
            (test, removed) = self._meta_add(index, metadata);
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

    fn _reorganize_cold(&mut self, metadata: &mut MetaData) {
        let entry = &mut metadata.data[self.hand_cold as usize];
        if entry.clock_info.1 == COLD_PAGE {
            return;
        }
        let mut next = entry.next;
        if next == self.link.root {
            next = metadata.data[next as usize].next;
        }
        self.hand_cold = next;
    }

    fn _reorganize_hot(&mut self, metadata: &mut MetaData) {
        let entry = &mut metadata.data[self.hand_hot as usize];
        if entry.clock_info.1 == HOT_PAGE {
            return;
        }
        let mut next = entry.next;
        if next == self.link.root {
            next = metadata.data[next as usize].next;
        }
        self.hand_hot = next;
    }

    fn _reorganize_test(&mut self, metadata: &mut MetaData) {
        let entry = &mut metadata.data[self.hand_test as usize];
        if entry.clock_info.1 == TEST_PAGE {
            return;
        }
        let mut next = entry.next;
        if next == self.link.root {
            next = metadata.data[next as usize].next;
        }
        self.hand_test = next;
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
            self._reorganize_test(metadata);
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
            self._reorganize_cold(metadata);
        }
        let mut removed = None;
        let entry = &mut metadata.data[self.hand_test as usize];
        let info = entry.clock_info;
        if info.1 == TEST_PAGE {
            // remove from metadata
            // data on Python side already removed because this is a test page
            removed = Some(self.hand_test);
            // metadata.remove(self.hand_test);
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
        // keep order
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
    use crate::metadata::{MetaData, COLD_PAGE, HOT_PAGE, TEST_PAGE};

    use super::ClockPro;

    fn key_to_index(key: &str, metadata: &mut MetaData) -> u32 {
        metadata.get_or_create(key).index
    }

    fn assert_pages(keys: Vec<i32>, page: u8, metadata: &mut MetaData) {
        for i in keys.iter() {
            println!("assert{}-{}", i, page);
            let index = metadata.get(&format!("key:{}", i));
            assert_eq!(metadata.data[index.unwrap() as usize].clock_info.1, page,);
        }
    }

    #[test]
    fn test_clock_pro_simple() {
        let mut metadata = MetaData::new(5);
        let mut policy = ClockPro::new(5, &mut metadata);

        for i in 0..5 {
            let (test, removed) = policy.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(test.is_none());
            assert!(removed.is_none());
        }
        assert_pages(vec![0, 1, 2, 3, 4], COLD_PAGE, &mut metadata);
        // 0 is hand hot, all insert before 0
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:1key:2key:3key:4key:0"
        );
        assert_eq!(metadata.len(), 5);
        assert_eq!(policy.count_cold, 5);
        assert_eq!(policy.count_hot, 0);
        assert_eq!(policy.count_test, 0);
        assert!(key_to_index("key:4", &mut metadata) < 12);

        for i in 5..10 {
            let (test, removed) = policy.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(test.is_some());
            assert!(removed.is_none());
        }
        assert_pages(vec![0, 6, 7, 8, 9], COLD_PAGE, &mut metadata);
        assert_pages(vec![1, 2, 3, 4, 5], TEST_PAGE, &mut metadata);
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:1key:2key:3key:4key:5key:6key:7key:8key:9key:0"
        );
        assert_eq!(metadata.len(), 10);
        assert_eq!(policy.count_cold, 5);
        assert_eq!(policy.count_hot, 0);
        assert_eq!(policy.count_test, 5);
        assert!(key_to_index("key:9", &mut metadata) < 12);

        // set key 1 again, test page -> hot page
        let index = policy.access("key:1", &mut metadata);
        assert!(index.is_none());
        let (test, removed) = policy.set(key_to_index("key:1", &mut metadata), &mut metadata);
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:2key:3key:4key:5key:6key:7key:8key:9key:1key:0"
        );
        assert_pages(vec![0, 7, 8, 9], COLD_PAGE, &mut metadata);
        assert_pages(vec![1], HOT_PAGE, &mut metadata);
        assert_pages(vec![2, 3, 4, 5, 6], TEST_PAGE, &mut metadata);
        // need to move one to test becauce hot + cold already reach max
        assert!(test.is_some());
        assert!(removed.is_none());
        assert_eq!(metadata.len(), 10);
        assert_eq!(policy.count_cold, 4);
        assert_eq!(policy.count_hot, 1);
        assert_eq!(policy.count_test, 5);

        // move 5 pages to test and remove 5 pages from test
        for i in 10..15 {
            let (test, removed) = policy.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(test.is_some());
            assert!(removed.is_some());
            metadata.remove(removed.unwrap());
        }
        assert_pages(vec![0, 12, 13, 14], COLD_PAGE, &mut metadata);
        assert_pages(vec![1], HOT_PAGE, &mut metadata);
        assert_pages(vec![9, 7, 8, 10, 11], TEST_PAGE, &mut metadata);
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:7key:8key:9key:1key:10key:11key:12key:13key:14key:0"
        );
        assert_eq!(metadata.len(), 10);
        assert!(key_to_index("key:14", &mut metadata) < 12);
        assert_eq!(policy.count_cold, 4);
        assert_eq!(policy.count_hot, 1);
        assert_eq!(policy.count_test, 5);

        // move 7 from test to hot: remove 7 from test -> move 12 to test -> add 7 to hot
        let index = policy.access("key:7", &mut metadata);
        assert!(index.is_none());
        let (test, removed) = policy.set(key_to_index("key:7", &mut metadata), &mut metadata);
        assert!(test.is_some());
        assert!(removed.is_none());
        assert_pages(vec![0, 13, 14], COLD_PAGE, &mut metadata);
        assert_pages(vec![1, 7], HOT_PAGE, &mut metadata);
        assert_pages(vec![9, 8, 10, 11, 12], TEST_PAGE, &mut metadata);
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:8key:9key:1key:10key:11key:12key:13key:14key:7key:0"
        );
        assert_eq!(test.unwrap(), key_to_index("key:12", &mut metadata));
        assert_eq!(metadata.len(), 10);
        assert_eq!(policy.count_cold, 3);
        assert_eq!(policy.count_hot, 2);
        assert_eq!(policy.count_test, 5);

        // access cold page 13, mark ref bit to true
        let index = policy.access("key:13", &mut metadata);
        assert_eq!(metadata.data[index.unwrap() as usize].clock_info, (true, 0));

        // insert new key: move 13 to hot, move 14 to test, remove 7
        let (test, removed) = policy.set(key_to_index("key:15", &mut metadata), &mut metadata);
        assert!(test.is_some());
        assert!(removed.is_some());
        metadata.remove(removed.unwrap());
        assert_pages(vec![0, 15], COLD_PAGE, &mut metadata);
        assert_pages(vec![1, 7, 13], HOT_PAGE, &mut metadata);
        assert_pages(vec![9, 10, 11, 12, 14], TEST_PAGE, &mut metadata);
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:9key:1key:10key:11key:12key:13key:14key:7key:15key:0"
        );
        assert_eq!(metadata.len(), 10);
        assert_eq!(policy.count_cold, 2);
        assert_eq!(policy.count_hot, 3);
        assert_eq!(policy.count_test, 5);
        let index = metadata.get("key:13");
        assert_eq!(
            metadata.data[index.unwrap() as usize].clock_info,
            (false, 1)
        );
        assert!(key_to_index("key:15", &mut metadata) < 12);

        // access all
        for i in 0..16 {
            policy.access(&format!("key:{}", i), &mut metadata);
        }

        // insert 16
        let (test, removed) = policy.set(key_to_index("key:16", &mut metadata), &mut metadata);
        assert!(test.is_some());
        assert!(removed.is_some());
        metadata.remove(removed.unwrap());
        assert!(key_to_index("key:16", &mut metadata) < 12);
        assert_pages(vec![16], COLD_PAGE, &mut metadata);
        assert_pages(vec![1, 7, 13, 15], HOT_PAGE, &mut metadata);
        assert_pages(vec![0, 10, 11, 12, 14], TEST_PAGE, &mut metadata);
        assert_eq!(
            policy.link.display(true, &metadata),
            "key:1key:10key:11key:12key:13key:14key:7key:15key:16key:0"
        );
        assert_eq!(metadata.len(), 10);
        assert_eq!(policy.count_cold, 1);
        assert_eq!(policy.count_hot, 4);
        assert_eq!(policy.count_test, 5);
    }
}
