use ahash::AHashMap;
use compact_str::CompactString;
use std::mem::replace;

pub const COLD_PAGE: u8 = 1;
pub const HOT_PAGE: u8 = 2;
pub const TEST_PAGE: u8 = 3;

pub struct Entry {
    pub key: CompactString,
    pub index: u32,
    pub prev: u32,
    pub next: u32,
    pub link_id: u8,
    pub wheel_link_id: u8,
    pub wheel_index: (u8, u8),
    pub wheel_prev: u32,
    pub wheel_next: u32,
    pub expire: u128,
    pub clock_info: (bool, u8),
}

impl Entry {
    pub fn new(key: &str) -> Self {
        Self {
            key: CompactString::new(key),
            index: 0,
            prev: 0,
            next: 0,
            wheel_prev: 0,
            wheel_next: 0,
            link_id: 0,
            wheel_link_id: 0,
            wheel_index: (0, 0),
            expire: 0,
            clock_info: (false, COLD_PAGE), // new entry should be cold page and no reference
        }
    }
}

pub struct Link {
    pub id: u8, // 1: lru, 2: slru_protected, 3: slru probation, 4+: wheel
    pub root: u32,
    pub len: u32,
    pub capacity: u32,
}

impl Link {
    pub fn new(id: u8, capacity: u32, metadata: &mut MetaData) -> Self {
        metadata.meta_key_count += 1;
        let root = metadata.insert_key(format!("__root:{}__", id).as_str());
        root.clock_info = (false, 0);
        root.link_id = id;
        root.wheel_link_id = id;
        // 1: lru, 2: probation, 3: protected, 3+: timerwheel
        if id < 4 {
            root.prev = root.index;
            root.next = root.index;
        } else {
            root.wheel_prev = root.index;
            root.wheel_next = root.index;
        }
        Self {
            id,
            root: root.index,
            len: 0,
            capacity,
        }
    }

    /// Insert entry after at, increments len, and returns evicted entry
    pub fn insert(&mut self, index: u32, at: u32, metadata: &mut MetaData) -> Option<u32> {
        // remove from tail if full
        let mut removed = 0;
        if self.len == self.capacity {
            let tail = metadata.data[self.root as usize].prev;
            self.remove(tail, metadata);
            removed = tail;
        }
        let at_entry = &mut metadata.data[at as usize];
        let old_next = at_entry.next;
        at_entry.next = index;
        let entry = &mut metadata.data[index as usize];
        entry.link_id = self.id;
        entry.prev = at;
        entry.next = old_next;
        let next_entry = &mut metadata.data[old_next as usize];
        next_entry.prev = index;
        self.len += 1;
        if removed > 0 {
            return Some(removed);
        }
        None
    }

    /// Insert entry before at, increments len, and returns evicted entry
    pub fn insert_before(&mut self, index: u32, at: u32, metadata: &mut MetaData) -> Option<u32> {
        // remove from tail if full
        let mut removed = 0;
        if self.len == self.capacity {
            let tail = metadata.data[self.root as usize].prev;
            self.remove(tail, metadata);
            removed = tail;
        }
        let at_entry = &mut metadata.data[at as usize];
        let old_prev = at_entry.prev;
        at_entry.prev = index;
        let entry = &mut metadata.data[index as usize];
        entry.link_id = self.id;
        entry.next = at;
        entry.prev = old_prev;
        let prev_entry = &mut metadata.data[old_prev as usize];
        prev_entry.next = index;
        self.len += 1;
        if removed > 0 {
            return Some(removed);
        }
        None
    }

    /// Insert entry after at, increments len, and returns evicted entry
    pub fn insert_wheel(&mut self, index: u32, at: u32, metadata: &mut MetaData) {
        let at_entry = &mut metadata.data[at as usize];
        let old_next = at_entry.wheel_next;
        at_entry.wheel_next = index;
        let entry = &mut metadata.data[index as usize];
        entry.wheel_prev = at;
        entry.wheel_next = old_next;
        entry.wheel_link_id = self.id;
        let next_entry = &mut metadata.data[old_next as usize];
        next_entry.wheel_prev = index;
        self.len += 1;
    }

    /// Remove entry at index from link, decrease len
    pub fn remove(&mut self, index: u32, metadata: &mut MetaData) -> Option<u32> {
        if index == self.root {
            return None;
        }
        let entry = &mut metadata.data[index as usize];
        if entry.link_id != self.id {
            return None;
        }
        let removed_index = entry.index;
        let prev = entry.prev;
        let next = entry.next;
        let prev_entry = &mut metadata.data[prev as usize];
        prev_entry.next = next;
        let next_entry = &mut metadata.data[next as usize];
        next_entry.prev = prev;
        self.len -= 1;
        Some(removed_index)
    }

    /// Remove entry from wheel link, decrease len
    pub fn remove_wheel(&mut self, index: u32, metadata: &mut MetaData) {
        let entry = &mut metadata.data[index as usize];
        if entry.wheel_link_id != self.id {
            panic!("link id not match");
        }
        entry.wheel_link_id = 0;
        entry.wheel_index = (0, 0);
        let prev = entry.wheel_prev;
        let next = entry.wheel_next;
        entry.wheel_prev = 0;
        entry.wheel_next = 0;
        let prev_entry = &mut metadata.data[prev as usize];
        prev_entry.wheel_next = next;
        let next_entry = &mut metadata.data[next as usize];
        next_entry.wheel_prev = prev;
        self.len -= 1;
    }

    /// Insert entry to link front and return evicted key
    pub fn insert_front(&mut self, index: u32, metadata: &mut MetaData) -> Option<u32> {
        self.insert(index, self.root, metadata)
    }

    /// Insert entry to link front and return evicted key
    pub fn insert_front_wheel(&mut self, index: u32, metadata: &mut MetaData) {
        self.insert_wheel(index, self.root, metadata)
    }

    /// Get tail entry, return None if empty
    pub fn tail(&self, metadata: &MetaData) -> Option<u32> {
        let tail = &metadata.data[self.root as usize];
        if tail.prev == self.root {
            return None;
        }
        Some(tail.prev)
    }

    /// Remove tail entry from link
    pub fn pop_tail(&mut self, metadata: &mut MetaData) -> Option<u32> {
        let tail_index = metadata.data[self.root as usize].prev;
        if tail_index != self.root {
            self.remove(tail_index, metadata);
            return Some(tail_index);
        }
        None
    }

    /// Move entry to front of link
    pub fn touch(&mut self, index: u32, metadata: &mut MetaData) {
        self.remove(index, metadata);
        self.insert_front(index, metadata);
    }

    /// Clear link, only keep root
    pub fn clear(&mut self, metadata: &mut MetaData) {
        let entry = &mut metadata.data[self.root as usize];
        entry.prev = entry.index;
        entry.next = entry.index;
        entry.wheel_prev = entry.index;
        entry.wheel_next = entry.index;
    }

    /// Creates an iterator that yields mutable references to values in the link
    pub fn iter_wheel<'a>(&'a self, metadata: &'a MetaData) -> IterWheel {
        let index = metadata.data[self.root as usize].wheel_next;
        IterWheel {
            metadata,
            root: self.root,
            index,
            _id: self.id,
        }
    }

    pub fn display(&self, latest: bool, metadata: &MetaData) -> String {
        let root = &metadata.data[self.root as usize];
        let mut result = String::from("");
        if latest {
            let mut current = root.next;
            while current != self.root {
                let node = &metadata.data[current as usize];
                if node.link_id != self.id {
                    panic!(
                        "link id mismatch! node link id: {}, link id: {}",
                        node.link_id, self.id,
                    );
                }
                result.push_str(node.key.as_str());
                current = node.next;
            }
        } else {
            let mut current = root.prev;
            while current != self.root {
                let node = &metadata.data[current as usize];
                if node.link_id != self.id {
                    panic!(
                        "link id mismatch! node link id: {}, link id: {}",
                        node.link_id, self.id,
                    );
                }
                result.push_str(node.key.as_str());
                current = node.prev;
            }
        }
        result
    }

    pub fn display_wheel(&self, latest: bool, metadata: &MetaData) -> String {
        let root = &metadata.data[self.root as usize];
        let mut result = String::from("");
        if latest {
            let mut current = root.wheel_next;
            while current != self.root {
                let node = &metadata.data[current as usize];
                if node.wheel_link_id != self.id {
                    panic!(
                        "link id mismatch! node link id: {}, link id: {}",
                        node.wheel_link_id, self.id,
                    );
                }
                result.push_str(node.key.as_str());
                current = node.wheel_next;
            }
        } else {
            let mut current = root.wheel_prev;
            while current != self.root {
                let node = &metadata.data[current as usize];
                if node.wheel_link_id != self.id {
                    panic!(
                        "link id mismatch! node link id: {}, link id: {}",
                        node.wheel_link_id, self.id,
                    );
                }
                result.push_str(node.key.as_str());
                current = node.wheel_prev;
            }
        }
        result
    }
}

/// An iterator that yields mutable references to entries in the link
pub struct IterWheel<'a> {
    metadata: &'a MetaData,
    index: u32,
    root: u32,
    _id: u8,
}

impl<'a> Iterator for IterWheel<'a> {
    type Item = (u32, String, u128);

    fn next(&mut self) -> Option<Self::Item> {
        if self.root == self.index {
            None
        } else {
            let current = self.index;
            let entry = &self.metadata.data[current as usize];
            if entry.wheel_link_id != self._id {
                panic!("wheel link not match");
            }
            self.index = entry.wheel_next;
            Some((current, entry.key.to_string(), entry.expire))
        }
    }
}

pub struct MetaData {
    keys: AHashMap<CompactString, u32>,
    pub data: Vec<Entry>,
    empty: Vec<u32>,
    meta_key_count: usize,
}

impl MetaData {
    pub fn new(size: usize) -> Self {
        Self {
            keys: AHashMap::new(),
            data: Vec::with_capacity(size + 500), // key node size + meta node size
            empty: Vec::with_capacity(size),
            meta_key_count: 0,
        }
    }

    // get entry by key
    pub fn get(&mut self, key: &str) -> Option<u32> {
        if let Some(index) = self.keys.get(key) {
            return Some(*index);
        }
        None
    }

    // get entry by key string, create new if not exist
    pub fn get_or_create(&mut self, key: &str) -> &mut Entry {
        if let Some(index) = self.keys.get(key) {
            return &mut self.data[*index as usize];
        }
        self.insert_key(key)
    }

    // remove entry
    pub fn remove(&mut self, index: u32) {
        self.keys.remove(&self.data[index as usize].key);
        self.empty.push(index);
    }

    // insert new entry to container and return
    fn insert_key(&mut self, key: &str) -> &mut Entry {
        let mut entry = Entry::new(key);
        if let Some(index) = self.empty.pop() {
            let tmp = &mut self.data[index as usize];
            entry.index = index;
            _ = replace(tmp, entry);
            if !key.starts_with("__root:") {
                self.keys.insert(CompactString::new(key), index);
            }
            &mut self.data[index as usize]
        } else {
            let index = self.data.len();
            entry.index = index as u32;
            self.data.push(entry);
            if !key.starts_with("__root:") {
                self.keys.insert(CompactString::new(key), index as u32);
            }
            &mut self.data[index]
        }
    }

    pub fn clear(&mut self) {
        self.empty.clear();
        self.keys.clear();
        for d in self.data.iter() {
            if !d.key.starts_with("__root:") {
                self.empty.push(d.index);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{Link, MetaData};

    #[test]
    fn test_link() {
        let mut metadata = MetaData::new(5);
        let mut link = Link::new(1, 5, &mut metadata);
        let entry_a = metadata.get_or_create("a");
        link.insert_front(entry_a.index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "a");
        assert_eq!(link.display(false, &metadata), "a");
        assert_eq!(metadata.len(), 1);
        let entry_b = metadata.get_or_create("b");
        link.insert_front(entry_b.index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "ba");
        assert_eq!(link.display(false, &metadata), "ab");
        assert_eq!(metadata.len(), 2);
        let entry_c = metadata.get_or_create("c");
        link.insert_front(entry_c.index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "cba");
        assert_eq!(link.display(false, &metadata), "abc");
        assert_eq!(metadata.len(), 3);
        let entry_d = metadata.get_or_create("d");
        link.insert_front(entry_d.index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "dcba");
        assert_eq!(link.display(false, &metadata), "abcd");
        assert_eq!(metadata.len(), 4);
        let entry_e = metadata.get_or_create("e");
        link.insert_front(entry_e.index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "edcba");
        assert_eq!(link.display(false, &metadata), "abcde");
        assert_eq!(metadata.len(), 5);

        let entry_f = metadata.get_or_create("f");
        link.insert_front(entry_f.index, &mut metadata);
        // exceed max, remove least one(a)
        assert_eq!(link.display(true, &metadata), "fedcb");
        assert_eq!(link.display(false, &metadata), "bcdef");
        for i in 0..5 {
            link.insert_front(
                metadata.get_or_create(format!("{}", i).as_str()).index,
                &mut metadata,
            );
        }
        assert_eq!(link.display(true, &metadata), "43210");
        assert_eq!(link.display(false, &metadata), "01234");
        // tail test
        let tail = metadata.data[link.tail(&metadata).unwrap() as usize]
            .key
            .to_string();
        assert_eq!(tail, "0");
        // pop tail test
        let tail_index = link.pop_tail(&mut metadata).unwrap() as usize;
        let tail = metadata.data[tail_index].key.to_string();
        assert_eq!(tail, "0");
        assert_eq!(link.display(true, &metadata), "4321");
        assert_eq!(link.display(false, &metadata), "1234");
        // touch test
        link.touch(metadata.get("2").unwrap(), &mut metadata);
        assert_eq!(link.display(true, &metadata), "2431");
        assert_eq!(link.display(false, &metadata), "1342");
        // insert at
        let entry_x = metadata.get_or_create("x");
        link.insert(entry_x.index, metadata.get("3").unwrap(), &mut metadata);
        assert_eq!(link.display(true, &metadata), "243x1");
        assert_eq!(link.display(false, &metadata), "1x342");
        // remove test
        let index = metadata.get("1").unwrap();
        link.remove(index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "243x");
        assert_eq!(link.display(false, &metadata), "x342");
        let index = metadata.get("2").unwrap();
        link.remove(index, &mut metadata);
        assert_eq!(link.display(true, &metadata), "43x");
        assert_eq!(link.display(false, &metadata), "x34");
        // insert before
        let entry_q = metadata.get_or_create("q");
        link.insert_before(entry_q.index, metadata.get("x").unwrap(), &mut metadata);
        assert_eq!(link.display(true, &metadata), "43qx");
        assert_eq!(link.display(false, &metadata), "xq34");
        // clear test
        link.clear(&mut metadata);
        assert_eq!(link.display(true, &metadata), "");
        assert_eq!(link.display(false, &metadata), "");
    }

    #[test]
    fn test_link_wheel() {
        let mut metadata = MetaData::new(5);
        let mut link = Link::new(5, 100, &mut metadata);
        let entry_a = metadata.get_or_create("a");
        link.insert_front_wheel(entry_a.index, &mut metadata);
        assert_eq!(link.display_wheel(true, &metadata), "a");
        assert_eq!(link.display_wheel(false, &metadata), "a");
        let entry_b = metadata.get_or_create("b");
        link.insert_front_wheel(entry_b.index, &mut metadata);
        let entry_c = metadata.get_or_create("c");
        link.insert_front_wheel(entry_c.index, &mut metadata);
        let entry_d = metadata.get_or_create("d");
        link.insert_front_wheel(entry_d.index, &mut metadata);
        let entry_e = metadata.get_or_create("e");
        link.insert_front_wheel(entry_e.index, &mut metadata);
        // latest first
        assert_eq!(link.display_wheel(true, &metadata), "edcba");
        // least first
        assert_eq!(link.display_wheel(false, &metadata), "abcde");
        // test iter
        let mut data = String::new();
        for (_index, key, _expire) in link.iter_wheel(&metadata) {
            data.push_str(key.as_str());
        }
        assert_eq!(data, "edcba");
        // test remove
        let index = metadata.get("c").unwrap();
        link.remove_wheel(index, &mut metadata);
        assert_eq!(link.display_wheel(true, &metadata), "edba");
        assert_eq!(link.display_wheel(false, &metadata), "abde");
        let index = metadata.get("e").unwrap();
        link.remove_wheel(index, &mut metadata);
        assert_eq!(link.display_wheel(true, &metadata), "dba");
        assert_eq!(link.display_wheel(false, &metadata), "abd");
        let index = metadata.get("b").unwrap();
        link.remove_wheel(index, &mut metadata);
        assert_eq!(link.display_wheel(true, &metadata), "da");
        assert_eq!(link.display_wheel(false, &metadata), "ad");
        let index = metadata.get("d").unwrap();
        link.remove_wheel(index, &mut metadata);
        assert_eq!(link.display_wheel(true, &metadata), "a");
        assert_eq!(link.display_wheel(false, &metadata), "a");
        let index = metadata.get("a").unwrap();
        link.remove_wheel(index, &mut metadata);
        assert_eq!(link.display_wheel(true, &metadata), "");
        assert_eq!(link.display_wheel(false, &metadata), "");
    }

    #[test]
    fn test_len() {
        let mut metadata = MetaData::new(5);
        let mut link = Link::new(1, 5, &mut metadata);
        assert_eq!(metadata.len(), 0);
        let index_a = metadata.get_or_create("a").index;
        link.insert_front(index_a, &mut metadata);
        assert_eq!(metadata.len(), 1);
        let index_b = metadata.get_or_create("b").index;
        link.insert_front(index_a, &mut metadata);
        assert_eq!(metadata.len(), 2);
        metadata.remove(index_a);
        assert_eq!(metadata.len(), 1);
        metadata.remove(index_b);
        assert_eq!(metadata.len(), 0);
        metadata.clear();
        assert_eq!(metadata.len(), 0);
    }
}
