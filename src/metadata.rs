use dlv_list::{Index, Iter, VecList};

pub struct Entry {
    pub policy_list_id: u8,
    pub policy_list_index: Option<Index<u64>>,
    pub wheel_list_index: Option<Index<u64>>,
    pub wheel_index: (u8, u8),
    pub expire: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Entry::new()
    }
}

impl Entry {
    pub fn new() -> Self {
        Self {
            policy_list_index: None,
            wheel_list_index: None,
            wheel_index: (0, 0),
            expire: 0,
            policy_list_id: 0,
        }
    }
}

#[derive(Debug)]
pub struct List<T> {
    pub list: VecList<T>,
    pub capacity: usize,
}

impl<T> List<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            list: VecList::with_capacity(capacity),
        }
    }

    /// Remove entry at index from list
    pub fn remove(&mut self, index: Index<T>) {
        self.list.remove(index);
    }

    /// Insert entry to list front and return evicted key
    pub fn insert_front(&mut self, entry: T) -> Index<T> {
        if let Some(index) = self.list.front_index() {
            self.list.insert_before(index, entry)
        } else {
            // no frony entry, list is empty

            self.list.push_front(entry)
        }
    }

    /// Get tail entry, return None if empty
    pub fn tail(&self) -> Option<&T> {
        self.list.back()
    }

    /// Returns the value previous to the value at the given index
    pub fn prev(&self, index: Index<T>) -> Option<&T> {
        if let Some(prev) = self.list.get_previous_index(index) {
            self.list.get(prev)
        } else {
            None
        }
    }

    /// Remove tail entry from list
    pub fn pop_tail(&mut self) -> Option<T> {
        self.list.pop_back()
    }

    /// Move entry to front of link
    pub fn touch(&mut self, index: Index<T>) {
        if let Some(front) = self.list.front_index() {
            if front != index {
                self.list.move_before(index, front);
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.list.iter()
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Clear list
    pub fn clear(&mut self) {
        self.list.clear()
    }
}
