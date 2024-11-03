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

pub struct List<T> {
    list: VecList<T>,
    pub capacity: usize,
    pub unbounded: bool,
}

impl<T> List<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            list: VecList::with_capacity(capacity),
            unbounded: false,
        }
    }

    pub fn new_unbounded(capacity: usize) -> Self {
        Self {
            capacity,
            list: VecList::with_capacity(capacity),
            unbounded: true,
        }
    }

    /// Remove entry at index from list
    pub fn remove(&mut self, index: Index<T>) {
        self.list.remove(index);
    }

    /// Insert entry to list front and return evicted key
    pub fn insert_front(&mut self, entry: T) -> (Index<T>, Option<T>) {
        let mut removed = None;

        if !self.unbounded && self.len() == self.capacity {
            removed = self.list.pop_back();
        }

        if let Some(index) = self.list.front_index() {
            let index = self.list.insert_before(index, entry);
            (index, removed)
        } else {
            // no frony entry, list is empty
            let index = self.list.push_front(entry);
            (index, removed)
        }
    }

    /// Get tail entry, return None if empty
    pub fn tail(&self) -> Option<&T> {
        self.list.back()
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::metadata::List;

    #[test]
    fn test_list() {
        let mut list = List::new(5);
        list.insert_front(1);

        assert_eq!(list.iter().map(|i| *i).collect::<Vec<u64>>(), vec![1]);
        assert_eq!(list.len(), 1);

        list.insert_front(2);
        assert_eq!(list.iter().map(|i| *i).collect::<Vec<u64>>(), vec![2, 1]);
        assert_eq!(list.len(), 2);

        list.insert_front(3);
        assert_eq!(list.iter().map(|i| *i).collect::<Vec<u64>>(), vec![3, 2, 1]);
        assert_eq!(list.len(), 3);

        list.insert_front(4);
        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![4, 3, 2, 1]
        );
        assert_eq!(list.len(), 4);

        list.insert_front(5);
        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![5, 4, 3, 2, 1]
        );
        assert_eq!(list.len(), 5);

        list.insert_front(6);
        // exceed max, remove least one(a)
        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![6, 5, 4, 3, 2]
        );

        let mut indexmap = HashMap::new();
        for i in 10..15 {
            let (index, _) = list.insert_front(i);
            indexmap.insert(i, index);
        }

        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![14, 13, 12, 11, 10]
        );

        // tail test
        assert_eq!(*list.tail().unwrap(), 10);

        // pop tail test
        assert_eq!(list.pop_tail().unwrap(), 10);
        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![14, 13, 12, 11]
        );

        // touch test
        list.touch(*indexmap.get(&12).unwrap());
        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![12, 14, 13, 11]
        );

        // remove test
        list.remove(*indexmap.get(&11).unwrap());
        assert_eq!(
            list.iter().map(|i| *i).collect::<Vec<u64>>(),
            vec![12, 14, 13]
        );

        // clear test
        list.clear();
        assert_eq!(list.iter().map(|i| *i).collect::<Vec<u64>>(), vec![]);
    }
}
