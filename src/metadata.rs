use dlv_list::{Index, Iter, VecList};

/// Entry represents a cached item with metadata about its position in various data structures.
///
/// # Fields
///
/// - `policy_list_id`: Which list the entry belongs to
///   - `0`: not in policy
///   - `1`: window/lru
///   - `2`: probation
///   - `3`: protected
/// - `policy_list_index`: Position in the policy list (window, probation, or protected)
/// - `wheel_list_index`: Position in the timer wheel for TTL expiration
/// - `wheel_index`: Which bucket in the timer wheel (level, slot)
/// - `expire`: Expiration time in nanoseconds (0 = no expiration)
#[derive(Debug, Clone)]
pub struct Entry {
    pub policy_list_id: u8,
    pub policy_list_index: Option<Index<u64>>,
    pub wheel_list_index: Option<Index<u64>>,
    pub wheel_index: (u8, u8),
    pub expire: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Self::new()
    }
}

impl Entry {
    /// Creates a new cache entry with default values.
    ///
    /// The entry is not associated with any policy list or timer wheel initially.
    #[inline]
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

/// A doubly-linked list wrapper for managing ordered entries in the cache policy.
///
/// This list maintains entries in insertion order, with O(1) operations for
/// inserting at the front and moving entries to the front (for LRU/SLRU policies).
#[derive(Debug)]
pub struct List<T> {
    pub list: VecList<T>,
    pub capacity: usize,
}

impl<T> List<T> {
    /// Creates a new list with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The maximum number of items the list can hold. If 0, defaults to 1.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let list: List<u64> = List::new(100);
    /// ```
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            list: VecList::with_capacity(capacity),
        }
    }

    /// Removes entry at index from list.
    ///
    /// This operation is safe - `dlv_list` handles invalid indices gracefully.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the entry to remove
    #[inline]
    pub fn remove(&mut self, index: Index<T>) {
        self.list.remove(index);
    }

    /// Inserts entry to list front and returns its index.
    ///
    /// Maintains the invariant that newly inserted items are at the front.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to insert
    ///
    /// # Returns
    ///
    /// The index of the newly inserted entry
    pub fn insert_front(&mut self, entry: T) -> Index<T> {
        if let Some(index) = self.list.front_index() {
            self.list.insert_before(index, entry)
        } else {
            self.list.push_front(entry)
        }
    }

    /// Returns the tail (last) entry, if present.
    ///
    /// # Returns
    ///
    /// `Some(&T)` if the list is not empty, `None` otherwise
    #[inline]
    pub fn tail(&self) -> Option<&T> {
        self.list.back()
    }

    /// Returns the value previous to the value at the given index.
    ///
    /// # Arguments
    ///
    /// * `index` - The index to look backwards from
    ///
    /// # Returns
    ///
    /// `Some(&T)` if a previous entry exists, `None` otherwise
    pub fn prev(&self, index: Index<T>) -> Option<&T> {
        self.list
            .get_previous_index(index)
            .and_then(|prev| self.list.get(prev))
    }

    /// Removes and returns the tail entry from the list.
    ///
    /// # Returns
    ///
    /// `Some(T)` if the list was not empty, `None` if empty
    #[inline]
    pub fn pop_tail(&mut self) -> Option<T> {
        self.list.pop_back()
    }

    /// Moves entry to front of list, but only if not already at front.
    ///
    /// This avoids unnecessary operations and maintains LRU semantics efficiently.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the entry to move to front
    pub fn touch(&mut self, index: Index<T>) {
        if let Some(front) = self.list.front_index()
            && front != index
        {
            self.list.move_before(index, front);
        }
    }

    /// Returns an iterator over the list entries.
    ///
    /// # Returns
    ///
    /// An iterator from front to back of the list
    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        self.list.iter()
    }

    /// Returns the current number of entries in the list.
    ///
    /// # Returns
    ///
    /// The number of entries currently stored
    #[inline]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Clears all entries from the list.
    pub fn clear(&mut self) {
        self.list.clear();
    }
}
