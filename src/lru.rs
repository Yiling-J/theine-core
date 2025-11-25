//! Least Recently Used (LRU) eviction policy.
//!
//! A simple policy that evicts the least recently accessed entries first.

use crate::metadata::{Entry, List};
use anyhow::Result;
use dlv_list::Index;
use std::collections::HashMap;

/// Least Recently Used cache policy implementation.
///
/// This policy maintains a doubly-linked list where newly accessed items
/// are moved to the front, and eviction always removes from the back.
///
/// # Note
///
/// Policy list ID for LRU entries is `1`.
#[derive(Debug)]
pub struct Lru {
    pub list: List<u64>,
}

impl Lru {
    /// Creates a new LRU policy with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `maxsize` - Maximum number of entries. Defaults to 1 if 0.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let lru = Lru::new(1000);
    /// ```
    pub fn new(maxsize: usize) -> Self {
        let maxsize = maxsize.max(1);
        log::debug!("LRU created with maxsize={}", maxsize);
        Self {
            list: List::new(maxsize),
        }
    }

    /// Inserts a new key into the LRU list at the front.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key to insert
    /// * `entry` - The entry metadata to update with position information
    pub fn insert(&mut self, key: u64, entry: &mut Entry) {
        let index = self.list.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 1;
    }

    /// Marks an entry as accessed by moving it to the front of the list.
    ///
    /// # Arguments
    ///
    /// * `index` - The current position of the entry in the list
    #[inline]
    pub fn access(&mut self, index: Index<u64>) {
        self.list.touch(index);
    }

    /// Returns the current number of entries in the list.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Removes an entry from the LRU list.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove
    ///
    /// # Returns
    ///
    /// `Ok(())` if removal succeeded, `Err` if the entry's position was missing
    pub fn remove(&mut self, entry: &Entry) -> Result<()> {
        entry
            .policy_list_index
            .ok_or_else(|| {
                let err = anyhow::anyhow!(
                    "LRU remove: missing policy_list_index for entry, this indicates a bug"
                );
                log::error!("{}", err);
                err
            })
            .map(|index| self.list.remove(index))
    }
}

/// Segmented Least Recently Used cache policy.
///
/// This policy maintains two lists: probation (80% of capacity) and protected (20% remainder).
/// New entries start in probation, and are promoted to protected on second access.
/// This provides better scan resistance than simple LRU.
///
/// # Policy List IDs
///
/// - `2`: Probation list (80% capacity)
/// - `3`: Protected list (20% capacity)
#[derive(Debug)]
pub struct Slru {
    pub probation: List<u64>,
    pub protected: List<u64>,
}

impl Slru {
    /// Creates a new SLRU policy with 80/20 split between probation and protected.
    ///
    /// # Arguments
    ///
    /// * `maxsize` - Total capacity. Defaults to 1 if 0.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let slru = Slru::new(1000); // 800 probation, 200 protected
    /// ```
    pub fn new(maxsize: usize) -> Self {
        let maxsize = maxsize.max(1);
        let protected_cap = (maxsize as f64 * 0.8) as usize;
        log::debug!(
            "SLRU created with maxsize={}, protected_cap={}",
            maxsize,
            protected_cap
        );
        Self {
            probation: List::new(maxsize),
            protected: List::new(protected_cap),
        }
    }

    /// Inserts a new key into the probation list.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key to insert
    /// * `entry` - The entry metadata to update with position information
    pub fn insert(&mut self, key: u64, entry: &mut Entry) {
        let index = self.probation.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 2;
    }

    /// Updates policy state when an entry is accessed.
    ///
    /// If in probation (first access), promotes to protected.
    /// If in protected (subsequent accesses), marks as recently used.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key being accessed
    /// * `entries` - Mutable reference to the cache entries map
    ///
    /// # Returns
    ///
    /// `Ok(())` if access succeeded, `Err` if entry state is invalid
    pub fn access(&mut self, key: u64, entries: &mut HashMap<u64, Entry>) -> Result<()> {
        entries
            .get_mut(&key)
            .ok_or_else(|| {
                // Entry not found is not an error in this context
                anyhow::anyhow!("Entry not found during access")
            })
            .and_then(|entry| self.handle_access(entry, key))
    }

    /// Internal helper to handle access for a specific entry.
    fn handle_access(&mut self, entry: &mut Entry, key: u64) -> Result<()> {
        match entry.policy_list_id {
            2 => self.promote_from_probation(entry, key),
            3 => self.touch_in_protected(entry),
            list_id => {
                let err = anyhow::anyhow!(
                    "SLRU access: unexpected policy_list_id {} for entry {}, this indicates a bug",
                    list_id,
                    key
                );
                log::error!("{}", err);
                Err(err)
            }
        }
    }

    /// Promotes an entry from probation to protected list.
    fn promote_from_probation(&mut self, entry: &mut Entry, key: u64) -> Result<()> {
        entry
            .policy_list_index
            .ok_or_else(|| {
                let err = anyhow::anyhow!(
                    "SLRU access: missing policy_list_index for probation entry {}, this indicates a bug",
                    key
                );
                log::error!("{}", err);
                err
            })
            .map(|index| {
                self.probation.remove(index);
                let new_index = self.protected.insert_front(key);
                entry.policy_list_index = Some(new_index);
                entry.policy_list_id = 3;
            })
    }

    /// Marks an entry in protected list as recently used.
    fn touch_in_protected(&mut self, entry: &mut Entry) -> Result<()> {
        entry
            .policy_list_index
            .ok_or_else(|| {
                let err = anyhow::anyhow!(
                    "SLRU access: missing policy_list_index for protected entry, this indicates a bug"
                );
                log::error!("{}", err);
                err
            })
            .map(|index| {
                self.protected.touch(index);
            })
    }

    /// Removes an entry from either the probation or protected list.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove
    ///
    /// # Returns
    ///
    /// `Ok(())` if removal succeeded, `Err` if entry state is invalid
    pub fn remove(&mut self, entry: &Entry) -> Result<()> {
        let list_index = entry
            .policy_list_index
            .ok_or_else(|| {
                let err = anyhow::anyhow!(
                    "SLRU remove: missing policy_list_index for entry with policy_list_id {}, this indicates a bug",
                    entry.policy_list_id
                );
                log::error!("{}", err);
                err
            })?;

        match entry.policy_list_id {
            2 => {
                self.probation.remove(list_index);
                Ok(())
            }
            3 => {
                self.protected.remove(list_index);
                Ok(())
            }
            list_id => {
                let err = anyhow::anyhow!(
                    "SLRU remove: unexpected policy_list_id {}, this indicates a bug",
                    list_id
                );
                log::error!("{}", err);
                Err(err)
            }
        }
    }
}
