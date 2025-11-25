//! Core cache implementation
//!
//! This module implements the TinyLFU cache algorithm with TTL support,
//! exposed to Python through PyO3.
//!
//! # Thread Safety
//!
//! `TlfuCore` is not thread-safe. Users must wrap it in a `Mutex` or `RwLock`
//! when sharing across threads.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;

use crate::errors::catch_panic;
use crate::{metadata::Entry, timerwheel::TimerWheel, tlfu::DebugInfo, tlfu::TinyLfu};

/// TinyLFU cache with TTL support
///
/// Thread-safe operation requires external synchronization (Mutex/RwLock).
/// See module documentation for usage details.
#[pyclass]
pub struct TlfuCore {
    policy: TinyLfu,
    pub(crate) wheel: TimerWheel,
    pub(crate) entries: HashMap<u64, Entry>,
}

#[pymethods]
impl TlfuCore {
    /// Creates a new cache with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `size` - Maximum number of entries to cache
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut cache = TlfuCore::new(1000);
    /// ```
    #[new]
    pub fn new(size: usize) -> Self {
        Self {
            policy: TinyLfu::new(size),
            wheel: TimerWheel::new(),
            entries: HashMap::with_capacity(size),
        }
    }

    /// Sets or updates a cache entry, handling eviction if necessary.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key
    /// * `ttl` - Time-to-live in nanoseconds
    ///
    /// # Returns
    ///
    /// `Some(evicted_key)` if an entry was evicted to make room, `None` otherwise
    fn set_entry(&mut self, key: u64, ttl: u64) -> Option<u64> {
        // Update existing entry
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.expire = self.wheel.clock.expire_ns(ttl);
            self.wheel.schedule(key, entry);
            return None;
        }

        // Create new entry
        let mut entry = Entry::new();
        entry.expire = self.wheel.clock.expire_ns(ttl);
        self.wheel.schedule(key, &mut entry);
        self.entries.insert(key, entry);

        self.policy
            .set(key, &mut self.entries)
            .ok()
            .flatten()
            .inspect(|&evicted_key| {
                if let Some(evicted) = self.entries.get_mut(&evicted_key) {
                    self.wheel.deschedule(evicted);
                }
                self.entries.remove(&evicted_key);
                log::debug!("Evicted key {} for key {}", evicted_key, key);
            })
    }

    /// Sets multiple cache entries in a batch operation.
    ///
    /// Entries with TTL of -1 are removed instead of added.
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of (key, ttl) pairs where ttl=-1 means remove
    ///
    /// # Returns
    ///
    /// Vector of keys that were evicted to make room for new entries
    pub fn set(&mut self, entries: Vec<(u64, i64)>) -> Vec<u64> {
        let mut evicted = HashSet::new();

        for (key, ttl) in entries {
            match ttl {
                -1 => self.remove_internal(key),
                _ if !evicted.contains(&key) => {
                    if let Some(evicted_key) = self.set_entry(key, ttl.unsigned_abs()) {
                        evicted.insert(evicted_key);
                    }
                }
                _ => {}
            }
        }

        // Clean up evicted entries
        evicted.retain(|key| self.entries.remove(key).is_some());

        log::debug!(
            "Set: {} entries evicted, size={}",
            evicted.len(),
            self.entries.len()
        );

        evicted.into_iter().collect()
    }

    /// Removes an entry from all internal structures.
    #[inline]
    fn remove_internal(&mut self, key: u64) {
        if let Some(mut entry) = self.entries.remove(&key) {
            let _ = self.policy.remove(&mut entry).map_err(|e| {
                log::warn!("Failed to remove key {} from policy: {}", key, e);
            });
            self.wheel.deschedule(&mut entry);
            log::debug!("Removed key {}", key);
        }
    }

    /// Removes a specific key from the cache.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove
    ///
    /// # Returns
    ///
    /// `Some(key)` if the key was found and removed, `None` if not present
    pub fn remove(&mut self, key: u64) -> Option<u64> {
        self.entries.remove(&key).map(|mut entry| {
            let _ = self.policy.remove(&mut entry).map_err(|e| {
                log::error!("remove(key={}): {}", key, e);
            });
            self.wheel.deschedule(&mut entry);
            log::debug!("Removed key {}", key);
            key
        })
    }

    /// Marks entries as accessed to update their position in the policy.
    ///
    /// # Arguments
    ///
    /// * `keys` - Vector of keys to mark as accessed
    pub fn access(&mut self, keys: Vec<u64>) {
        log::trace!("Accessing {} keys", keys.len());
        for key in keys {
            self.access_entry(key);
        }
    }

    /// Updates policy state for a single accessed entry.
    #[inline]
    fn access_entry(&mut self, key: u64) {
        let _ = self
            .policy
            .access(key, &self.wheel.clock, &mut self.entries)
            .map_err(|e| {
                log::error!("access(key={}): {}", key, e);
            });
    }

    /// Processes TTL expirations and removes expired entries from the cache.
    ///
    /// This advances the internal timer wheel and returns all keys that expired
    /// during the advancement.
    ///
    /// # Returns
    ///
    /// Vector of keys that were expired and removed
    pub fn advance(&mut self) -> Vec<u64> {
        let expired = self
            .wheel
            .advance(self.wheel.clock.now_ns(), &mut self.entries);

        let expired_count = expired.len();

        for &key in &expired {
            if let Some(mut entry) = self.entries.remove(&key) {
                let _ = self.policy.remove(&mut entry).map_err(|e| {
                    log::error!("advance(key={}): {}", key, e);
                });
                log::trace!("Expired key {}", key);
            }
        }

        if expired_count > 0 {
            log::debug!("Advance: {} entries expired", expired_count);
        }

        expired
    }

    /// Removes all entries from the cache.
    pub fn clear(&mut self) {
        self.wheel.clear();
        self.entries.clear();
        log::debug!("Cache cleared");
    }

    /// Returns the number of entries currently in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns debugging information about the cache state.
    #[must_use]
    pub fn debug_info(&self) -> DebugInfo {
        self.policy.debug_info()
    }

    /// Returns all keys currently stored in the cache.
    #[must_use]
    pub fn keys(&self) -> Vec<u64> {
        self.entries.keys().copied().collect()
    }

    /// Sets multiple entries with panic safety for Python FFI.
    pub fn set_with_error(&mut self, entries: Vec<(u64, i64)>) -> PyResult<Vec<u64>> {
        use std::panic::AssertUnwindSafe;
        catch_panic(AssertUnwindSafe(|| self.set(entries)), "set")
    }

    /// Marks entries as accessed with panic safety for Python FFI.
    pub fn access_with_error(&mut self, keys: Vec<u64>) -> PyResult<()> {
        use std::panic::AssertUnwindSafe;
        catch_panic(AssertUnwindSafe(|| self.access(keys)), "access")
    }

    /// Advances the timer wheel with panic safety for Python FFI.
    pub fn advance_with_error(&mut self) -> PyResult<Vec<u64>> {
        use std::panic::AssertUnwindSafe;
        catch_panic(AssertUnwindSafe(|| self.advance()), "advance")
    }
}

/// Supplemental hash function for Python hash values.
///
/// Python's hash function returns `i64` which can be negative or weakly distributed.
/// This function applies the MurmurHash3 finalizer to improve distribution and prevent hash DoS.
///
/// # Arguments
///
/// * `h` - Python's hash value (an i64)
///
/// # Returns
///
/// A well-distributed u64 hash value
///
/// # Examples
///
/// ```ignore
/// let python_hash = 12345i64;
/// let improved_hash = spread(python_hash);
/// ```
#[pyfunction]
#[must_use]
pub fn spread(h: i64) -> u64 {
    // Convert i64 to u64 preserving bit pattern
    let mut z = u64::from_ne_bytes(h.to_ne_bytes());

    // Apply MurmurHash3 finalizer for better distribution
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^= z >> 31;

    z
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_set_operations() {
        let mut cache = TlfuCore::new(1000);

        // Add entries
        cache.set(vec![(1, 0), (2, 0), (3, 0)]);
        let mut keys: Vec<_> = cache.entries.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 3]);

        // Remove entry 3, add entry 4, re-add entry 3
        cache.set(vec![(3, -1), (4, 0), (3, 0)]);
        keys = cache.entries.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 3, 4]);

        // Remove entry 3, keep entry 4
        cache.set(vec![(3, -1), (4, 0)]);
        keys = cache.entries.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 4]);
    }

    #[test]
    fn test_remove_operation() {
        let mut cache = TlfuCore::new(1000);
        cache.set(vec![(1, 0), (2, 0), (3, 0)]);
        cache.remove(2);

        let mut keys: Vec<_> = cache.entries.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 3]);
    }

    #[test]
    fn test_bounded_cache_size() {
        for size in [1, 2, 3] {
            let mut cache = TlfuCore::new(size);
            cache.set(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]);
            assert_eq!(cache.len(), size);

            cache.access(vec![1]);
            cache.set(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]);
            assert_eq!(cache.len(), size);
        }
    }

    #[test]
    fn test_spread_hash_function() {
        let mut rng = rand::rng();

        for _ in 0..100_000 {
            let k = rng.random_range(-i64::MAX..i64::MAX);
            let _hashed = spread(k);
            // Just verify it doesn't panic
        }
    }

    #[test]
    fn test_clear() {
        let mut cache = TlfuCore::new(100);
        cache.set(vec![(1, 0), (2, 0), (3, 0)]);
        assert!(cache.len() > 0);

        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_keys() {
        let mut cache = TlfuCore::new(100);
        let entries = vec![(1, 0), (2, 0), (3, 0)];
        cache.set(entries.clone());

        let mut keys = cache.keys();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 3]);
    }
}
