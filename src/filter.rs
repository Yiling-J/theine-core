//! Probabilistic data structure for membership testing.
//!
//! A Bloom filter is a space-efficient probabilistic data structure that can tell you
//! whether an element is definitely not in a set or might be in a set.
//!
//! # Note on Thread Safety
//!
//! `BloomFilter` is not thread-safe. Wrap it in a `Mutex` when sharing across threads.

use pyo3::prelude::*;

/// A Bloom filter implementation optimized for cache admission control.
///
/// The filter automatically resets when the number of additions exceeds the
/// configured insertion count to control false positive rate growth.
///
/// # Examples
///
/// ```ignore
/// let mut filter = BloomFilter::new(1000, 0.001);
/// filter.put(42);
/// assert!(filter.contains(42));
/// ```
#[pyclass]
pub struct BloomFilter {
    insertions: usize,
    bits_mask: usize,
    slice_count: usize,
    bits: Vec<u64>,
    additions: usize,
}

#[pymethods]
impl BloomFilter {
    /// Creates a new Bloom filter with the specified false positive probability.
    ///
    /// # Arguments
    ///
    /// * `insertions` - Expected number of elements to insert. Defaults to 1 if 0.
    /// * `fpp` - False positive probability. Will be clamped to range [0.001, 0.999].
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Create filter for ~100 elements with 0.1% false positive rate
    /// let filter = BloomFilter::new(100, 0.001);
    /// ```
    #[new]
    fn new(insertions: usize, fpp: f64) -> Self {
        let insertions = insertions.max(1);
        let fpp = fpp.clamp(0.001, 0.999);

        let ln2 = 2f64.ln();
        let factor = -fpp.ln() / (ln2 * ln2);
        let bits = ((insertions as f64 * factor) as usize)
            .next_power_of_two()
            .max(1);

        let slice_count = ((ln2 * bits as f64 / insertions as f64) as usize).max(1);

        log::debug!(
            "BloomFilter created: insertions={}, fpp={}, bits={}, slice_count={}",
            insertions,
            fpp,
            bits,
            slice_count
        );

        Self {
            insertions,
            bits_mask: bits - 1,
            slice_count,
            bits: vec![0; bits.div_ceil(64)],
            additions: 0,
        }
    }

    /// Adds a key to the filter.
    ///
    /// Automatically resets the filter when the number of additions reaches
    /// the configured insertion count.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to add to the filter
    pub fn put(&mut self, key: u64) {
        self.additions += 1;
        if self.additions >= self.insertions {
            self.reset();
        }

        for i in 0..self.slice_count {
            let hash = key.wrapping_add((i as u64).wrapping_mul(key >> 32));
            let hash_index = (hash & self.bits_mask as u64) as usize;
            self.set(hash_index);
        }
    }

    /// Checks if a bit at the given index is set.
    ///
    /// # Arguments
    ///
    /// * `key` - The bit index to check
    ///
    /// # Returns
    ///
    /// `true` if the bit is set, `false` otherwise
    #[inline]
    fn get(&self, key: usize) -> bool {
        if key >= self.bits.len() * 64 {
            log::warn!("BloomFilter get: key {} out of bounds", key);
            return false;
        }

        let idx = key >> 6;
        let offset = key & 63;

        idx < self.bits.len() && ((self.bits[idx] >> offset) & 1) != 0
    }

    /// Sets a bit at the given index.
    ///
    /// # Arguments
    ///
    /// * `key` - The bit index to set
    #[inline]
    fn set(&mut self, key: usize) {
        if key >= self.bits.len() * 64 {
            log::warn!("BloomFilter set: key {} out of bounds", key);
            return;
        }

        let idx = key >> 6;
        let offset = key & 63;

        if idx < self.bits.len() {
            self.bits[idx] |= 1u64 << offset;
        } else {
            log::warn!(
                "BloomFilter set: computed idx {} exceeds bits length {}",
                idx,
                self.bits.len()
            );
        }
    }

    /// Tests whether a key might be in the set.
    ///
    /// Returns `false` if the key is definitely not in the set.
    /// Returns `true` if the key might be in the set (could be a false positive).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to test for membership
    ///
    /// # Returns
    ///
    /// `false` if definitely not present, `true` if possibly present
    #[must_use]
    pub fn contains(&self, key: u64) -> bool {
        if self.slice_count == 0 {
            log::warn!(
                "BloomFilter contains: slice_count is 0, this indicates a configuration error"
            );
            return false;
        }

        (0..self.slice_count).all(|i| {
            let hash = key.wrapping_add((i as u64).wrapping_mul(key >> 32));
            let hash_index = (hash & self.bits_mask as u64) as usize;
            self.get(hash_index)
        })
    }

    /// Resets the filter, clearing all bits and resetting the addition counter.
    fn reset(&mut self) {
        self.bits = vec![0; self.bits.len()];
        self.additions = 0;
        log::debug!("BloomFilter reset: cleared all bits");
    }
}

#[cfg(test)]
mod tests {

    use super::BloomFilter;

    #[test]
    fn test_filter() {
        let mut bf = BloomFilter::new(100, 0.001);
        assert_eq!(bf.slice_count, 14);
        assert_eq!(bf.bits.len(), 32);
        for i in 0..100 {
            let exist = bf.contains(i);
            assert!(!exist);
            bf.put(i);
        }
        bf.reset();
        for i in 0..40 {
            let exist = bf.contains(i);
            assert!(!exist);
            bf.put(i);
        }
        // test exists
        for i in 0..40 {
            let exist = bf.contains(i);
            assert!(exist);
        }
    }

    #[test]
    fn test_filter_edge_cases() {
        // Test with zero insertions
        let mut bf = BloomFilter::new(0, 0.001);
        assert!(bf.insertions > 0);
        bf.put(1);

        // Test with invalid FPP
        let mut bf = BloomFilter::new(100, 0.0);
        bf.put(1);

        let mut bf = BloomFilter::new(100, 1.5);
        bf.put(1);

        // Test with large keys
        let mut bf = BloomFilter::new(100, 0.001);
        bf.put(u64::MAX);
        assert!(bf.contains(u64::MAX));
    }

    #[test]
    fn test_filter_bounds() {
        let mut bf = BloomFilter::new(100, 0.001);

        // These should not panic
        let _ = bf.contains(0);
        let _ = bf.contains(u64::MAX);

        bf.put(0);
        bf.put(u64::MAX);

        assert!(bf.contains(0));
        assert!(bf.contains(u64::MAX));
    }
}
