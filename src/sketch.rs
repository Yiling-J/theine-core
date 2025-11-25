//! Count-Min Sketch probabilistic counter for frequency estimation.
//!
//! The Count-Min Sketch is a probabilistic data structure that estimates the frequency
//! of elements in a stream. It provides O(1) insertion and lookup with controlled
//! memory usage and accuracy via the false positive parameter.

const RESET_MASK: u64 = 0x7777777777777777;
const ONE_MASK: u64 = 0x1111111111111111;

/// A Count-Min Sketch data structure for frequency estimation.
///
/// This implementation uses a 2D array of 4-bit counters to track item frequencies
/// with automatic reduction when reaching sample capacity to control accuracy.
///
/// # Thread Safety
///
/// `CountMinSketch` is not thread-safe. Wrap it in a `Mutex` when sharing across threads.
#[derive(Debug)]
pub struct CountMinSketch {
    block_mask: usize,
    table: Vec<u64>,
    additions: usize,
    pub sample_size: usize,
}

impl CountMinSketch {
    /// Creates a new Count-Min Sketch with the specified size.
    ///
    /// # Arguments
    ///
    /// * `size` - Desired sketch size. Will be adjusted to at least 64 and next power of two.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let sketch = CountMinSketch::new(10000);
    /// ```
    pub fn new(size: usize) -> Self {
        let sketch_size = size.max(64);
        let counter_size = sketch_size.next_power_of_two();

        if counter_size > 1 << 20 {
            log::warn!(
                "CountMinSketch: counter_size {} is very large, may cause memory issues",
                counter_size
            );
        }

        let block_mask = (counter_size >> 3).saturating_sub(1);
        let table = vec![0; counter_size];
        let sample_size = counter_size.saturating_mul(10);

        log::debug!(
            "CountMinSketch created: size={}, counter_size={}, block_mask={}, sample_size={}",
            size,
            counter_size,
            block_mask,
            sample_size
        );

        Self {
            additions: 0,
            sample_size,
            table,
            block_mask,
        }
    }

    /// Computes the table index and counter offset for a given hash and block.
    ///
    /// # Arguments
    ///
    /// * `counter_hash` - Rehashed value for counter selection
    /// * `block` - Block offset within the table
    /// * `offset` - Counter slot (0-3)
    ///
    /// # Returns
    ///
    /// `(table_index, counter_offset)` pair for accessing the appropriate counter
    #[inline]
    fn index_of(&self, counter_hash: u64, block: u64, offset: u8) -> (usize, usize) {
        if offset > 3 {
            log::warn!("CountMinSketch: offset {} out of range [0-3]", offset);
            return (0, 0);
        }

        let h = counter_hash >> (offset << 3);
        let index = block
            .saturating_add(h & 1)
            .saturating_add((offset as u64) << 1);

        let table_len = self.table.len();
        let index_safe = if index as usize >= table_len {
            log::warn!(
                "CountMinSketch: computed index {} exceeds table length {}",
                index,
                table_len
            );
            0
        } else {
            index as usize
        };

        let offset_val = (h >> 1 & 0xf) as usize;
        (index_safe, offset_val)
    }

    /// Increments a counter if it hasn't reached the maximum value (15).
    ///
    /// # Arguments
    ///
    /// * `index` - Table index
    /// * `offset` - Counter slot offset within the u64
    ///
    /// # Returns
    ///
    /// `true` if the counter was successfully incremented, `false` if at max
    #[inline]
    fn inc(&mut self, index: usize, offset: usize) -> bool {
        if index >= self.table.len() {
            log::error!(
                "CountMinSketch: index {} out of bounds [0-{})",
                index,
                self.table.len()
            );
            return false;
        }

        if offset > 15 {
            log::warn!("CountMinSketch: offset {} out of range [0-15]", offset);
            return false;
        }

        let offset_bits = offset << 2;
        let mask = 0xF << offset_bits;

        if self.table[index] & mask != mask {
            self.table[index] = self.table[index].saturating_add(1 << offset_bits);
            return true;
        }
        false
    }

    /// Adds a hash value to the sketch, incrementing its frequency counters.
    ///
    /// # Arguments
    ///
    /// * `h` - Hash value to add
    pub fn add(&mut self, h: u64) {
        let counter_hash = rehash(h);
        let block_hash = h;
        let block = (block_hash & (self.block_mask as u64)).saturating_mul(8);

        let indices: [(usize, usize); 4] = [
            self.index_of(counter_hash, block, 0),
            self.index_of(counter_hash, block, 1),
            self.index_of(counter_hash, block, 2),
            self.index_of(counter_hash, block, 3),
        ];

        let added = indices.iter().any(|(idx, offset)| self.inc(*idx, *offset));

        if added {
            self.additions = self.additions.saturating_add(1);
            if self.additions >= self.sample_size {
                self.reset();
            }
        }
    }

    /// Reduces all counters by half to prevent overflow and decay old estimates.
    fn reset(&mut self) {
        let count: usize = self
            .table
            .iter_mut()
            .map(|cell| {
                let ones = (*cell & ONE_MASK).count_ones() as usize;
                *cell = (*cell >> 1) & RESET_MASK;
                ones
            })
            .sum();

        self.additions = self.additions.saturating_sub(count >> 2) >> 1;
        log::debug!("CountMinSketch reset: additions={}", self.additions);
    }

    /// Reads a counter value at the specified position.
    ///
    /// # Arguments
    ///
    /// * `h` - Rehashed counter value
    /// * `block` - Block offset
    /// * `offset` - Counter slot (0-3)
    ///
    /// # Returns
    ///
    /// The current counter value (0-15)
    #[inline]
    fn count(&self, h: u64, block: u64, offset: u8) -> usize {
        let (index, offset) = self.index_of(h, block, offset);

        if index >= self.table.len() || offset > 15 {
            return 0;
        }

        let offset_bits = offset << 2;
        ((self.table[index] >> offset_bits) & 0xF) as usize
    }

    /// Estimates the frequency of a hash value.
    ///
    /// Uses the minimum of all 4 counter values as the estimate
    /// (the "min trick" of Count-Min Sketch).
    ///
    /// # Arguments
    ///
    /// * `h` - Hash value to estimate frequency for
    ///
    /// # Returns
    ///
    /// Estimated frequency (conservative estimate)
    #[must_use]
    #[inline]
    pub fn estimate(&self, h: u64) -> usize {
        let counter_hash = rehash(h);
        let block_hash = h;
        let block = (block_hash & (self.block_mask as u64)).saturating_mul(8);

        [
            self.count(counter_hash, block, 0),
            self.count(counter_hash, block, 1),
            self.count(counter_hash, block, 2),
            self.count(counter_hash, block, 3),
        ]
        .iter()
        .min()
        .copied()
        .unwrap_or(0)
    }

    #[cfg(test)]
    fn table_counters(&self) -> Vec<Vec<i32>> {
        self.table
            .iter()
            .map(|&val| uint64_to_base10_slice(val))
            .collect()
    }
}

/// Applies a secondary hash to improve distribution.
///
/// Uses the MurmurHash3 finalizer to redistribute bits across the counter table.
#[inline]
fn rehash(h: u64) -> u64 {
    let h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

#[cfg(test)]
fn uint64_to_base10_slice(n: u64) -> Vec<i32> {
    (0..16)
        .rev()
        .map(|i| ((n >> (i * 4)) & 0xF) as i32)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ahash::RandomState;

    use super::CountMinSketch;

    #[test]
    fn test_sketch() {
        let mut sketch = CountMinSketch::new(10000);
        assert_eq!(sketch.table.len(), 16384);
        assert_eq!(sketch.block_mask, 2047);
        assert_eq!(sketch.sample_size, 163840);

        let hasher = RandomState::with_seeds(9, 0, 7, 2);
        let mut failed = 0;
        for i in 0..8000 {
            let key = format!("foo:bar:{}", i);
            let h = hasher.hash_one(key);
            sketch.add(h);
            sketch.add(h);
            sketch.add(h);
            sketch.add(h);
            sketch.add(h);
            let keyb = format!("foo:bar:{}:b", i);
            let h2 = hasher.hash_one(keyb);
            sketch.add(h2);
            sketch.add(h2);
            sketch.add(h2);

            let es1 = sketch.estimate(h);
            let es2 = sketch.estimate(h2);
            if es1 != 5 {
                failed += 1
            }
            if es2 != 3 {
                failed += 1
            }
            assert!(es1 >= 5);
            assert!(es2 >= 3);
        }
        assert!(failed < 40);
    }

    #[test]
    fn test_sketch_reset_counter() {
        let mut sketch = CountMinSketch::new(1000);
        for i in sketch.table.iter_mut() {
            *i = !0;
        }
        sketch.additions = 100000;
        let hasher = RandomState::with_seeds(9, 0, 7, 2);
        let h = hasher.hash_one("foo");
        assert_eq!(sketch.estimate(h), 15);
        sketch.reset();
        assert_eq!(sketch.estimate(h), 7);

        for i in sketch.table_counters().iter() {
            for c in i.iter() {
                assert_eq!(*c, 7);
            }
        }
    }

    #[test]
    fn test_sketch_reset_addition() {
        let mut sketch = CountMinSketch::new(500);
        let hasher = RandomState::with_seeds(9, 0, 7, 2);
        let mut counts = HashMap::new();
        for i in 0..5 {
            let key = format!("foo:bar:{}", i);
            let h = hasher.hash_one(key);
            sketch.add(h);
            sketch.add(h);
            sketch.add(h);
            sketch.add(h);
            sketch.add(h);
            let keyb = format!("foo:bar:{}:b", i);
            let h2 = hasher.hash_one(keyb);
            sketch.add(h2);
            sketch.add(h2);
            sketch.add(h2);

            let es1 = sketch.estimate(h);
            let es2 = sketch.estimate(h2);
            counts.insert(h, es1);
            counts.insert(h2, es2);
        }
        let total_before = sketch.additions;
        let mut diff = 0;
        sketch.reset();
        for i in 0..5 {
            let key = format!("foo:bar:{}", i);
            let h = hasher.hash_one(key);
            let keyb = format!("foo:bar:{}:b", i);
            let h2 = hasher.hash_one(keyb);

            let es1 = sketch.estimate(h);
            let es2 = sketch.estimate(h2);
            let es1_prev = counts.get(&h).copied().unwrap_or(0);
            let es2_prev = counts.get(&h2).copied().unwrap_or(0);
            diff += es1_prev - es1;
            diff += es2_prev - es2;

            assert_eq!(es1, es1_prev / 2_usize);
            assert_eq!(es2, es2_prev / 2_usize);
        }

        assert_eq!(total_before - sketch.additions, diff);
    }

    #[test]
    fn test_sketch_heavy_hitters() {
        let mut sketch = CountMinSketch::new(512);
        let hasher = RandomState::with_seeds(9, 0, 7, 2);

        for i in 100..100000 {
            let h = hasher.hash_one(format!("k:{}", i));
            sketch.add(h);
        }

        for i in (0..10).step_by(2) {
            for _ in 0..i {
                let h = hasher.hash_one(format!("k:{}", i));
                sketch.add(h);
            }
        }

        // A perfect popularity count yields an array [0, 0, 2, 0, 4, 0, 6, 0, 8, 0]
        let mut popularity = [0; 10];
        for i in 0..10 {
            let h = hasher.hash_one(format!("k:{}", i));
            popularity[i] = sketch.estimate(h) as i32;
        }

        for (i, &pop_count) in popularity.iter().enumerate() {
            if [0, 1, 3, 5, 7, 9].contains(&i) {
                assert!(pop_count <= popularity[2]);
            } else if i == 2 {
                assert!(popularity[2] <= popularity[4]);
            } else if i == 4 {
                assert!(popularity[4] <= popularity[6]);
            } else if i == 6 {
                assert!(popularity[6] <= popularity[8]);
            }
        }
    }

    #[test]
    fn test_sketch_edge_cases() {
        // Test with size 0
        let mut sketch = CountMinSketch::new(0);
        assert!(sketch.table.len() >= 64);
        sketch.add(1);
        let _ = sketch.estimate(1);

        // Test with very small size
        let mut sketch = CountMinSketch::new(1);
        assert!(sketch.table.len() >= 64);
        sketch.add(1);

        // Test with large hash values
        let mut sketch = CountMinSketch::new(1000);
        sketch.add(u64::MAX);
        sketch.add(0);
        let _ = sketch.estimate(u64::MAX);
        let _ = sketch.estimate(0);
    }
}
