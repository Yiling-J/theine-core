use ahash::RandomState;
use pyo3::prelude::*;

#[pyclass]
pub struct BloomFilter {
    insertions: usize,
    bits_mask: usize,
    slice_count: usize,
    bits: Vec<u64>,
    additions: usize,
    hasher: RandomState,
}

#[pymethods]
impl BloomFilter {
    #[new]
    fn new(insertions: usize, fpp: f64) -> Self {
        let ln2 = 2f64.ln();
        let factor = -fpp.ln() / (ln2 * ln2);
        let mut bits = ((insertions as f64 * factor) as usize).next_power_of_two();
        if bits == 0 {
            bits = 1
        }
        Self {
            insertions,
            bits_mask: bits - 1,
            slice_count: (ln2 * bits as f64 / insertions as f64) as usize,
            bits: vec![0; (bits + 63) / 64],
            additions: 0,
            hasher: RandomState::new(),
        }
    }

    pub fn put(&mut self, key: &str) {
        let h = self.hasher.hash_one(key);
        self.additions += 1;
        if self.additions == self.insertions {
            self.reset();
        }
        for i in 0..self.slice_count {
            let hash = h + (i as u64) * (h >> 32);
            self.set(hash & self.bits_mask as u64);
        }
    }

    fn get(&self, h: u64) -> bool {
        let idx = h >> 6;
        let offset = h & 63;
        let mask = 1u64 << offset;
        let val = self.bits[idx as usize];
        ((val & mask) >> offset) != 0
    }

    fn set(&mut self, h: u64) {
        let idx = h >> 6;
        let offset = h & 63;
        let mask = 1u64 << offset;
        self.bits[idx as usize] |= mask;
    }

    pub fn contains(&self, key: &str) -> bool {
        let h = self.hasher.hash_one(key);
        let mut o = true;
        for i in 0..self.slice_count {
            let hash = h + i as u64 * (h >> 32);
            o &= self.get(hash & self.bits_mask as u64);
        }
        o
    }

    fn reset(&mut self) {
        self.bits = vec![0; self.bits.len()];
        self.additions = 0;
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
            let exist = bf.contains(&format!("key:{}", i));
            assert!(!exist);
            bf.put(&format!("key:{}", i));
        }
        bf.reset();
        for i in 0..40 {
            let exist = bf.contains(&format!("key:{}", i));
            assert!(!exist);
            bf.put(&format!("key:{}", i));
        }
        // test exists
        for i in 0..40 {
            let exist = bf.contains(&format!("key:{}", i));
            assert!(exist);
        }
    }
}
