use pyo3::prelude::*;

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
        }
    }

    pub fn put(&mut self, key: u64) {
        self.additions += 1;
        if self.additions == self.insertions {
            self.reset();
        }
        for i in 0..self.slice_count {
            let hash = key + (i as u64) * (key >> 32);
            self.set(hash & self.bits_mask as u64);
        }
    }

    fn get(&self, key: u64) -> bool {
        let idx = key >> 6;
        let offset = key & 63;
        let mask = 1u64 << offset;
        let val = self.bits[idx as usize];
        ((val & mask) >> offset) != 0
    }

    fn set(&mut self, key: u64) {
        let idx = key >> 6;
        let offset = key & 63;
        let mask = 1u64 << offset;
        self.bits[idx as usize] |= mask;
    }

    pub fn contains(&self, key: u64) -> bool {
        let mut o = true;
        for i in 0..self.slice_count {
            let hash = key + i as u64 * (key >> 32);
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
}
