pub struct CountMinSketch {
    row_counter_size: usize,
    row_64_size: usize,
    row_mask: usize,
    table: Vec<u64>,
    additions: usize,
    sample_size: usize,
}

impl CountMinSketch {
    pub fn new(size: usize) -> CountMinSketch {
        let row_counter_size = ((size * 3) as u64).next_power_of_two() as usize;
        let row_64_size = row_counter_size / 16;
        let row_mask = row_counter_size - 1;
        // each u64 contains 16 counters, so vec size is (row_counter_size * 4 / 16)
        let table = vec![0; row_counter_size >> 2];
        CountMinSketch {
            additions: 0,
            sample_size: 10 * row_counter_size,
            table,
            row_mask,
            row_64_size,
            row_counter_size,
        }
    }

    fn index_of(&self, h: u64, offset: u8) -> (usize, usize) {
        let hn = h + (offset as u64) * (h >> 32);
        let i = (hn & (self.row_mask as u64)) as usize;
        let index = offset as usize * self.row_64_size + (i >> 4);
        let offset = (i & 0xF) << 2;
        (index, offset)
    }

    fn inc(&mut self, index: usize, offset: usize) -> bool {
        let mask = 0xF << offset;
        if self.table[index] & mask != mask {
            self.table[index] += 1 << offset;
            return true;
        }
        false
    }

    pub fn add(&mut self, h: u64) {
        let (index0, offset0) = self.index_of(h, 0);
        let (index1, offset1) = self.index_of(h, 1);
        let (index2, offset2) = self.index_of(h, 2);
        let (index3, offset3) = self.index_of(h, 3);

        let mut added: bool;
        added = self.inc(index0, offset0);
        added |= self.inc(index1, offset1);
        added |= self.inc(index2, offset2);
        added |= self.inc(index3, offset3);

        if added {
            self.additions += 1;
            if self.additions == self.sample_size {
                self.reset()
            }
        }
    }

    fn reset(&mut self) {
        let _ = self.table.iter().map(|x| x >> 1);
        self.additions >>= 1;
    }

    fn count(&self, h: u64, offset: u8) -> usize {
        let (index, offset) = self.index_of(h, offset);
        let count = (self.table[index] >> offset) & 0xF;
        count as usize
    }

    pub fn estimate(&self, h: u64) -> usize {
        let count0 = self.count(h, 0);
        let count1 = self.count(h, 1);
        let count2 = self.count(h, 2);
        let count3 = self.count(h, 3);
        let s = [count0, count1, count2, count3];
        let min = s.iter().min().unwrap();
        *min
    }
}

#[cfg(test)]
mod tests {
    use ahash::RandomState;

    use super::CountMinSketch;

    #[test]
    fn test_sketch() {
        let mut sketch = CountMinSketch::new(100);
        // 512 counters per row, 2048 bits per row, 32 uint64 per row
        assert_eq!(sketch.row_counter_size, 512);
        assert_eq!(sketch.row_mask, 511);
        // 32 uint64 * 4 rows
        assert_eq!(sketch.table.len(), 128);
        assert_eq!(sketch.sample_size, 5120);

        let hasher = RandomState::new();
        let mut failed = 0;
        for i in 0..500 {
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
            if es2 > es1 {
                failed += 1
            }
            assert!(es1 >= 5);
            assert!(es2 >= 3);
        }
        assert!(failed as f64 / 4000.0 < 0.1);
        assert!(sketch.additions > 3900);
        let a = sketch.additions;

        sketch.reset();
        assert_eq!(sketch.additions, a >> 1);
    }
}
