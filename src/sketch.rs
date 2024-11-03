const RESET_MASK: u64 = 0x7777777777777777;
const ONE_MASK: u64 = 0x1111111111111111;

pub struct CountMinSketch {
    block_mask: usize,
    table: Vec<u64>,
    additions: usize,
    sample_size: usize,
}

impl CountMinSketch {
    pub fn new(size: usize) -> CountMinSketch {
        let mut sketch_size = size;
        if sketch_size < 64 {
            sketch_size = 64;
        }
        let counter_size = sketch_size.next_power_of_two();
        let block_mask = (counter_size >> 3) - 1;
        let table = vec![0; counter_size];
        CountMinSketch {
            additions: 0,
            sample_size: 10 * counter_size,
            table,
            block_mask,
        }
    }

    fn index_of(&self, counter_hash: u64, block: u64, offset: u8) -> (usize, usize) {
        let h = counter_hash >> (offset << 3);
        let index = block + (h & 1) + (offset << 1) as u64;
        (index as usize, (h >> 1 & 0xf) as usize)
    }

    fn inc(&mut self, index: usize, offset: usize) -> bool {
        let offset = offset << 2;
        let mask = 0xF << offset;
        if self.table[index] & mask != mask {
            self.table[index] += 1 << offset;
            return true;
        }
        false
    }

    pub fn add(&mut self, h: u64) {
        let counter_hash = rehash(h);
        let block_hash = h;
        let block = (block_hash & (self.block_mask as u64)) << 3;
        let (index0, offset0) = self.index_of(counter_hash, block, 0);
        let (index1, offset1) = self.index_of(counter_hash, block, 1);
        let (index2, offset2) = self.index_of(counter_hash, block, 2);
        let (index3, offset3) = self.index_of(counter_hash, block, 3);

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
        let mut count = 0;

        for i in self.table.iter_mut() {
            count += (*i & ONE_MASK).count_ones();
            *i = (*i >> 1) & RESET_MASK;
        }

        self.additions = (self.additions - ((count >> 2) as usize)) >> 1;
    }

    fn count(&self, h: u64, block: u64, offset: u8) -> usize {
        let (index, offset) = self.index_of(h, block, offset);
        let offset = offset << 2;
        let count = (self.table[index] >> offset) & 0xF;
        count as usize
    }

    pub fn estimate(&self, h: u64) -> usize {
        let counter_hash = rehash(h);
        let block_hash = h;
        let block = (block_hash & (self.block_mask as u64)) << 3;
        let count0 = self.count(counter_hash, block, 0);
        let count1 = self.count(counter_hash, block, 1);
        let count2 = self.count(counter_hash, block, 2);
        let count3 = self.count(counter_hash, block, 3);
        let s = [count0, count1, count2, count3];
        let min = s.iter().min().unwrap();
        *min
    }

    #[cfg(test)]
    fn table_counters(&self) -> Vec<Vec<i32>> {
        self.table
            .iter()
            .map(|&val| uint64_to_base10_slice(val))
            .collect()
    }
}

fn rehash(h: u64) -> u64 {
    let mut h = h.wrapping_mul(0x94d049bb133111eb);
    h ^= h >> 31;
    h
}

#[cfg(test)]
fn uint64_to_base10_slice(n: u64) -> Vec<i32> {
    let mut result = vec![0; 16];
    for i in 0..16 {
        result[15 - i] = ((n >> (i * 4)) & 0xF) as i32;
    }
    result
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        hash::{BuildHasher, RandomState},
    };

    use super::CountMinSketch;

    #[test]
    fn test_sketch() {
        let mut sketch = CountMinSketch::new(10000);
        assert_eq!(sketch.table.len(), 16384);
        assert_eq!(sketch.block_mask, 2047);
        assert_eq!(sketch.sample_size, 163840);

        let hasher = RandomState::new();
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
        let hasher = RandomState::new();
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
        let hasher = RandomState::new();
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
            let es1_prev = *counts.get(&h).unwrap();
            let es2_prev = *counts.get(&h2).unwrap();
            diff += es1_prev - es1;
            diff += es2_prev - es2;

            assert_eq!(es1, es1_prev / 2 as usize);
            assert_eq!(es2, es2_prev / 2 as usize);
        }

        assert_eq!(total_before - sketch.additions, diff);
    }

    #[test]
    fn test_sketch_heavy_hitters() {
        let mut sketch = CountMinSketch::new(512);
        let hasher = RandomState::new();

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
        let mut popularity = vec![0; 10];
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
}
