use crate::lru::{Lru, Slru};
use crate::metadata::MetaData;
use crate::policy::Policy;
use crate::sketch::CountMinSketch;
use crate::timerwheel::Clock;
use ahash::RandomState;

pub struct TinyLfu {
    size: usize,
    lru: Lru,
    slru: Slru,
    pub sketch: CountMinSketch,
    hasher: RandomState,
    lru_factor: usize,
    total: usize,
    hit: usize,
    hr: f32,
    step: i8,
}

impl Policy for TinyLfu {
    // remove key
    fn remove(&mut self, index: u32, metadata: &mut MetaData) {
        let entry = &mut metadata.data[index as usize];
        match entry.link_id {
            0 => (),
            1 => self.lru.remove(index, metadata),
            2 | 3 => self.slru.remove(index, metadata),
            _ => unreachable!(),
        };
    }
}

impl TinyLfu {
    pub fn new(size: usize, metadata: &mut MetaData) -> TinyLfu {
        let mut lru_size = (size as f64 * 0.01) as usize;
        if lru_size == 0 {
            lru_size = 1;
        }
        let slru_size = size - lru_size;
        TinyLfu {
            size,
            lru: Lru::new(lru_size, metadata),
            slru: Slru::new(slru_size, metadata),
            sketch: CountMinSketch::new(size),
            hasher: RandomState::new(),
            lru_factor: 0,
            total: 0, // total since last climbing
            hit: 0,   // hit since last climbing
            hr: 0.0,  // last hit ratio
            step: 1,
        }
    }

    // add/update key
    pub fn set(&mut self, index: u32, metadata: &mut MetaData) -> Option<u32> {
        // hill climbing lru factor
        if self.total >= 10 * self.size && (self.total - self.hit) > self.size / 2 {
            let current = self.hit as f32 / self.total as f32;
            let delta = current - self.hr;
            if delta > 0.0 {
                if self.step.is_negative() {
                    self.step -= 1;
                } else {
                    self.step += 1
                }
                self.step = self.step.clamp(-13, 13);
                let new_factor = self.lru_factor as isize + self.step as isize;
                self.lru_factor = new_factor.clamp(0, 13) as usize;
            } else if delta < 0.0 {
                // reset
                if self.step.is_positive() {
                    self.step = -1;
                } else {
                    self.step = 1
                }
                let new_factor = self.lru_factor as isize + self.step as isize;
                self.lru_factor = new_factor.clamp(0, 13) as usize;
            }
            self.hr = current;
            self.hit = 0;
            self.total = 0;
        }

        let entry = &mut metadata.data[index as usize];
        // new entry
        if entry.link_id == 0 {
            if let Some(evicted) = self.lru.insert(index, metadata) {
                if let Some(victim) = self.slru.victim(metadata) {
                    let ekey = metadata.data[evicted as usize].key.to_string();
                    let vkey = metadata.data[victim as usize].key.to_string();
                    let evicted_count =
                        self.sketch.estimate(self.hasher.hash_one(ekey)) + self.lru_factor;
                    let victim_count = self.sketch.estimate(self.hasher.hash_one(vkey));
                    if evicted_count <= victim_count {
                        return Some(evicted);
                    }
                }
                // reinsert evicted one from lru to slru
                if let Some(evicted_new) = self.slru.insert(evicted, metadata) {
                    return Some(evicted_new);
                }
            }
        }
        None
    }

    /// Mark access, update sketch and lru/slru
    pub fn access(&mut self, key: &str, clock: &Clock, metadata: &mut MetaData) -> Option<u32> {
        self.sketch.add(self.hasher.hash_one(key.to_string()));
        self.total += 1;
        if let Some(index) = metadata.get(key) {
            self.hit += 1;
            let entry = &metadata.data[index as usize];
            if entry.expire != 0 && entry.expire <= clock.now_ns() {
                return None;
            }
            let link_id = metadata.data[index as usize].link_id;
            match link_id {
                1 => self.lru.access(index, metadata),
                2 | 3 => self.slru.access(index, metadata),
                _ => unreachable!(),
            }
            return Some(index);
        }
        None
    }

    /// Current length of policy(lru + slru)
    pub fn len(&self) -> usize {
        self.lru.len() + self.slru.protected_len() + self.slru.probation_len()
    }
}

#[cfg(test)]
mod tests {
    use crate::{metadata::MetaData, timerwheel::Clock};

    use super::TinyLfu;
    use crate::policy::Policy;

    fn key_to_index(key: &str, metadata: &mut MetaData) -> u32 {
        metadata.get_or_create(key).index
    }

    #[test]
    fn test_tlfu() {
        let mut metadata = MetaData::new(1000);
        let mut tlfu = TinyLfu::new(1000, &mut metadata);
        let clock = Clock::new();
        assert_eq!(tlfu.lru.capacity(), 10);
        assert_eq!(tlfu.slru.probation_capacity(), 990);
        assert_eq!(tlfu.slru.protected_capacity(), 792);
        assert_eq!(tlfu.slru.probation_len(), 0);
        assert_eq!(tlfu.slru.protected_len(), 0);

        for i in 0..200 {
            let evicted = tlfu.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(evicted.is_none());
        }
        assert_eq!(
            "key:199key:198key:197key:196key:195key:194key:193key:192key:191key:190",
            tlfu.lru.link.display(true, &metadata)
        );
        assert_eq!(
            "key:190key:191key:192key:193key:194key:195key:196key:197key:198key:199",
            tlfu.lru.link.display(false, &metadata)
        );

        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 190);
        assert_eq!(tlfu.slru.protected_len(), 0);

        // access same key will move the key from probation to protected
        tlfu.access("key:10", &clock, &mut metadata);
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 189);
        assert_eq!(tlfu.slru.protected_len(), 1);
        assert_eq!(
            "key:199key:198key:197key:196key:195key:194key:193key:192key:191key:190",
            tlfu.lru.link.display(true, &metadata)
        );
        assert_eq!(
            "key:190key:191key:192key:193key:194key:195key:196key:197key:198key:199",
            tlfu.lru.link.display(false, &metadata)
        );
        // access again, length should be same
        tlfu.access("key:10", &clock, &mut metadata);
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 189);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // fill tlfu
        for i in 200..1000 {
            let evicted = tlfu.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(evicted.is_none());
        }
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 989);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // set again, should evicate one
        let evicted = tlfu.set(key_to_index("key:0a", &mut metadata), &mut metadata);
        // lru size is 10, and last 10 is 990-1000, so evicate 990
        assert_eq!(evicted.unwrap(), metadata.get("key:990").unwrap());
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 989);
        assert_eq!(tlfu.slru.protected_len(), 1);
        // test estimate
        let victim = tlfu.slru.victim(&mut metadata);
        assert_eq!(victim.unwrap(), metadata.get("key:0").unwrap());
        tlfu.access("key:991", &clock, &mut metadata);
        tlfu.access("key:991", &clock, &mut metadata);
        tlfu.access("key:991", &clock, &mut metadata);
        tlfu.access("key:991", &clock, &mut metadata);
        let evicted = tlfu.set(key_to_index("key:1a", &mut metadata), &mut metadata);
        assert_eq!(evicted.unwrap(), metadata.get("key:992").unwrap());
        assert_eq!(tlfu.slru.probation_len(), 989);

        for i in 0..1000 {
            tlfu.set(
                key_to_index(&format!("key:{}:b", i), &mut metadata),
                &mut metadata,
            );
        }
        assert_eq!(tlfu.lru.len(), 10);
        assert_eq!(tlfu.slru.probation_len(), 989);
        assert_eq!(tlfu.slru.protected_len(), 1);

        // test remove
        assert_eq!(
            "key:999:bkey:998:bkey:997:bkey:996:bkey:995:bkey:994:bkey:993:bkey:992:bkey:991:bkey:990:b",
            tlfu.lru.link.display(true, &metadata)
        );
        tlfu.remove(metadata.get("key:996:b").unwrap(), &mut metadata);
        assert_eq!(
            "key:999:bkey:998:bkey:997:bkey:995:bkey:994:bkey:993:bkey:992:bkey:991:bkey:990:b",
            tlfu.lru.link.display(true, &metadata)
        );
        assert_eq!(
            "key:990:bkey:991:bkey:992:bkey:993:bkey:994:bkey:995:bkey:997:bkey:998:bkey:999:b",
            tlfu.lru.link.display(false, &metadata)
        );
        for key in [
            "key:0:b",
            "key:20:b",
            "key:300:b",
            "key:500:b",
            "key:899:b",
            "key:999:b",
        ] {
            tlfu.remove(metadata.get(key).unwrap(), &mut metadata);
            tlfu.slru.probation.display(true, &metadata);
            tlfu.slru.probation.display(false, &metadata);
            tlfu.slru.protected.display(true, &metadata);
            tlfu.slru.protected.display(false, &metadata);
        }
    }

    #[test]
    fn test_tlfu_set_same() {
        let mut metadata = MetaData::new(1000);
        let mut tlfu = TinyLfu::new(1000, &mut metadata);

        for i in 0..200 {
            let evicted = tlfu.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(evicted.is_none());
        }

        for i in 0..200 {
            let evicted = tlfu.set(
                key_to_index(&format!("key:{}", i), &mut metadata),
                &mut metadata,
            );
            assert!(evicted.is_none());
        }
    }
}
