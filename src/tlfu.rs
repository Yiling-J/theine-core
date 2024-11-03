use crate::lru::Lru;
use crate::lru::Slru;
use crate::metadata::Entry;
use crate::sketch::CountMinSketch;
use crate::timerwheel::Clock;
use std::collections::HashMap;

pub struct TinyLfu {
    size: usize,
    window: Lru,
    main: Slru,
    pub sketch: CountMinSketch,
    lru_factor: usize,
    total: usize,
    hit: usize,
    hr: f32,
    step: i8,
}

impl TinyLfu {
    pub fn new(size: usize) -> TinyLfu {
        let mut lru_size = (size as f64 * 0.01) as usize;
        if lru_size == 0 {
            lru_size = 1;
        }
        let slru_size = size - lru_size;
        TinyLfu {
            size,
            window: Lru::new(lru_size),
            main: Slru::new(slru_size),
            sketch: CountMinSketch::new(size),
            lru_factor: 0,
            total: 0, // total since last climbing
            hit: 0,   // hit since last climbing
            hr: 0.0,  // last hit ratio
            step: 1,
        }
    }

    // add/update key
    pub fn set(&mut self, key: u64, entries: &mut HashMap<u64, Entry>) -> Option<u64> {
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

        if let Some(entry) = entries.get_mut(&key) {
            // new entry
            if entry.policy_list_id == 0 {
                if let Some(evicted) = self.window.insert(key, entry) {
                    if let Some(victim) = self.main.victim() {
                        let evicted_count = self.sketch.estimate(evicted) + self.lru_factor;
                        let victim_count = self.sketch.estimate(*victim);
                        if evicted_count <= victim_count {
                            return Some(evicted);
                        }
                    }

                    // reinsert evicted one from window to main
                    if let Some(entry) = entries.get_mut(&evicted) {
                        if let Some(evicted_new) = self.main.insert(evicted, entry) {
                            return Some(evicted_new);
                        }
                    }
                }
            }
        }
        None
    }

    /// Mark access, update sketch and lru/slru
    pub fn access(&mut self, key: u64, clock: &Clock, entries: &mut HashMap<u64, Entry>) {
        self.sketch.add(key);
        self.total += 1;

        if let Some(entry) = entries.get_mut(&key) {
            self.hit += 1;
            if entry.expire != 0 && entry.expire <= clock.now_ns() {
                return;
            }

            if let Some(index) = entry.policy_list_index {
                match entry.policy_list_id {
                    1 => self.window.access(index),
                    2 | 3 => self.main.access(key, entries),
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Current length of policy(lru + slru)
    pub fn len(&self) -> usize {
        self.window.len() + self.main.protected_len() + self.main.probation_len()
    }

    // remove key
    pub fn remove(&mut self, entry: &mut Entry) {
        match entry.policy_list_id {
            0 => (),
            1 => self.window.remove(entry),
            2 | 3 => self.main.remove(entry),
            _ => unreachable!(),
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{metadata::Entry, timerwheel::Clock};

    use super::TinyLfu;

    #[test]
    fn test_tlfu() {
        let mut tlfu = TinyLfu::new(1000);
        let clock = Clock::new();
        let mut entries = HashMap::new();
        assert_eq!(tlfu.window.list.capacity, 10);
        assert_eq!(tlfu.main.probation.capacity, 990);
        assert_eq!(tlfu.main.protected.capacity, 792);
        assert_eq!(tlfu.main.probation_len(), 0);
        assert_eq!(tlfu.main.protected_len(), 0);

        for i in 0..200 {
            let entry = Entry::new();
            entries.insert(i, entry);
            let evicted = tlfu.set(i, &mut entries);
            assert!(evicted.is_none());
        }

        assert_eq!(
            vec![199, 198, 197, 196, 195, 194, 193, 192, 191, 190],
            tlfu.window.list.iter().map(|i| *i).collect::<Vec<u64>>(),
        );

        assert_eq!(tlfu.window.len(), 10);
        assert_eq!(tlfu.main.probation_len(), 190);
        assert_eq!(tlfu.main.protected_len(), 0);

        // access same key will move the key from probation to protected
        tlfu.access(10, &clock, &mut entries);
        assert_eq!(tlfu.window.len(), 10);
        assert_eq!(tlfu.main.probation_len(), 189);
        assert_eq!(tlfu.main.protected_len(), 1);
        assert_eq!(
            vec![199, 198, 197, 196, 195, 194, 193, 192, 191, 190],
            tlfu.window.list.iter().map(|i| *i).collect::<Vec<u64>>(),
        );

        // access again, length should be same
        tlfu.access(10, &clock, &mut entries);
        assert_eq!(tlfu.window.len(), 10);
        assert_eq!(tlfu.main.probation_len(), 189);
        assert_eq!(tlfu.main.protected_len(), 1);
        // fill tlfu
        for i in 200..1000 {
            let entry = Entry::new();
            entries.insert(i, entry);
            let evicted = tlfu.set(i, &mut entries);
            assert!(evicted.is_none());
        }
        assert_eq!(tlfu.window.len(), 10);
        assert_eq!(tlfu.main.probation_len(), 989);
        assert_eq!(tlfu.main.protected_len(), 1);
        // set again, should evicate one
        let entry = Entry::new();
        entries.insert(9876, entry);
        let evicted = tlfu.set(9876, &mut entries);
        // lru size is 10, and last 10 is 990-1000, so evicate 990
        assert_eq!(evicted.unwrap(), 990);
        assert_eq!(tlfu.window.len(), 10);
        assert_eq!(tlfu.main.probation_len(), 989);
        assert_eq!(tlfu.main.protected_len(), 1);
        // test estimate
        let victim = tlfu.main.victim();
        assert_eq!(*victim.unwrap(), 0);

        // 991 moved to window front, and sketch is updated
        tlfu.access(991, &clock, &mut entries);
        tlfu.access(991, &clock, &mut entries);
        tlfu.access(991, &clock, &mut entries);
        tlfu.access(991, &clock, &mut entries);
        let entry = Entry::new();
        entries.insert(9877, entry);
        let evicted = tlfu.set(9877, &mut entries);
        assert_eq!(evicted.unwrap(), 992);
        assert_eq!(tlfu.main.probation_len(), 989);

        for i in 0..1000 {
            let entry = Entry::new();
            entries.insert(10000 + i, entry);
            tlfu.set(10000 + i, &mut entries);
        }
        assert_eq!(tlfu.window.len(), 10);
        assert_eq!(tlfu.main.probation_len(), 989);
        assert_eq!(tlfu.main.protected_len(), 1);
        // 991 shoud be in main cache now

        // test remove
        assert_eq!(
            vec![10999, 10998, 10997, 10996, 10995, 10994, 10993, 10992, 10991, 10990],
            tlfu.window.list.iter().map(|i| *i).collect::<Vec<u64>>(),
        );

        tlfu.remove(entries.get_mut(&10996).unwrap());
        assert_eq!(
            vec![10999, 10998, 10997, 10995, 10994, 10993, 10992, 10991, 10990],
            tlfu.window.list.iter().map(|i| *i).collect::<Vec<u64>>(),
        );
    }

    #[test]
    fn test_tlfu_set_same() {
        let mut tlfu = TinyLfu::new(1000);
        let mut entries = HashMap::new();

        for i in 0..200 {
            let evicted = tlfu.set(i, &mut entries);
            assert!(evicted.is_none());
        }

        for i in 0..200 {
            let evicted = tlfu.set(i, &mut entries);
            assert!(evicted.is_none());
        }
    }
}
