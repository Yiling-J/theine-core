use crate::lru::Lru;
use crate::lru::Slru;
use crate::metadata::Entry;
use crate::sketch::CountMinSketch;
use crate::timerwheel::Clock;
use rand::Rng;
use std::collections::HashMap;

const ADMIT_HASHDOS_THRESHOLD: usize = 6;
const HILL_CLIMBER_STEP_DECAY_RATE: f32 = 0.98;
const HILL_CLIMBER_STEP_PERCENT: f32 = 0.0625;

#[derive(PartialEq)]
enum PolicyList {
    ListWindow,
    ListProbation,
    ListProtected,
}

pub struct TinyLfu {
    size: usize,
    capacity: usize,
    window: Lru,
    main: Slru,
    pub sketch: CountMinSketch,
    hit_in_sample: usize,
    misses_in_sample: usize,
    hr: f32,
    step: f32,
    amount: isize,
}

impl TinyLfu {
    pub fn new(size: usize) -> TinyLfu {
        let mut lru_size = (size as f64 * 0.01) as usize;
        if lru_size == 0 {
            lru_size = 1;
        }
        let slru_size = size - lru_size;
        TinyLfu {
            size: 0,
            capacity: size,
            window: Lru::new(lru_size),
            main: Slru::new(slru_size),
            sketch: CountMinSketch::new(size),
            hit_in_sample: 0,
            misses_in_sample: 0,
            hr: 0.0,
            step: 0.0,
            amount: 0,
        }
    }

    fn increase_window(&mut self, amount: isize, entries: &mut HashMap<u64, Entry>) -> isize {
        let mut amount = amount;

        // try move from protected/probation to window
        loop {
            let mut key = self.main.probation.tail();
            if key.is_none() {
                key = self.main.protected.tail()
            }
            if key.is_none() {
                break;
            }
            if amount <= 0 {
                break;
            }
            amount -= 1;
            if let Some(entry) = entries.get_mut(&key.unwrap()) {
                self.main.remove(entry);
            }
        }
        amount
    }

    fn decrease_window(&mut self, amount: isize, entries: &mut HashMap<u64, Entry>) -> isize {
        let mut amount = amount;

        // try move from window to probation
        loop {
            let key = self.window.list.tail();
            if key.is_none() {
                break;
            }
            if amount <= 0 {
                break;
            }
            amount -= 1;
            let kk = *key.unwrap();
            if let Some(entry) = entries.get_mut(&kk) {
                self.window.remove(entry);
                self.main.insert(kk, entry);
            }
        }
        amount
    }

    // move entry from protected to probation
    fn demote_from_protected(&mut self, entries: &mut HashMap<u64, Entry>) {
        while self.main.protected.len() > self.main.protected.capacity {
            if let Some(key) = self.main.protected.pop_tail() {
                if let Some(entry) = entries.get_mut(&key) {
                    self.main.insert(key, entry);
                }
            }
        }
    }

    fn resize_window(&mut self, entries: &mut HashMap<u64, Entry>) {
        self.window.list.capacity = self.window.list.capacity.saturating_add_signed(self.amount);
        self.main.protected.capacity = self
            .main
            .protected
            .capacity
            .saturating_add_signed(-self.amount);
        // demote first to make sure policy size is right
        self.demote_from_protected(entries);

        let remain;
        if self.amount > 0 {
            remain = self.increase_window(self.amount, entries);
            self.amount = remain;
        } else if self.amount < 0 {
            remain = self.decrease_window(-self.amount, entries);
            self.amount = -remain;
        }
        self.window.list.capacity = self
            .window
            .list
            .capacity
            .saturating_add_signed(-self.amount);
        self.main.protected.capacity = self
            .main
            .protected
            .capacity
            .saturating_add_signed(self.amount);
    }

    fn climb(&mut self) {
        let delta;

        if self.hit_in_sample + self.misses_in_sample == 0 {
            delta = 0.0;
        } else {
            let sample_hr = self.hit_in_sample as f32 / self.misses_in_sample as f32;
            delta = sample_hr - self.hr;
            self.hr = sample_hr;
        }

        let amount;
        if delta > 0.0 {
            amount = self.step;
        } else {
            amount = -self.step;
        }

        let mut next_step_size = amount * HILL_CLIMBER_STEP_DECAY_RATE;
        if delta.abs() >= 0.05 {
            let next_step_size_abs = self.size as f32 * HILL_CLIMBER_STEP_PERCENT;
            if amount >= 0.0 {
                next_step_size = next_step_size_abs;
            } else {
                next_step_size = -next_step_size_abs;
            }
        }
        self.step = next_step_size;
        self.amount = amount as isize;

        // decrease protected, min protected is 0
        if self.amount > 0 && self.amount as usize > (self.window.list.capacity - 1) {
            self.amount = self.window.list.capacity as isize;
        }

        if self.amount < 0 && self.amount.abs() as usize > (self.window.list.capacity - 1) {
            self.amount = -((self.window.list.capacity - 1) as isize);
        }
    }

    // add/update key
    pub fn set(&mut self, key: u64, entries: &mut HashMap<u64, Entry>) -> Option<u64> {
        if self.hit_in_sample + self.misses_in_sample > self.sketch.sample_size {
            self.climb();
            self.resize_window(entries);
        }

        if let Some(entry) = entries.get_mut(&key) {
            // new entry
            if entry.policy_list_id == 0 {
                self.misses_in_sample += 1;
                self.window.insert(key, entry);
            }
        }

        self.demote_from_protected(entries);
        self.evict_entries(entries);

        None
    }

    /// Mark access, update sketch and lru/slru
    pub fn access(&mut self, key: u64, clock: &Clock, entries: &mut HashMap<u64, Entry>) {
        if self.hit_in_sample + self.misses_in_sample > self.sketch.sample_size {
            self.climb();
            self.resize_window(entries);
        }
        self.sketch.add(key);

        if let Some(entry) = entries.get_mut(&key) {
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
        self.size
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

    fn evict_from_window(&mut self, entries: &mut HashMap<u64, Entry>) -> Option<u64> {
        let mut first = None;
        while self.window.len() > self.window.list.capacity {
            if let Some(evicted) = self.window.list.pop_tail() {
                if first.is_none() {
                    first = Some(evicted);
                }
                if let Some(entry) = entries.get_mut(&evicted) {
                    self.main.insert(evicted, entry);
                }
            }
        }
        first
    }

    // comapre and evict entries until cache size fit.
    // candidate is the first entry evicted from window,
    // if head is null, start from last entry from window.
    fn evict_from_main(&mut self, candidate: Option<u64>, entries: &mut HashMap<u64, Entry>) {
        let mut victim_queue = PolicyList::ListProbation;
        let mut candidate_queue = PolicyList::ListProbation;
        let mut victim = self.main.probation.tail().copied();
        let mut candidate = candidate;

        while self.size > self.capacity {
            if candidate.is_none() && candidate_queue == PolicyList::ListProbation {
                candidate = self.window.list.tail().copied();
                candidate_queue = PolicyList::ListWindow;
            }

            if candidate.is_none() && victim.is_none() {
                if victim_queue == PolicyList::ListProbation {
                    victim = self.main.protected.tail().copied();
                    victim_queue = PolicyList::ListProtected;
                    continue;
                } else if victim_queue == PolicyList::ListProtected {
                    victim = self.window.list.tail().copied();
                    victim_queue = PolicyList::ListWindow;
                    continue;
                }
            }

            if victim.is_none() {
                let prev = self.prev_key(candidate, entries);
                let evict = candidate;
                candidate = prev;
                if let Some(key) = evict {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                    }
                }
                continue;
            } else if candidate.is_none() {
                let evict = victim;
                victim = self.prev_key(victim, entries);
                if let Some(key) = evict {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                    }
                }
                continue;
            }

            if victim == candidate {
                victim = self.prev_key(victim, entries);
                if let Some(key) = candidate {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                    }
                }
                candidate = None;
                continue;
            }

            if self.admit(candidate.unwrap(), victim.unwrap()) {
                let evict = victim;
                victim = self.prev_key(victim, entries);
                if let Some(key) = evict {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                    }
                }
                candidate = self.prev_key(candidate, entries);
            } else {
                let evict = candidate;
                candidate = self.prev_key(candidate, entries);
                if let Some(key) = evict {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                    }
                }
            }
        }
    }

    fn prev_key(&self, key: Option<u64>, entries: &mut HashMap<u64, Entry>) -> Option<u64> {
        if let Some(entry) = entries.get(&key.unwrap()) {
            let list;
            match entry.policy_list_id {
                1 => list = &self.window.list,
                2 => list = &self.main.probation,
                3 => list = &self.main.protected,
                _ => unreachable!(),
            };
            list.prev(entry.policy_list_index.unwrap()).copied()
        } else {
            None
        }
    }

    fn evict_entries(&mut self, entries: &mut HashMap<u64, Entry>) {
        let first = self.evict_from_window(entries);
        self.evict_from_main(first, entries);
    }

    fn admit(&self, candidate: u64, victim: u64) -> bool {
        let victim_freq = self.sketch.estimate(victim);
        let candidate_freq = self.sketch.estimate(candidate);
        if candidate_freq > victim_freq {
            true
        } else if candidate_freq > ADMIT_HASHDOS_THRESHOLD {
            rand::thread_rng().gen::<i32>() & 127 == 0
        } else {
            false
        }
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
