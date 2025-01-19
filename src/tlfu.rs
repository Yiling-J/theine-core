use crate::lru::Lru;
use crate::lru::Slru;
use crate::metadata::Entry;
use crate::sketch::CountMinSketch;
use crate::timerwheel::Clock;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashMap;

const ADMIT_HASHDOS_THRESHOLD: usize = 6;
const HILL_CLIMBER_STEP_DECAY_RATE: f32 = 0.98;
const HILL_CLIMBER_STEP_PERCENT: f32 = 0.0625;

#[derive(PartialEq)]
enum PolicyList {
    Window,
    Probation,
    Protected,
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
            step: -(size as f32) * 0.0625,
            amount: 0,
        }
    }

    #[cfg(test)]
    pub fn new_sized(wsize: usize, msize: usize, psize: usize) -> TinyLfu {
        let mut t = TinyLfu {
            size: 0,
            capacity: wsize + msize,
            window: Lru::new(wsize),
            main: Slru::new(msize),
            sketch: CountMinSketch::new(wsize + msize),
            hit_in_sample: 0,
            misses_in_sample: 0,
            hr: 0.0,
            step: -((wsize + msize) as f32) * 0.0625,
            amount: 0,
        };
        t.main.protected.capacity = psize;
        t
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
            let k = *key.unwrap();
            if let Some(entry) = entries.get_mut(&k) {
                self.main.remove(entry);
                self.window.insert(k, entry);
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
            let k = *key.unwrap();
            if let Some(entry) = entries.get_mut(&k) {
                self.window.remove(entry);
                self.main.insert(k, entry);
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
        match self.amount.cmp(&0) {
            Ordering::Greater => {
                remain = self.increase_window(self.amount, entries);
                self.amount = remain;
            }
            Ordering::Less => {
                remain = self.decrease_window(-self.amount, entries);
                self.amount = -remain;
            }
            _ => {}
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

        let amount = if delta > 0.0 { self.step } else { -self.step };

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

        if self.amount < 0 && self.amount.unsigned_abs() > (self.window.list.capacity - 1) {
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
                self.size += 1;
            }
        }

        self.demote_from_protected(entries);
        self.evict_entries(entries)
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
        self.size -= 1;
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
    fn evict_from_main(
        &mut self,
        candidate: Option<u64>,
        entries: &mut HashMap<u64, Entry>,
    ) -> Option<u64> {
        let mut victim_queue = PolicyList::Probation;
        let mut candidate_queue = PolicyList::Probation;
        let mut victim = self.main.probation.tail().copied();
        let mut candidate = candidate;
        let mut evicted = None;

        while self.size > self.capacity {
            if candidate.is_none() && candidate_queue == PolicyList::Probation {
                candidate = self.window.list.tail().copied();
                candidate_queue = PolicyList::Window;
            }

            if candidate.is_none() && victim.is_none() {
                if victim_queue == PolicyList::Probation {
                    victim = self.main.protected.tail().copied();
                    victim_queue = PolicyList::Protected;
                    continue;
                } else if victim_queue == PolicyList::Protected {
                    victim = self.window.list.tail().copied();
                    victim_queue = PolicyList::Window;
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
                        evicted = Some(key);
                    }
                }
                continue;
            } else if candidate.is_none() {
                let evict = victim;
                victim = self.prev_key(victim, entries);
                if let Some(key) = evict {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                        evicted = Some(key);
                    }
                }
                continue;
            }

            if victim == candidate {
                victim = self.prev_key(victim, entries);
                if let Some(key) = candidate {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                        evicted = Some(key);
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
                        evicted = Some(key);
                    }
                }
                candidate = self.prev_key(candidate, entries);
            } else {
                let evict = candidate;
                candidate = self.prev_key(candidate, entries);
                if let Some(key) = evict {
                    if let Some(entry) = entries.get_mut(&key) {
                        self.remove(entry);
                        evicted = Some(key);
                    }
                }
            }
        }
        evicted
    }

    fn prev_key(&self, key: Option<u64>, entries: &mut HashMap<u64, Entry>) -> Option<u64> {
        if let Some(entry) = entries.get(&key.unwrap()) {
            let list = match entry.policy_list_id {
                1 => &self.window.list,
                2 => &self.main.probation,
                3 => &self.main.protected,
                _ => unreachable!(),
            };
            list.prev(entry.policy_list_index.unwrap()).copied()
        } else {
            None
        }
    }

    fn evict_entries(&mut self, entries: &mut HashMap<u64, Entry>) -> Option<u64> {
        let first = self.evict_from_window(entries);
        self.evict_from_main(first, entries)
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
    use std::str::FromStr;

    use crate::metadata::Entry;
    use crate::timerwheel::Clock;

    use super::TinyLfu;

    fn group_numbers(input: Vec<String>) -> String {
        if input.is_empty() {
            return String::new();
        }

        let mut result = Vec::new();
        let mut current_group = Vec::new();

        // Parse the first number
        let mut prev = i32::from_str(&input[0]).unwrap();
        current_group.push(input[0].clone());

        for i in 1..input.len() {
            let num = i32::from_str(&input[i]).unwrap();
            if num == prev + 1 || num == prev - 1 {
                current_group.push(input[i].clone());
            } else {
                result.push(format!(
                    "{}-{}",
                    current_group.first().unwrap(),
                    current_group.last().unwrap()
                ));
                current_group = vec![input[i].clone()];
            }
            prev = num;
        }

        // Append the last group
        result.push(format!(
            "{}-{}",
            current_group.first().unwrap(),
            current_group.last().unwrap()
        ));

        result.join(">")
    }

    fn grouped(tlfu: &TinyLfu) -> (String, usize) {
        let total = tlfu.window.list.len()
            + tlfu.main.probation.list.len()
            + tlfu.main.protected.list.len();

        let window_seq = group_numbers(
            tlfu.window
                .list
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>(),
        );
        let probation_seq = group_numbers(
            tlfu.main
                .probation
                .list
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>(),
        );
        let protected_seq = group_numbers(
            tlfu.main
                .protected
                .list
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>(),
        );
        let result = [window_seq, probation_seq, protected_seq].join(":");
        (result, total)
    }

    struct AdaptiveTestEvent {
        hr_changes: Vec<f32>,
        expected: &'static str,
    }

    #[test]
    fn test_tlfu_adaptive() {
        let adaptive_tests = vec![
            // init, default hr will be 0.2
            AdaptiveTestEvent {
                hr_changes: vec![],
                expected: "149-100:99-80:79-0",
            },
            // same hr, window size decrease(repeat default), 100-108 move to probation front
            AdaptiveTestEvent {
                hr_changes: vec![0.2],
                expected: "149-109:108-80:79-0",
            },
            // hr increase, decrease window, 100-108 move to probation front
            AdaptiveTestEvent {
                hr_changes: vec![0.4],
                expected: "149-109:108-80:79-0",
            },
            // hr decrease, increase window, decrease protected
            // move 0-8 from protected to probation front,
            // move 80-88 from probation tail to window front
            AdaptiveTestEvent {
                hr_changes: vec![0.1],
                expected: "88-80>149-100:8-0>99-89:79-9",
            },
            // increase twice (decrease/decrease window)
            AdaptiveTestEvent {
                hr_changes: vec![0.4, 0.6],
                expected: "149-118:117-80:79-0",
            },
            // decrease twice (increase/decrease window)
            AdaptiveTestEvent {
                hr_changes: vec![0.1, 0.08],
                expected: "88-80>149-109:108-100>8-0>99-89:79-9",
            },
            // increase decrease (decrease/increase window)
            AdaptiveTestEvent {
                hr_changes: vec![0.4, 0.2],
                expected: "88-80>149-109:108-89:79-0",
            },
            // decrease increase (increase/increase window)
            AdaptiveTestEvent {
                hr_changes: vec![0.1, 0.2],
                expected: "97-80>149-100:17-0>99-98:79-18",
            },
        ];

        for test in &adaptive_tests {
            let mut tlfu = TinyLfu::new_sized(50, 100, 80);
            let mut entries = HashMap::new();
            let clock = Clock::new();
            tlfu.hr = 0.2;

            for i in 0..150 {
                entries.insert(i, Entry::new());
                tlfu.set(i, &mut entries);
            }
            tlfu.evict_entries(&mut entries);

            for i in 0..80 {
                tlfu.access(i, &clock, &mut entries);
            }

            for hrc in &test.hr_changes {
                let new_hits = (hrc * 100.0) as usize;
                let new_misses = 100 - new_hits;
                tlfu.hit_in_sample = new_hits;
                tlfu.misses_in_sample = new_misses;
                tlfu.climb();
                tlfu.resize_window(&mut entries);
            }
            let (result, total) = grouped(&tlfu);
            assert_eq!(
                tlfu.size,
                tlfu.window.len() + tlfu.main.probation.len() + tlfu.main.protected.len()
            );
            // let (result, total) = grouped(&tlfu);
            assert_eq!(150, total);
            assert_eq!(test.expected, result);
        }
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
