use std::cmp;
use std::time::Duration;
use std::time::Instant;

use crate::metadata::Entry;
use crate::metadata::List;
use std::collections::HashMap;

pub struct Clock {
    start: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}

impl Clock {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn now_ns(&self) -> u64 {
        // u64 is about 500 years, should be enough for most system, so ignore overflow here
        (Instant::now() - self.start).as_nanos() as u64
    }

    pub fn expire_ns(&self, ttl: u64) -> u64 {
        if ttl > 0 {
            self.now_ns() + ttl
        } else {
            0
        }
    }
}

pub struct TimerWheel {
    buckets: Vec<usize>,
    spans: Vec<u64>,
    shift: Vec<u32>,
    wheel: Vec<Vec<List<u64>>>,
    pub clock: Clock,
    nanos: u64,
}

impl Default for TimerWheel {
    fn default() -> Self {
        TimerWheel::new()
    }
}

impl TimerWheel {
    pub fn new() -> Self {
        let buckets = vec![64, 64, 32, 4, 1];
        let clock = Clock::new();
        let nanos = clock.now_ns();
        let spans = vec![
            Duration::from_secs(1).as_nanos().next_power_of_two() as u64, // 1.07s
            Duration::from_secs(60).as_nanos().next_power_of_two() as u64, // 1.14m
            Duration::from_secs(60 * 60).as_nanos().next_power_of_two() as u64, // 1.22h
            Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two() as u64, // 1.63d
            (Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two()
                * 4) as u64, // 6.5d
            (Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two()
                * 4) as u64, // 6.5d
        ];
        let shift = vec![
            spans[0].trailing_zeros(),
            spans[1].trailing_zeros(),
            spans[2].trailing_zeros(),
            spans[3].trailing_zeros(),
            spans[4].trailing_zeros(),
        ];
        let mut wheel = Vec::new();
        for bucket in buckets.iter().take(5) {
            let mut tmp = Vec::new();
            for _ in 0..*bucket {
                tmp.push(List::new(8));
            }
            wheel.push(tmp);
        }

        Self {
            buckets,
            spans,
            shift,
            wheel,
            clock,
            nanos,
        }
    }

    fn find_index(&self, expire: u64) -> (u8, u8) {
        let duration = expire - self.nanos;
        for i in 0..5 {
            if duration < self.spans[i + 1] {
                let ticks = expire >> self.shift[i];
                let slot = ticks & (self.buckets[i] - 1) as u64;
                return (i as u8, slot as u8);
            }
        }
        (4, 0)
    }

    pub fn schedule(&mut self, key: u64, entry: &mut Entry) {
        self.deschedule(entry);
        if entry.expire > 0 {
            let w_index = self.find_index(entry.expire);
            entry.wheel_index = w_index;
            let index = self.wheel[w_index.0 as usize][w_index.1 as usize].insert_front(key);
            entry.wheel_list_index = Some(index);
        }
    }

    pub fn deschedule(&mut self, entry: &mut Entry) {
        let w_index = entry.wheel_index;
        if let Some(index) = entry.wheel_list_index {
            self.wheel[w_index.0 as usize][w_index.1 as usize].remove(index);
        }
        entry.wheel_list_index = None;
        entry.wheel_index = (0, 0);
    }

    pub fn advance(&mut self, now: u64, entries: &mut HashMap<u64, Entry>) -> Vec<u64> {
        let previous = self.nanos;
        self.nanos = now;
        let mut removed_all = Vec::new();

        for i in 0..5 {
            let prev_ticks = previous >> self.shift[i];
            let current_ticks = now >> self.shift[i];
            if current_ticks <= prev_ticks {
                break;
            }
            let mut removed = self.expire(i, prev_ticks, current_ticks - prev_ticks, entries);
            removed_all.append(&mut removed);
        }
        removed_all
    }

    fn expire(
        &mut self,
        index: usize,
        prev_ticks: u64,
        delta: u64,
        entries: &mut HashMap<u64, Entry>,
    ) -> Vec<u64> {
        let mask = (self.buckets[index] - 1) as u64;
        let steps = cmp::min(delta as usize, self.buckets[index]);
        let start = prev_ticks & mask;
        let end = start + steps as u64;
        let mut removed_all = Vec::new();
        for i in start..end {
            let mut modified = Vec::new();
            let mut removed = Vec::new();

            for key in self.wheel[index][(i & mask) as usize].iter() {
                if let Some(entry) = entries.get(key) {
                    if entry.expire <= self.nanos {
                        removed.push(*key);
                    } else {
                        modified.push(*key);
                    }
                }
            }

            for key in removed.iter() {
                if let Some(entry) = entries.get_mut(key) {
                    self.deschedule(entry);
                }
            }

            for key in modified.iter() {
                if let Some(entry) = entries.get_mut(key) {
                    self.schedule(*key, entry);
                }
            }

            removed_all.append(&mut removed);
        }
        removed_all
    }

    pub fn clear(&mut self) {
        for i in self.wheel.iter_mut() {
            for j in i.iter_mut() {
                j.clear()
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{core::TlfuCore, metadata::Entry};

    use super::TimerWheel;
    use rand::prelude::*;
    use std::{collections::HashMap, time::Duration};

    #[test]
    fn test_find_bucket() {
        let tw = TimerWheel::new();
        let now = tw.clock.now_ns();
        // max 1.14m
        for i in [0, 10, 30, 68] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos() as u64);
            assert_eq!(index.0, 0);
        }
        // max 1.22h
        for i in [69, 120, 200, 1000, 2500, 4398] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos() as u64);
            assert_eq!(index.0, 1);
        }
        // max 1.63d
        for i in [4399, 8000, 20000, 50000, 140737] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos() as u64);
            assert_eq!(index.0, 2);
        }

        // max 6.5d
        for i in [140738, 200000, 400000, 562949] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos() as u64);
            assert_eq!(index.0, 3);
        }

        // > 6.5d, safe because we will check expire time again on each advance
        for i in [562950, 1562950, 2562950, 3562950] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos() as u64);
            assert_eq!(index.0, 4);
        }
    }

    #[test]
    fn test_schedule() {
        let mut tw = TimerWheel::new();
        let now = tw.clock.now_ns();
        let mut entries = HashMap::new();
        for (key, expire) in [(1, 1u64), (2, 69u64), (3, 4399u64)] {
            let mut entry = Entry::new();
            entry.expire = now + Duration::from_secs(expire).as_nanos() as u64;
            tw.schedule(key, &mut entry);
            assert!(entry.wheel_list_index.is_some());
            entries.insert(key, entry);
        }

        assert!(tw.wheel[0].iter().any(|x| x.iter().any(|x| *x == 1)));
        assert!(tw.wheel[1].iter().any(|x| x.iter().any(|x| *x == 2)));
        assert!(tw.wheel[2].iter().any(|x| x.iter().any(|x| *x == 3)));

        // deschedule test
        for key in [1, 2, 3] {
            if let Some(entry) = entries.get_mut(&key) {
                tw.deschedule(entry);
                assert!(entry.wheel_index == (0, 0));
                assert!(entry.wheel_list_index.is_none());
            } else {
                assert!(false, "entry not found");
            }
        }

        assert!(!tw.wheel[0].iter().any(|x| x.iter().any(|x| *x == 1)));
        assert!(!tw.wheel[1].iter().any(|x| x.iter().any(|x| *x == 2)));
        assert!(!tw.wheel[2].iter().any(|x| x.iter().any(|x| *x == 3)));
    }

    #[test]
    fn test_advance() {
        let mut tw = TimerWheel::new();
        let mut entries = HashMap::new();
        let now = tw.clock.now_ns();
        for (key, expire) in [
            (1, 1u64),
            (2, 10u64),
            (3, 30u64),
            (4, 120u64),
            (5, 6500u64),
            (6, 142000u64),
            (7, 1420000u64),
        ] {
            let mut entry = Entry::new();
            entry.expire = now + Duration::from_secs(expire).as_nanos() as u64;
            tw.schedule(key, &mut entry);
            entries.insert(key, entry);
        }

        let mut expired = tw.advance(
            now + Duration::from_secs(64).as_nanos() as u64,
            &mut entries,
        );
        expired.sort();
        assert_eq!(expired, vec![1, 2, 3]);

        expired = tw.advance(
            now + Duration::from_secs(200).as_nanos() as u64,
            &mut entries,
        );
        assert_eq!(expired, vec![4]);

        expired = tw.advance(
            now + Duration::from_secs(12000).as_nanos() as u64,
            &mut entries,
        );
        assert_eq!(expired, vec![5]);
        expired = tw.advance(
            now + Duration::from_secs(350000).as_nanos() as u64,
            &mut entries,
        );
        assert_eq!(expired, vec![6]);

        expired = tw.advance(
            now + Duration::from_secs(1520000).as_nanos() as u64,
            &mut entries,
        );
        assert_eq!(expired, vec![7]);
    }

    // Simple no panic test
    #[test]
    fn test_advance_large() {
        let mut core = TlfuCore::new(1000);
        let now = core.wheel.clock.now_ns();
        let mut rng = rand::thread_rng();
        for _ in 0..50000 {
            let expire = now + Duration::from_secs(rng.gen_range(5..250)).as_nanos() as u64;
            core.set(vec![(rng.gen_range(0..10000), expire as i64)]);
        }

        for dt in [5, 6, 7, 10, 15, 20, 25, 50, 51, 52, 53, 70, 75, 85, 100] {
            core.wheel.advance(
                now + Duration::from_secs(dt).as_nanos() as u64,
                &mut core.entries,
            );
        }

        let now = core.wheel.clock.now_ns();
        for _ in 0..10000 {
            let expire = now + Duration::from_secs(rng.gen_range(110..250)).as_nanos() as u64;
            core.set(vec![(rng.gen_range(0..1000), expire as i64)]);
        }
        for dt in [5, 6, 7, 10, 15, 20, 25, 50, 51, 52, 53, 70, 75, 85, 100] {
            core.wheel.advance(
                now + Duration::from_secs(100 + dt).as_nanos() as u64,
                &mut core.entries,
            );
        }
    }
}
