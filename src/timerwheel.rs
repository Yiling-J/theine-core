//! Hierarchical timer wheel for efficient TTL expiration scheduling.
//!
//! A timer wheel is a data structure for scheduling events at specific times
//! with O(1) insertion and removal operations. This implementation uses 5 levels
//! with exponentially increasing time ranges to handle everything from milliseconds
//! to days efficiently.

use std::cmp;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::metadata::{Entry, List};

/// A monotonic clock for tracking elapsed time since cache creation.
///
/// Uses `Instant` internally for reliable measurements across system time changes.
#[derive(Debug)]
pub struct Clock {
    start: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    /// Creates a new clock starting at the current instant.
    #[inline]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Returns the current elapsed time in nanoseconds since clock creation.
    ///
    /// # Note
    ///
    /// u64 can represent ~500 years in nanoseconds, which is sufficient for cache lifetimes.
    #[inline]
    pub fn now_ns(&self) -> u64 {
        (Instant::now() - self.start).as_nanos() as u64
    }

    /// Calculates the absolute expiration time given a TTL duration.
    ///
    /// # Arguments
    ///
    /// * `ttl` - Time-to-live duration in nanoseconds
    ///
    /// # Returns
    ///
    /// The absolute expiration time in nanoseconds, or 0 if ttl is 0 (no expiration)
    #[inline]
    pub fn expire_ns(&self, ttl: u64) -> u64 {
        if ttl > 0 {
            self.now_ns().saturating_add(ttl)
        } else {
            0
        }
    }
}

/// A hierarchical timer wheel for efficient TTL expiration scheduling.
///
/// Uses 5 levels with exponentially increasing time ranges:
/// - Level 0: ~1.07 seconds (64 buckets)
/// - Level 1: ~1.14 minutes (64 buckets)
/// - Level 2: ~1.22 hours (32 buckets)
/// - Level 3: ~1.63 days (4 buckets)
/// - Level 4: ~6.5 days+ (1 bucket)
#[derive(Debug)]
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
        Self::new()
    }
}

impl TimerWheel {
    /// Creates a new timer wheel with 5 hierarchical levels.
    pub fn new() -> Self {
        let buckets = vec![64, 64, 32, 4, 1];
        let clock = Clock::new();
        let nanos = clock.now_ns();

        // Pre-calculate span sizes and bit shifts for each level
        let spans = vec![
            Duration::from_secs(1).as_nanos().next_power_of_two() as u64, // ~1.07s
            Duration::from_secs(60).as_nanos().next_power_of_two() as u64, // ~1.14m
            Duration::from_secs(60 * 60).as_nanos().next_power_of_two() as u64, // ~1.22h
            Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two() as u64, // ~1.63d
            (Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two()
                * 4) as u64, // ~6.5d
            (Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two()
                * 4) as u64, // ~6.5d
        ];

        let shift: Vec<u32> = spans.iter().map(|s| s.trailing_zeros()).collect();

        let wheel = buckets
            .iter()
            .take(5)
            .map(|&bucket_count| (0..bucket_count).map(|_| List::new(8)).collect())
            .collect();

        log::debug!("TimerWheel initialized with {} levels", buckets.len());

        Self {
            buckets,
            spans,
            shift,
            wheel,
            clock,
            nanos,
        }
    }

    /// Finds the appropriate wheel level and slot for an expiration time.
    ///
    /// # Arguments
    ///
    /// * `expire` - Absolute expiration time in nanoseconds
    ///
    /// # Returns
    ///
    /// `(level, slot)` tuple indicating which level and bucket to use
    #[inline]
    fn find_index(&self, expire: u64) -> (u8, u8) {
        let duration = expire.saturating_sub(self.nanos);
        for i in 0..5 {
            if duration < self.spans[i + 1] {
                let ticks = expire >> self.shift[i];
                let slot = ticks & (self.buckets[i] - 1) as u64;
                return (i as u8, slot as u8);
            }
        }
        (4, 0)
    }

    /// Schedules an entry in the timer wheel.
    ///
    /// First removes the entry from any existing wheel position, then inserts it
    /// at the appropriate level and slot based on its expiration time.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key associated with the entry
    /// * `entry` - The entry to schedule (modified in place)
    pub fn schedule(&mut self, key: u64, entry: &mut Entry) {
        self.deschedule(entry);
        if entry.expire > 0 {
            let w_index = self.find_index(entry.expire);

            if let Some(level) = self.wheel.get_mut(w_index.0 as usize) {
                if let Some(bucket) = level.get_mut(w_index.1 as usize) {
                    entry.wheel_index = w_index;
                    entry.wheel_list_index = Some(bucket.insert_front(key));
                } else {
                    log::error!(
                        "TimerWheel schedule: slot index {} out of bounds for level {}",
                        w_index.1,
                        w_index.0
                    );
                }
            } else {
                log::error!(
                    "TimerWheel schedule: wheel index {} out of bounds",
                    w_index.0
                );
            }
        }
    }

    /// Removes an entry from the timer wheel.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove (modified in place)
    pub fn deschedule(&mut self, entry: &mut Entry) {
        let w_index = entry.wheel_index;

        if let Some(level) = self.wheel.get_mut(w_index.0 as usize) {
            if let Some(bucket) = level.get_mut(w_index.1 as usize) {
                if let Some(index) = entry.wheel_list_index {
                    bucket.remove(index);
                }
            } else {
                log::warn!(
                    "TimerWheel deschedule: slot index {} out of bounds for level {}",
                    w_index.1,
                    w_index.0
                );
            }
        } else {
            log::warn!(
                "TimerWheel deschedule: wheel index {} out of bounds",
                w_index.0
            );
        }

        entry.wheel_list_index = None;
        entry.wheel_index = (0, 0);
    }

    /// Advances the timer wheel to the current time and expires all stale entries.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time in nanoseconds
    /// * `entries` - Mutable reference to the cache entries map
    ///
    /// # Returns
    ///
    /// Vector of keys that were expired and removed
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

    /// Processes expiration for a specific wheel level.
    ///
    /// Scans through the affected buckets, separating expired entries from those
    /// that need to be rescheduled to higher levels.
    fn expire(
        &mut self,
        index: usize,
        prev_ticks: u64,
        delta: u64,
        entries: &mut HashMap<u64, Entry>,
    ) -> Vec<u64> {
        if index >= self.wheel.len() {
            log::error!("TimerWheel expire: index {} out of bounds", index);
            return Vec::new();
        }

        let mask = (self.buckets[index] - 1) as u64;
        let steps = cmp::min(delta as usize + 1, self.buckets[index]);
        let start = prev_ticks & mask;
        let end = start.saturating_add(steps as u64);
        let mut removed_all = Vec::new();

        for i in start..end {
            let bucket_idx = (i & mask) as usize;

            if bucket_idx >= self.wheel[index].len() {
                log::warn!(
                    "TimerWheel expire: bucket index {} out of bounds for level {}",
                    bucket_idx,
                    index
                );
                continue;
            }

            let mut modified = Vec::new();
            let mut removed = Vec::new();

            // Collect keys that are expired vs. those that need rescheduling
            for key in self.wheel[index][bucket_idx].iter() {
                if let Some(entry) = entries.get(key) {
                    if entry.expire <= self.nanos {
                        removed.push(*key);
                    } else {
                        modified.push(*key);
                    }
                }
            }

            // Deschedule expired entries
            for &key in &removed {
                if let Some(entry) = entries.get_mut(&key) {
                    self.deschedule(entry);
                }
            }

            // Reschedule entries that aren't actually expired yet
            for &key in &modified {
                if let Some(entry) = entries.get_mut(&key) {
                    self.schedule(key, entry);
                }
            }

            removed_all.extend(removed);
        }
        removed_all
    }

    /// Clears all entries from all wheel levels.
    pub fn clear(&mut self) {
        for level in self.wheel.iter_mut() {
            for bucket in level.iter_mut() {
                bucket.clear();
            }
        }
        log::debug!("TimerWheel cleared");
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
    fn test_advance_compact() {
        use std::collections::HashMap;
        use std::time::Duration;

        let mut tw = TimerWheel::new();
        let now = tw.clock.now_ns();
        let mut entries = HashMap::new();

        for i in 0..5_000_000 {
            let mut entry = Entry::new();
            entry.expire = now + Duration::from_secs((i + 1) as u64).as_nanos() as u64;
            tw.schedule(i + 1, &mut entry);
            entries.insert(i + 1, entry);
        }

        let mut evicted: Vec<u64> = Vec::new();
        let mut prev = 0;
        let mut counter = 0;

        for second in 1..=5_000_005 {
            let advanced_to = now + Duration::from_secs(second).as_nanos() as u64;
            let expired_keys = tw.advance(advanced_to, &mut entries);
            counter += expired_keys.len();
            evicted.extend(expired_keys.clone());

            let delta = counter - prev;
            assert!(
                (0..=2).contains(&delta),
                "unexpected number of expirations: {}",
                delta
            );
            prev = counter;
        }

        assert_eq!(evicted.len(), 5_000_000);
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
            now + Duration::from_secs(121).as_nanos() as u64,
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
        let mut rng = rand::rng();
        for _ in 0..50000 {
            let expire = now + Duration::from_secs(rng.random_range(5..250)).as_nanos() as u64;
            core.set(vec![(rng.random_range(0..10000), expire as i64)]);
        }

        for dt in [5, 6, 7, 10, 15, 20, 25, 50, 51, 52, 53, 70, 75, 85, 100] {
            core.wheel.advance(
                now + Duration::from_secs(dt).as_nanos() as u64,
                &mut core.entries,
            );
        }

        let now = core.wheel.clock.now_ns();
        for _ in 0..10000 {
            let expire = now + Duration::from_secs(rng.random_range(110..250)).as_nanos() as u64;
            core.set(vec![(rng.random_range(0..1000), expire as i64)]);
        }
        for dt in [5, 6, 7, 10, 15, 20, 25, 50, 51, 52, 53, 70, 75, 85, 100] {
            core.wheel.advance(
                now + Duration::from_secs(100 + dt).as_nanos() as u64,
                &mut core.entries,
            );
        }
    }
}
