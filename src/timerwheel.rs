use std::cmp;
use std::time::Duration;
use std::time::Instant;

use crate::metadata::Link;
use crate::metadata::MetaData;
use crate::policy::Policy;

pub trait Cache {
    fn del_item(&mut self, key: &str, index: u32);
}

pub struct Clock {
    start: Instant,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn now_ns(&self) -> u128 {
        (Instant::now() - self.start).as_nanos()
    }

    pub fn expire_ns(&self, ttl: u128) -> u128 {
        if ttl > 0 {
            self.now_ns() + ttl
        } else {
            0
        }
    }
}

pub struct TimerWheel {
    buckets: Vec<usize>,
    spans: Vec<u128>,
    shift: Vec<u32>,
    wheel: Vec<Vec<Link>>,
    pub clock: Clock,
    nanos: u128,
}

impl TimerWheel {
    pub fn new(size: usize, metadata: &mut MetaData) -> Self {
        let buckets = vec![64, 64, 32, 4, 1];
        let clock = Clock::new();
        let nanos = clock.now_ns();
        let spans = vec![
            Duration::from_secs(1).as_nanos().next_power_of_two(), // 1.07s
            Duration::from_secs(60).as_nanos().next_power_of_two(), // 1.14m
            Duration::from_secs(60 * 60).as_nanos().next_power_of_two(), // 1.22h
            Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two(), // 1.63d
            Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two()
                * 4, // 6.5d
            Duration::from_secs(24 * 60 * 60)
                .as_nanos()
                .next_power_of_two()
                * 4, // 6.5d
        ];
        let shift = vec![
            spans[0].trailing_zeros(),
            spans[1].trailing_zeros(),
            spans[2].trailing_zeros(),
            spans[3].trailing_zeros(),
            spans[4].trailing_zeros(),
        ];
        let mut wheel = Vec::new();
        // counter is the index of link, start from 4 because 0,1,2,3 are reserved
        let mut counter = 4;
        for i in 0..5 {
            let mut tmp = Vec::new();
            for _ in 0..buckets[i] {
                tmp.push(Link::new(counter, size as u32, metadata));
                counter += 1;
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

    fn find_index(&self, expire: u128) -> (u8, u8) {
        let duration = expire - self.nanos;
        for i in 0..5 {
            if duration < self.spans[i + 1] {
                let ticks = expire >> self.shift[i];
                let slot = ticks & (self.buckets[i] - 1) as u128;
                return (i as u8, slot as u8);
            }
        }
        (4, 0)
    }

    pub fn schedule(&mut self, index: u32, metadata: &mut MetaData) {
        self.deschedule(index, metadata);
        let entry = &mut metadata.data[index as usize];
        if entry.expire > 0 {
            let w_index = self.find_index(entry.expire);
            entry.wheel_index = w_index;
            self.wheel[w_index.0 as usize][w_index.1 as usize].insert_front_wheel(index, metadata);
        }
    }

    pub fn deschedule(&mut self, index: u32, metadata: &mut MetaData) {
        let entry = &mut metadata.data[index as usize];
        let w_index = entry.wheel_index;
        let link_id = entry.wheel_link_id;
        if link_id > 0 {
            self.wheel[w_index.0 as usize][w_index.1 as usize].remove_wheel(index, metadata);
        }
    }

    pub fn advance(
        &mut self,
        now: u128,
        cache: &mut impl Cache,
        policy: &mut impl Policy,
        metadata: &mut MetaData,
    ) {
        let previous = self.nanos;
        self.nanos = now;

        for i in 0..5 {
            let prev_ticks = previous >> self.shift[i];
            let current_ticks = now >> self.shift[i];
            if current_ticks <= prev_ticks {
                break;
            }
            self.expire(
                i,
                prev_ticks,
                current_ticks - prev_ticks,
                cache,
                policy,
                metadata,
            );
        }
    }

    fn expire(
        &mut self,
        index: usize,
        prev_ticks: u128,
        delta: u128,
        cache: &mut impl Cache,
        policy: &mut impl Policy,
        metadata: &mut MetaData,
    ) {
        let mask = (self.buckets[index] - 1) as u128;
        let steps = cmp::min(delta as usize, self.buckets[index]);
        let start = prev_ticks & mask;
        let end = start + steps as u128;
        for i in start..end {
            let mut modified = Vec::new();
            let mut removed = Vec::new();

            for (index, key, expire) in self.wheel[index][(i & mask) as usize].iter_wheel(metadata)
            {
                if expire <= self.nanos {
                    cache.del_item(key.as_str(), index);
                    removed.push(index);
                } else {
                    modified.push(index);
                }
            }

            for index in removed.iter() {
                self.deschedule(*index, metadata);
                metadata.remove(*index);
                policy.remove(*index, metadata);
            }

            // clear current bucket and reschedule items in current bucket
            self.wheel[index][(i & mask) as usize].clear(metadata);

            for index in modified.iter() {
                self.schedule(*index, metadata)
            }
        }
    }

    pub fn clear(&mut self, metadata: &mut MetaData) {
        for i in self.wheel.iter_mut() {
            for j in i.iter_mut() {
                j.clear(metadata)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{core::TlfuCore, metadata::MetaData, tlfu::TinyLfu};

    use super::{Cache, TimerWheel};
    use rand::prelude::*;
    use std::time::Duration;

    struct MockCache {
        deleted: Vec<String>,
    }

    impl Cache for MockCache {
        fn del_item(&mut self, key: &str, _index: u32) {
            self.deleted.push(key.to_string())
        }
    }

    #[test]
    fn test_find_bucket() {
        let mut metadata = MetaData::new(1000);
        let tw = TimerWheel::new(1000, &mut metadata);
        let now = tw.clock.now_ns();
        // max 1.14m
        for i in [0, 10, 30, 68] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos());
            assert_eq!(index.0, 0);
        }
        // max 1.22h
        for i in [69, 120, 200, 1000, 2500, 4398] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos());
            assert_eq!(index.0, 1);
        }
        // max 1.63d
        for i in [4399, 8000, 20000, 50000, 140737] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos());
            assert_eq!(index.0, 2);
        }

        // max 6.5d
        for i in [140738, 200000, 400000, 562949] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos());
            assert_eq!(index.0, 3);
        }

        // > 6.5d, safe because we will check expire time again on each advance
        for i in [562950, 1562950, 2562950, 3562950] {
            let index = tw.find_index(now + Duration::from_secs(i).as_nanos());
            assert_eq!(index.0, 4);
        }
    }

    #[test]
    fn test_schedule() {
        let mut metadata = MetaData::new(1000);
        let mut tw = TimerWheel::new(1000, &mut metadata);
        let now = tw.clock.now_ns();
        for (key, expire) in [("k1", 1u64), ("k2", 69u64), ("k3", 4399u64)] {
            let entry = metadata.get_or_create(key);
            entry.expire = now + Duration::from_secs(expire).as_nanos();
            let index = entry.index;
            tw.schedule(index, &mut metadata);
            assert!(metadata.data[index as usize].wheel_link_id > 0);
        }

        assert!(tw.wheel[0]
            .iter()
            .any(|x| x.iter_wheel(&metadata).any(|x| x.1 == "k1")));
        assert!(tw.wheel[1]
            .iter()
            .any(|x| x.iter_wheel(&metadata).any(|x| x.1 == "k2")));
        assert!(tw.wheel[2]
            .iter()
            .any(|x| x.iter_wheel(&metadata).any(|x| x.1 == "k3")));
        // deschedule test
        for key in ["k1", "k2", "k3"] {
            let index = metadata.get_or_create(key).index;
            tw.deschedule(index, &mut metadata);
            assert!(metadata.data[index as usize].wheel_link_id == 0);
        }
        assert!(!tw.wheel[0]
            .iter()
            .any(|x| x.iter_wheel(&metadata).any(|x| x.1 == "k1")));
        assert!(!tw.wheel[1]
            .iter()
            .any(|x| x.iter_wheel(&metadata).any(|x| x.1 == "k2")));
        assert!(!tw.wheel[2]
            .iter()
            .any(|x| x.iter_wheel(&metadata).any(|x| x.1 == "k3")));
    }

    #[test]
    fn test_advance() {
        let mut metadata = MetaData::new(1000);
        let mut tw = TimerWheel::new(1000, &mut metadata);
        let now = tw.clock.now_ns();
        let cache = &mut MockCache {
            deleted: Vec::new(),
        };
        let mut policy = TinyLfu::new(1000, &mut metadata);
        for (key, expire) in [
            ("k1", 1u64),
            ("k2", 10u64),
            ("k3", 30u64),
            ("k4", 120u64),
            ("k5", 6500u64),
            ("k6", 142000u64),
            ("k7", 1420000u64),
        ] {
            let entry = metadata.get_or_create(key);
            let index = entry.index;
            entry.expire = now + Duration::from_secs(expire).as_nanos();
            policy.set(index, &mut metadata);
            tw.schedule(index, &mut metadata);
        }

        tw.advance(
            now + Duration::from_secs(64).as_nanos(),
            cache,
            &mut policy,
            &mut metadata,
        );
        assert_eq!(cache.deleted.len(), 3);
        assert_eq!(policy.len(), 4);
        for key in ["k1", "k2", "k3"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id == 0);
        }
        for key in ["k4", "k5", "k6", "k7"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id > 0);
        }

        tw.advance(
            now + Duration::from_secs(200).as_nanos(),
            cache,
            &mut policy,
            &mut metadata,
        );
        assert_eq!(cache.deleted.len(), 4);
        assert_eq!(policy.len(), 3);
        for key in ["k1", "k2", "k3", "k4"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id == 0);
        }
        for key in ["k5", "k6", "k7"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id > 0);
        }
        tw.advance(
            now + Duration::from_secs(12000).as_nanos(),
            cache,
            &mut policy,
            &mut metadata,
        );
        assert_eq!(cache.deleted.len(), 5);
        assert_eq!(policy.len(), 2);
        for key in ["k1", "k2", "k3", "k4", "k5"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id == 0);
        }
        for key in ["k6", "k7"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id > 0);
        }
        tw.advance(
            now + Duration::from_secs(350000).as_nanos(),
            cache,
            &mut policy,
            &mut metadata,
        );
        assert_eq!(cache.deleted.len(), 6);
        assert_eq!(policy.len(), 1);
        for key in ["k1", "k2", "k3", "k4", "k5", "k6"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id == 0);
        }

        {
            let key = "k7";
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id > 0);
        }
        tw.advance(
            now + Duration::from_secs(1520000).as_nanos(),
            cache,
            &mut policy,
            &mut metadata,
        );
        assert_eq!(cache.deleted.len(), 7);
        assert_eq!(policy.len(), 0);
        for key in ["k1", "k2", "k3", "k4", "k5", "k6", "k7"] {
            let index = metadata.get_or_create(key).index;
            assert!(metadata.data[index as usize].wheel_link_id == 0);
        }
    }

    // Simple no panic test
    #[test]
    fn test_advance_large() {
        let mut core = TlfuCore::new(1000);
        let now = core.wheel.clock.now_ns();
        let cache = &mut MockCache {
            deleted: Vec::new(),
        };
        let mut rng = rand::thread_rng();
        for _ in 0..50000 {
            let expire = now + Duration::from_secs(rng.gen_range(5..250)).as_nanos();
            core.set(&format!("{}", rng.gen_range(0..10000)), expire);
        }

        for dt in [5, 6, 7, 10, 15, 20, 25, 50, 51, 52, 53, 70, 75, 85, 100] {
            core.wheel.advance(
                now + Duration::from_secs(dt).as_nanos(),
                cache,
                &mut core.policy,
                &mut core.metadata,
            );
        }
        let now = core.wheel.clock.now_ns();
        for _ in 0..10000 {
            let expire = now + Duration::from_secs(rng.gen_range(110..250)).as_nanos();
            core.set(&format!("{}n", rng.gen_range(0..1000)), expire);
        }
        for dt in [5, 6, 7, 10, 15, 20, 25, 50, 51, 52, 53, 70, 75, 85, 100] {
            core.wheel.advance(
                now + Duration::from_secs(100 + dt).as_nanos(),
                cache,
                &mut core.policy,
                &mut core.metadata,
            );
        }
    }
}
