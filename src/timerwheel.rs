use ahash::AHasher;
use std::cmp;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::time::Duration;
use std::time::SystemTime;

pub trait Cache {
    fn del_item(&mut self, key: &str);
}

pub struct TimerWheel {
    buckets: Vec<usize>,
    spans: Vec<u128>,
    shift: Vec<u32>,
    wheel: Vec<Vec<HashMap<String, u128>>>,
    keys: HashMap<String, (u8, u8), BuildHasherDefault<AHasher>>,
    nanos: u128,
}

impl TimerWheel {
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

    pub fn schedule(&mut self, key: &str, expire: u128) {
        self.deschedule(key);
        let index = self.find_index(expire);
        self.wheel[index.0 as usize][index.1 as usize].insert(key.to_string(), expire);
        self.keys.insert(key.to_string(), index);
    }

    pub fn deschedule(&mut self, key: &str) {
        let index = self.keys.remove(key);
        if let Some(i) = index {
            self.wheel[i.0 as usize][i.1 as usize].remove(key);
        }
    }

    pub fn advance(&mut self, now: u128, cache: &mut impl Cache) {
        let previous = self.nanos;
        self.nanos = now;

        for i in 0..5 {
            let prev_ticks = previous >> self.shift[i];
            let current_ticks = now >> self.shift[i];
            if current_ticks <= prev_ticks {
                break;
            }
            self.expire(i, prev_ticks, current_ticks - prev_ticks, cache);
        }
    }

    fn expire(&mut self, index: usize, prev_ticks: u128, delta: u128, cache: &mut impl Cache) {
        let mask = (self.buckets[index] - 1) as u128;
        let steps = cmp::min(delta as usize, self.buckets[index]);
        let start = prev_ticks & mask;
        let end = start + steps as u128;
        for i in start..end {
            let mut modified: HashMap<String, u128> = HashMap::new();
            for data in self.wheel[index][(i & mask) as usize].iter() {
                if *data.1 <= self.nanos {
                    cache.del_item(data.0);
                } else {
                    modified.insert(data.0.to_string(), *data.1);
                }
            }
            // clear current bucket and reschedule items in current bucket
            self.wheel[index][(i & mask) as usize].clear();
            for i in modified.iter() {
                self.schedule(i.0, *i.1);
            }
        }
    }
}

impl TimerWheel {
    pub fn new() -> Self {
        let buckets = vec![64, 64, 32, 4, 1];
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
        let mut wheel: Vec<Vec<HashMap<String, u128>>> = vec![Vec::new(); 5];
        for i in 0..5 {
            wheel[i] = vec![HashMap::new(); buckets[i]];
        }

        Self {
            buckets,
            spans,
            shift,
            wheel,
            keys: HashMap::default(),
            nanos: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{Cache, TimerWheel};
    use std::time::{Duration, SystemTime};

    struct MockCache {
        deleted: Vec<String>,
    }

    impl Cache for MockCache {
        fn del_item(&mut self, key: &str) {
            self.deleted.push(key.to_string())
        }
    }

    #[test]
    fn test_find_bucket() {
        let tw = TimerWheel::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
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
        let mut tw = TimerWheel::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        tw.schedule("k1", now + Duration::from_secs(1).as_nanos());
        tw.schedule("k2", now + Duration::from_secs(69).as_nanos());
        tw.schedule("k3", now + Duration::from_secs(4399).as_nanos());
        assert_eq!(tw.keys.len(), 3);
        assert!(tw.wheel[0].iter().any(|x| x.contains_key("k1")));
        assert!(tw.wheel[1].iter().any(|x| x.contains_key("k2")));
        assert!(tw.wheel[2].iter().any(|x| x.contains_key("k3")));
        // deschedule test
        tw.deschedule("k1");
        tw.deschedule("k2");
        tw.deschedule("k3");
        assert_eq!(tw.keys.len(), 0);
        assert!(!tw.wheel[0].iter().any(|x| x.contains_key("k1")));
        assert!(!tw.wheel[1].iter().any(|x| x.contains_key("k2")));
        assert!(!tw.wheel[2].iter().any(|x| x.contains_key("k3")));
    }

    #[test]
    fn test_advance() {
        let mut tw = TimerWheel::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let cache = &mut MockCache {
            deleted: Vec::new(),
        };

        tw.schedule("k1", now + Duration::from_secs(1).as_nanos());
        tw.schedule("k2", now + Duration::from_secs(10).as_nanos());
        tw.schedule("k3", now + Duration::from_secs(30).as_nanos());
        tw.schedule("k4", now + Duration::from_secs(120).as_nanos());
        tw.schedule("k5", now + Duration::from_secs(6500).as_nanos());
        tw.schedule("k6", now + Duration::from_secs(142000).as_nanos());
        tw.schedule("k7", now + Duration::from_secs(1420000).as_nanos());
        assert_eq!(tw.keys.len(), 7);
        tw.advance(now + Duration::from_secs(64).as_nanos(), cache);
        assert_eq!(cache.deleted.len(), 3);
        tw.advance(now + Duration::from_secs(200).as_nanos(), cache);
        assert_eq!(cache.deleted.len(), 4);
        tw.advance(now + Duration::from_secs(12000).as_nanos(), cache);
        assert_eq!(cache.deleted.len(), 5);
        tw.advance(now + Duration::from_secs(350000).as_nanos(), cache);
        assert_eq!(cache.deleted.len(), 6);
        tw.advance(now + Duration::from_secs(1520000).as_nanos(), cache);
        assert_eq!(cache.deleted.len(), 7);
    }
}
