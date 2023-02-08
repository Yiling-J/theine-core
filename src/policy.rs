pub trait Policy {
    fn set(&mut self, key: &str) -> Option<String>;
    fn remove(&mut self, key: &str);
    fn access(&mut self, key: &str);
}
