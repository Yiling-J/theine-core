pub trait Policy {
    fn remove(&mut self, key: &str);
}
