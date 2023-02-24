use crate::metadata::MetaData;

pub trait Policy {
    fn remove(&mut self, index: u32, metadata: &mut MetaData);
}
