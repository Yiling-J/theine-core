use pyo3::prelude::*;
mod filter;
mod lru;
mod sketch;
mod tlfu;

#[pymodule]
fn cacheme_utils(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<tlfu::TinyLfu>()?;
    m.add_class::<lru::Lru>()?;
    m.add_class::<filter::BloomFilter>()?;
    Ok(())
}
