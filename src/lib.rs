use pyo3::prelude::*;
mod core;
mod filter;
mod lru;
mod sketch;
mod timerwheel;
mod tlfu;

#[pymodule]
fn theine_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<core::TlfuCore>()?;
    m.add_class::<core::LruCore>()?;
    m.add_class::<filter::BloomFilter>()?;
    Ok(())
}
