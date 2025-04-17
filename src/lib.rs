use pyo3::prelude::*;
mod clockpro;
mod core;
mod filter;
mod lru;
mod metadata;
mod policy;
mod sketch;
mod timerwheel;
mod tlfu;

#[pymodule]
fn theine_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<core::TlfuCore>()?;
    m.add_class::<core::LruCore>()?;
    m.add_class::<core::ClockProCore>()?;
    m.add_class::<filter::BloomFilter>()?;
    Ok(())
}
