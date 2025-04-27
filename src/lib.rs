use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
mod core;
mod filter;
mod lru;
mod metadata;
mod sketch;
mod timerwheel;
mod tlfu;

#[pymodule(gil_used = false)]
fn theine_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<core::TlfuCore>()?;
    m.add_class::<filter::BloomFilter>()?;
    m.add_function(wrap_pyfunction!(core::spread, m)?)?;
    Ok(())
}
