use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

mod core;
mod errors;
mod filter;
mod lru;
mod metadata;
mod sketch;
mod timerwheel;
mod tlfu;

#[pymodule(gil_used = false)]
fn theine_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize logging using the log crate
    // Python can intercept these logs via the logging module's integration with rust logs
    // Users can configure Python's logging to see theine_core messages
    let _ = log::logger();

    m.add_class::<core::TlfuCore>()?;
    m.add_class::<filter::BloomFilter>()?;
    m.add_function(wrap_pyfunction!(core::spread, m)?)?;
    Ok(())
}
