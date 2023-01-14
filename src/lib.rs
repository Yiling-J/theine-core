use pyo3::prelude::*;
mod lru;
mod sketch;
mod tlfu;

#[pymodule]
fn cacheme_utils(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<tlfu::TinyLfu>()?;
    m.add_class::<lru::Lru>()?;
    Ok(())
}