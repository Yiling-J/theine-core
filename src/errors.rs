use std::fmt;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// Comprehensive error type for cache operations
///
/// This enum represents all error conditions that can occur during cache operations.
/// Each variant includes context information to aid debugging and monitoring.
///
/// # Examples
///
/// ```ignore
/// use theine_core::errors::CacheError;
///
/// let err = CacheError::policy(42, "inconsistent state");
/// eprintln!("{}", err); // Policy error for key 42: inconsistent state
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CacheError {
    /// Policy algorithm encountered an inconsistency
    ///
    /// This typically indicates a bug in the TinyLFU or SLRU implementation
    Policy { key: u64, message: String },

    /// Timer wheel expiration system encountered an error
    TimerWheel { key: u64, message: String },

    /// Cache metadata inconsistency detected
    ///
    /// Entry exists but is missing from expected data structures
    Metadata { key: u64, message: String },

    /// User input validation failed
    Validation(String),

    /// Internal state corruption detected
    ///
    /// This indicates the cache's internal consistency was violated
    StateCorruption(String),
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy { key, message } => {
                write!(f, "Policy error for key {}: {}", key, message)
            }
            Self::TimerWheel { key, message } => {
                write!(f, "Timer wheel error for key {}: {}", key, message)
            }
            Self::Metadata { key, message } => {
                write!(f, "Metadata error for key {}: {}", key, message)
            }
            Self::Validation(msg) => {
                write!(f, "Validation error: {}", msg)
            }
            Self::StateCorruption(msg) => {
                write!(f, "State corruption: {}", msg)
            }
        }
    }
}

impl From<anyhow::Error> for CacheError {
    fn from(err: anyhow::Error) -> Self {
        Self::StateCorruption(err.to_string())
    }
}

impl std::error::Error for CacheError {}

/// Automatic conversion to Python exceptions with logging
impl From<CacheError> for PyErr {
    fn from(err: CacheError) -> Self {
        let err_msg = err.to_string();

        match &err {
            CacheError::Policy { .. }
            | CacheError::TimerWheel { .. }
            | CacheError::Metadata { .. }
            | CacheError::StateCorruption(_) => {
                log::error!("{}", err_msg);
                PyRuntimeError::new_err(err_msg)
            }
            CacheError::Validation(_) => {
                log::warn!("{}", err_msg);
                PyValueError::new_err(err_msg)
            }
        }
    }
}

/// Constructor methods for creating errors with minimal boilerplate
impl CacheError {
    /// Create a policy error with context
    ///
    /// # Arguments
    /// * `key` - The cache key involved in the error
    /// * `message` - A descriptive message about what went wrong
    #[must_use]
    pub fn policy(key: u64, message: impl Into<String>) -> Self {
        Self::Policy {
            key,
            message: message.into(),
        }
    }

    /// Create a timer wheel error with context
    ///
    /// # Arguments
    /// * `key` - The cache key involved in the error
    /// * `message` - A descriptive message about what went wrong
    #[must_use]
    pub fn timer_wheel(key: u64, message: impl Into<String>) -> Self {
        Self::TimerWheel {
            key,
            message: message.into(),
        }
    }

    /// Create a metadata error with context
    ///
    /// # Arguments
    /// * `key` - The cache key involved in the error
    /// * `message` - A descriptive message about what went wrong
    #[must_use]
    pub fn metadata(key: u64, message: impl Into<String>) -> Self {
        Self::Metadata {
            key,
            message: message.into(),
        }
    }

    /// Create a validation error
    ///
    /// # Arguments
    /// * `message` - A descriptive validation error message
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    /// Create a state corruption error
    ///
    /// # Arguments
    /// * `message` - A descriptive message about the corruption
    #[must_use]
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::StateCorruption(message.into())
    }
}

/// Safely execute a closure, catching panics and converting them to PyErr
///
/// This function ensures that panics in cache operations are caught and
/// converted to Python exceptions, preventing crashes in Python code.
///
/// # Arguments
/// * `f` - A closure that might panic
/// * `operation` - A string describing the operation for error messages
///
/// # Returns
/// * `Ok(T)` if the closure completes successfully
/// * `Err(PyRuntimeError)` if the closure panics
///
/// # Example
///
/// ```ignore
/// let result = catch_panic(|| {
///     // Risky operation
///     cache.some_operation()
/// }, "operation_name")?;
/// ```
#[inline]
pub fn catch_panic<F, T>(f: F, operation: &str) -> PyResult<T>
where
    F: FnOnce() -> T + std::panic::UnwindSafe,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|_| {
        let msg = format!(
            "Cache operation panicked in {}: this indicates an internal bug",
            operation
        );
        log::error!("{}", msg);
        PyRuntimeError::new_err(msg)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_policy_error() {
        let err = CacheError::policy(123, "missing index");
        assert_eq!(err.to_string(), "Policy error for key 123: missing index");
    }

    #[test]
    fn test_display_validation_error() {
        let err = CacheError::validation("invalid size");
        assert_eq!(err.to_string(), "Validation error: invalid size");
    }

    #[test]
    fn test_display_corruption_error() {
        let err = CacheError::corruption("state inconsistency");
        assert_eq!(err.to_string(), "State corruption: state inconsistency");
    }

    #[test]
    fn test_builder_methods() {
        assert!(matches!(
            CacheError::policy(1, "test"),
            CacheError::Policy { .. }
        ));
        assert!(matches!(
            CacheError::validation("test"),
            CacheError::Validation(_)
        ));
        assert!(matches!(
            CacheError::corruption("test"),
            CacheError::StateCorruption(_)
        ));
    }

    #[test]
    fn test_error_to_pyerr_validation() {
        Python::attach(|py| {
            let err: CacheError = CacheError::validation("test");
            let py_err: PyErr = err.into();
            assert!(py_err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn test_error_to_pyerr_policy() {
        Python::attach(|py| {
            let err: CacheError = CacheError::policy(42, "test");
            let py_err: PyErr = err.into();
            assert!(py_err.is_instance_of::<PyRuntimeError>(py));
        });
    }

    #[test]
    fn test_catch_panic_success() {
        let result = catch_panic(|| 42, "test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_catch_panic_on_panic() {
        let result = catch_panic(
            || {
                panic!("test panic");
            },
            "failing_operation",
        );
        assert!(result.is_err());
    }
}
