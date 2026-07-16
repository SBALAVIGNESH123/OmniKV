#![expect(
    clippy::missing_errors_doc,
    reason = "PyO3 method docstrings are exposed to Python callers; Rust-level error and panic sections add little value for these thin wrappers."
)]

use omni_engine::{EmbeddedConfig, EmbeddedOmniKv as NativeEmbeddedOmniKv};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

#[expect(
    clippy::needless_pass_by_value,
    reason = "This function is used directly as a map_err adapter, which receives owned errors."
)]
fn to_py_err(error: omni_engine::EmbeddedError) -> PyErr {
    match error {
        omni_engine::EmbeddedError::InvalidKey(_)
        | omni_engine::EmbeddedError::InvalidNamespace(_) => {
            PyValueError::new_err(error.to_string())
        }
        _ => PyRuntimeError::new_err(error.to_string()),
    }
}

#[pyclass(name = "EmbeddedOmniKv")]
pub struct PyEmbeddedOmniKv {
    inner: Option<NativeEmbeddedOmniKv>,
}

impl PyEmbeddedOmniKv {
    const fn new(inner: NativeEmbeddedOmniKv) -> Self {
        Self { inner: Some(inner) }
    }

    fn inner(&self) -> PyResult<&NativeEmbeddedOmniKv> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("OmniKV embedded handle is closed"))
    }
}

#[pymethods]
impl PyEmbeddedOmniKv {
    /// Open an embedded `OmniKV` directory with an optional namespace.
    #[staticmethod]
    #[pyo3(signature = (data_dir, namespace = "sketchlog"))]
    pub fn open(data_dir: &str, namespace: &str) -> PyResult<Self> {
        open_embedded(data_dir, namespace)
    }

    /// Open an embedded `OmniKV` directory without a namespace.
    #[staticmethod]
    pub fn open_dir(data_dir: &str) -> PyResult<Self> {
        NativeEmbeddedOmniKv::open_dir(data_dir)
            .map(Self::new)
            .map_err(to_py_err)
    }

    /// Return a new handle over the same database scoped to `namespace`.
    pub fn scoped(&self, namespace: &str) -> PyResult<Self> {
        self.inner()?
            .scoped(namespace)
            .map(Self::new)
            .map_err(to_py_err)
    }

    /// Store a string value and return the committed sequence number.
    pub fn put(&self, key: &str, value: &str) -> PyResult<u64> {
        self.inner()?.put(key, value).map_err(to_py_err)
    }

    /// Read a string value by key.
    pub fn get(&self, key: &str) -> PyResult<Option<String>> {
        self.inner()?.get(key).map_err(to_py_err)
    }

    /// Delete a key and return the committed sequence number.
    pub fn delete(&self, key: &str) -> PyResult<u64> {
        self.inner()?.delete(key).map_err(to_py_err)
    }

    /// Scan key/value pairs whose application-visible key starts with `prefix`.
    #[pyo3(signature = (prefix, limit = None))]
    pub fn scan_prefix<'py>(
        &self,
        py: Python<'py>,
        prefix: &str,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let rows = self
            .inner()?
            .scan_prefix(prefix, limit)
            .map_err(to_py_err)?;
        let result = PyList::empty(py);
        for row in rows {
            let item = PyDict::new(py);
            item.set_item("key", row.key)?;
            item.set_item("value", row.value)?;
            result.append(item)?;
        }
        Ok(result)
    }

    /// Flush active `OmniKV` storage files.
    pub fn sync(&self) -> PyResult<()> {
        self.inner()?.sync().map_err(to_py_err)
    }

    /// Release this Python handle.
    pub fn close(&mut self) {
        self.inner = None;
    }

    /// Return lightweight operational stats.
    pub fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = self.inner()?.stats();
        let result = PyDict::new(py);
        result.set_item("sequence", stats.sequence)?;
        result.set_item("memtable_size", stats.memtable_size)?;
        result.set_item("total_records", stats.total_records)?;
        result.set_item("l0_sstables", stats.l0_sstables)?;
        result.set_item("l1_sstables", stats.l1_sstables)?;
        result.set_item(
            "scan_buffer_pool_available",
            stats.scan_buffer_pool_available,
        )?;
        Ok(result)
    }
}

/// Open an embedded `OmniKV` directory with an optional namespace.
#[pyfunction]
#[pyo3(signature = (data_dir, namespace = "sketchlog"))]
pub fn open_embedded(data_dir: &str, namespace: &str) -> PyResult<PyEmbeddedOmniKv> {
    let config = EmbeddedConfig::new(data_dir).namespace(namespace);
    NativeEmbeddedOmniKv::open(config)
        .map(PyEmbeddedOmniKv::new)
        .map_err(to_py_err)
}

#[pymodule]
fn omnikv(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyEmbeddedOmniKv>()?;
    module.add_function(wrap_pyfunction!(open_embedded, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
