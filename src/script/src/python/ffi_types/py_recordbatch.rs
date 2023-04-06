//! PyRecordBatch is a Python class that wraps a RecordBatch,
//! and provide a PyMapping Protocol to
//! access the columns of the RecordBatch.

use common_recordbatch::RecordBatch;
use crossbeam_utils::atomic::AtomicCell;
use pyo3::exceptions::{PyKeyError, PyRuntimeError};
#[cfg(feature = "pyo3_backend")]
use pyo3::pyclass as pyo3class;
use pyo3::{pymethods, PyObject, PyResult, Python};
use rustpython_vm::builtins::PyStr;
use rustpython_vm::protocol::PyMappingMethods;
use rustpython_vm::types::AsMapping;
use rustpython_vm::{
    atomic_func, pyclass as rspyclass, PyObject as RsPyObject, PyPayload, PyResult as RsPyResult,
    VirtualMachine,
};

use crate::python::ffi_types::PyVector;

#[cfg_attr(feature = "pyo3_backend", pyo3class(name = "PyRecordBatch"))]
#[rspyclass(module = false, name = "PyRecordBatch")]
#[derive(Debug, PyPayload)]
pub(crate) struct PyRecordBatch {
    record_batch: RecordBatch,
}

impl PyRecordBatch {
    pub fn new(record_batch: RecordBatch) -> Self {
        Self { record_batch }
    }
}

#[cfg(feature = "pyo3_backend")]
#[pymethods]
impl PyRecordBatch {
    fn __getitem__(&self, py: Python, key: PyObject) -> PyResult<PyVector> {
        let column = if let Ok(key) = key.extract::<String>(py) {
            self.record_batch.column_by_name(&key)
        } else if let Ok(key) = key.extract::<usize>(py) {
            Some(self.record_batch.column(key))
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "Expect either str or int, found {key:?}"
            )));
        }
        .ok_or_else(|| PyKeyError::new_err(format!("Column {} not found", key)))?;
        let v = PyVector::from(column.clone());
        Ok(v)
    }
    fn __iter__(&self) -> PyResult<Vec<PyVector>> {
        let iter: Vec<_> = self
            .record_batch
            .columns()
            .iter()
            .map(|i| PyVector::from(i.clone()))
            .collect();
        Ok(iter)
    }
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.len())
    }
}

impl PyRecordBatch {
    fn len(&self) -> usize {
        self.record_batch.num_rows()
    }
    fn get_item(&self, needle: &RsPyObject, vm: &VirtualMachine) -> RsPyResult {
        if let Ok(index) = needle.to_owned().try_into_value::<usize>(vm) {
            let column = self.record_batch.column(index);
            let v = PyVector::from(column.clone());
            Ok(v.into_pyobject(vm))
        } else if let Some(index) = needle.to_owned().payload::<PyStr>() {
            let key = index.as_str();

            let v = self.record_batch.column_by_name(key).ok_or_else(|| {
                vm.new_key_error(PyStr::from(format!("Column {} not found", key)).into_pyobject(vm))
            })?;
            let v: PyVector = v.clone().into();
            Ok(v.into_pyobject(vm))
        } else {
            Err(vm.new_key_error(
                PyStr::from(format!("Expect either str or int, found {needle:?}"))
                    .into_pyobject(vm),
            ))
        }
    }
}

#[rspyclass(with(AsMapping))]
impl PyRecordBatch {}

impl AsMapping for PyRecordBatch {
    fn as_mapping() -> &'static PyMappingMethods {
        static AS_MAPPING: PyMappingMethods = PyMappingMethods {
            length: atomic_func!(|mapping, _vm| Ok(PyRecordBatch::mapping_downcast(mapping).len())),
            subscript: atomic_func!(
                |mapping, needle, vm| PyRecordBatch::mapping_downcast(mapping).get_item(needle, vm)
            ),
            ass_subscript: AtomicCell::new(None),
        };
        &AS_MAPPING
    }
}
