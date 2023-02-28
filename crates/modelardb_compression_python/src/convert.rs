/* Copyright 2023 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Implementation of functions for converting Apache Arrow constructs from Python to Rust and from
//! Rust to Python. The functions are based on [code published] as part of arrow-rs under Apache2.
//! The functions all return [`PyErr`] if the conversion from Rust to Python or Python to Rust fail.
//!
//! [code published]: https://github.com/apache/arrow-rs/blob/master/arrow/src/pyarrow.rs

use std::ptr::{addr_of, addr_of_mut};
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::{PyAny, PyErr, PyResult, Python};
use pyo3::types::{PyDict, PyList};

import_exception!(pyarrow, ArrowException);

/// Convert an [`ArrowError`] to a [`PyErr`].
pub fn to_py_err(err: ArrowError) -> PyErr {
    ArrowException::new_err(err.to_string())
}

/// Convert an Apache Arrow record batch from Python to Rust.
pub fn python_record_batch_to_rust(record_batch: &PyAny) -> PyResult<RecordBatch> {
    // Convert the schema.
    let schema = record_batch.getattr("schema")?;
    let schema = Arc::new(python_schema_to_rust(schema)?);

    // Convert the columns.
    let columns = record_batch
        .getattr("columns")?
        .downcast::<PyList>()?
        .iter()
        .map(python_array_to_rust)
        .collect::<PyResult<_>>()?;

    // Construct and return the final arrow-rs record batch.
    RecordBatch::try_new(schema, columns).map_err(to_py_err)
}

/// Convert an Apache Arrow schema from Python to Rust.
fn python_schema_to_rust(schema: &PyAny) -> PyResult<Schema> {
    // Create a pointer to the schema's FFI representation.
    let ffi_arrow_schema = FFI_ArrowSchema::empty();
    let ffi_arrow_schema_ptr = &ffi_arrow_schema as *const FFI_ArrowSchema;

    // Convert the schema to its FFI representation using a hidden method.
    schema.call_method1("_export_to_c", (ffi_arrow_schema_ptr as Py_uintptr_t,))?;

    // Construct and return the final arrow-rs schema.
    Schema::try_from(&ffi_arrow_schema).map_err(to_py_err)
}

/// Convert an Apache Arrow array from Python to Rust.
fn python_array_to_rust(array: &PyAny) -> PyResult<Arc<dyn Array>> {
    // Create FFI representations for the array's components.
    let mut ffi_arrow_array = FFI_ArrowArray::empty();
    let mut ffi_arrow_schema = FFI_ArrowSchema::empty();

    // Convert the array to its FFI representation using a hidden method.
    array.call_method1(
        "_export_to_c",
        (
            addr_of_mut!(ffi_arrow_array) as Py_uintptr_t,
            addr_of_mut!(ffi_arrow_schema) as Py_uintptr_t,
        ),
    )?;

    // Construct and return the final arrow-rs array.
    let arrow_array = ArrowArray::new(ffi_arrow_array, ffi_arrow_schema);
    let array = make_array(ArrayData::try_from(arrow_array).map_err(to_py_err)?);

    Ok(array)
}

/// Convert an Apache Arrow record batch from Rust to Python.
pub fn rust_record_batch_to_python(record_batch: RecordBatch, python: Python) -> PyResult<&PyAny> {
    // Convert the schema.
    let schema = record_batch.schema();
    let schema = rust_schema_to_python(&schema, python)?;

    // Convert the columns.
    let columns = record_batch
        .columns()
        .iter()
        .map(|array| rust_array_to_python(array, python))
        .collect::<Result<Vec<_>, _>>()?;

    // Construct and return the final PyArrow record batch.
    let module = python.import("pyarrow")?;
    let class = module.getattr("RecordBatch")?;
    let kwargs = PyDict::new(python);
    kwargs.set_item("schema", schema)?;
    class.call_method("from_arrays", (columns,), Some(kwargs))
}

/// Convert an Apache Arrow schema from Rust to Python.
fn rust_schema_to_python<'a>(schema: &'a Schema, python: Python<'a>) -> PyResult<&'a PyAny> {
    // Create a pointer to the schema's C representation.
    let ffi_arrow_schema = FFI_ArrowSchema::try_from(schema).map_err(to_py_err)?;
    let ffi_arrow_schema_ptr = &ffi_arrow_schema as *const FFI_ArrowSchema;

    // Construct the schema from its FFI representation using a hidden method and return it.
    let module = python.import("pyarrow")?;
    let class = module.getattr("Schema")?;
    class.call_method1("_import_from_c", (ffi_arrow_schema_ptr as Py_uintptr_t,))
}

/// Convert an Apache Arrow array from Rust to Python.
fn rust_array_to_python<'a>(array: &'a Arc<dyn Array>, python: Python<'a>) -> PyResult<&'a PyAny> {
    // Create FFI representations of the array's components.
    let ffi_arrow_array = FFI_ArrowArray::new(array.data());
    let ffi_arrow_schema = FFI_ArrowSchema::try_from(array.data_type()).map_err(to_py_err)?;

    // Construct the array from its FFI representation using a hidden method and return it.
    let module = python.import("pyarrow")?;
    let class = module.getattr("Array")?;
    class.call_method1(
        "_import_from_c",
        (
            addr_of!(ffi_arrow_array) as Py_uintptr_t,
            addr_of!(ffi_arrow_schema) as Py_uintptr_t,
        ),
    )
}
