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

//! Implementation of a Python interface for ModelarDB's compression library.

mod convert;

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Float32Array, UInt64Array, UInt8Array};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use modelardb_common::schemas::{COMPRESSED_SCHEMA, QUERY_SCHEMA, UNCOMPRESSED_SCHEMA};
use modelardb_common::types::{
    ErrorBound, TimestampArray, TimestampBuilder, ValueArray, ValueBuilder,
};
use modelardb_compression::{self};
use pyo3::import_exception;
use pyo3::prelude::{pyfunction, pymodule, wrap_pyfunction, PyAny, PyModule, PyResult, Python};

import_exception!(pyarrow, ArrowException);

/// Initialize the module by registering functions in the Python module.
#[pymodule]
fn modelardb_compression_python(_python: Python<'_>, py_module: &PyModule) -> PyResult<()> {
    py_module.add_function(wrap_pyfunction!(compress, py_module)?)?;
    py_module.add_function(wrap_pyfunction!(decompress, py_module)?)?;

    Ok(())
}

/// Compress a [`RecordBatch`] containing an univariate time series within `error_bound`. A
/// `univariate_id` can optionally be set to specify which univariate time series the compressed
/// segments was produced from, if none is set zero is used. An [`ArrowException`] is returned if
/// the schema of `uncompressed` does not match [`UNCOMPRESSED_SCHEMA`] or if [`pyo3`] cannot
/// convert the arguments from Python to Rust and the result from Rust to Python.
#[pyfunction]
#[pyo3(signature = (uncompressed, error_bound, univariate_id = 0))]
fn compress<'a>(
    uncompressed: &'a PyAny,
    error_bound: &'a PyAny,
    univariate_id: u64,
    python: Python<'a>,
) -> PyResult<&'a PyAny> {
    // Convert the record batch from Python to Rust and validate the arguments.
    let uncompressed = convert::python_record_batch_to_rust(uncompressed)?;
    if !fields_has_equal_types(&uncompressed.schema(), &UNCOMPRESSED_SCHEMA.0) {
        return Err(ArrowException::new_err(
            "Uncompressed can only contain millisecond timestamps and float32 values.".to_owned(),
        ));
    }

    let error_bound = ErrorBound::try_new(error_bound.extract::<f32>()?)
        .map_err(|error| convert::to_py_err(ArrowError::InvalidArgumentError(error.to_string())))?;

    // Compress the timestamps and values to segments containing metadata and models.
    let timestamps = modelardb_common::array!(uncompressed, 0, TimestampArray);
    let values = modelardb_common::array!(uncompressed, 1, ValueArray);

    // unwrap() is safe as the timestamps and values are guaranteed to be the same length.
    let compressed =
        modelardb_compression::try_compress(univariate_id, error_bound, timestamps, values)
            .unwrap();

    // Convert and return the finished compressed PyArrow record batch.
    convert::rust_record_batch_to_python(compressed, python)
}

/// Decompress a [`RecordBatch`] containing compressed segments to one or more univariate time
/// series. An [`ArrowException`] is returned if the schema of `compressed` does not match
/// [`COMPRESSED_SCHEMA`] or if [`pyo3`] cannot convert the argument from Python to Rust and the
/// result from Rust to Python.
#[pyfunction]
fn decompress<'a>(compressed: &'a PyAny, python: Python<'a>) -> PyResult<&'a PyAny> {
    // Convert the record batch from Python to Rust and validate the arguments.
    let compressed = convert::python_record_batch_to_rust(compressed)?;
    if !fields_has_equal_types(&compressed.schema(), &COMPRESSED_SCHEMA.0) {
        let error = ArrowError::InvalidArgumentError(
            "Compressed can only contain compressed segments with metadata and models.".to_owned(),
        );
        return Err(convert::to_py_err(error));
    }

    // Reconstruct the data points from the compressed segments containing metadata and models.
    modelardb_common::arrays!(
        compressed,
        univariate_ids,
        model_type_ids,
        start_times,
        end_times,
        timestamps,
        min_values,
        max_values,
        values,
        residuals,
        _error_array
    );

    let mut univariate_id_builder = UInt64Array::builder(compressed.num_rows());
    let mut timestamp_builder = TimestampBuilder::with_capacity(compressed.num_rows());
    let mut value_builder = ValueBuilder::with_capacity(compressed.num_rows());

    for row_index in 0..compressed.num_rows() {
        modelardb_compression::grid(
            univariate_ids.value(row_index),
            model_type_ids.value(row_index),
            start_times.value(row_index),
            end_times.value(row_index),
            timestamps.value(row_index),
            min_values.value(row_index),
            max_values.value(row_index),
            values.value(row_index),
            residuals.value(row_index),
            &mut univariate_id_builder,
            &mut timestamp_builder,
            &mut value_builder,
        );
    }

    // Construct, convert, and return the finished PyArrow record batch.
    let columns: Vec<ArrayRef> = vec![
        Arc::new(univariate_id_builder.finish()),
        Arc::new(timestamp_builder.finish()),
        Arc::new(value_builder.finish()),
    ];

    // unwrap() is safe as the arrays have the same length and the record batch matches the schema.
    let decompressed = RecordBatch::try_new(QUERY_SCHEMA.0.clone(), columns).unwrap();
    convert::rust_record_batch_to_python(decompressed, python)
}

/// Return [`true`] if the two [`Schemas`](Schema) contain the same number of fields and the fields
/// have the same types, otherwise [`false`] is returned.
fn fields_has_equal_types(schema_one: &Schema, schema_two: &Schema) -> bool {
    schema_one
        .fields()
        .iter()
        .zip(schema_two.fields())
        .all(|(field_one, field_two)| field_one.data_type() == field_two.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use modelardb_common::types::{ArrowTimestamp, ArrowValue};

    // Tests for fields_has_equal_types().
    #[test]
    fn test_empty_fields_has_equal_types() {
        assert!(fields_has_equal_types(&Schema::empty(), &Schema::empty()))
    }

    #[test]
    fn test_same_fields_has_equal_types() {
        assert!(fields_has_equal_types(
            &UNCOMPRESSED_SCHEMA.0.clone(),
            &UNCOMPRESSED_SCHEMA.0.clone()
        ))
    }

    #[test]
    fn test_different_fields_has_equal_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("measured_at", ArrowTimestamp::DATA_TYPE, false),
            Field::new("wind_speed", ArrowValue::DATA_TYPE, false),
        ]));

        assert!(fields_has_equal_types(&schema, &UNCOMPRESSED_SCHEMA.0))
    }

    #[test]
    fn test_different_fields_has_different_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("wind_direction", ArrowValue::DATA_TYPE, false),
            Field::new("wind_speed", ArrowValue::DATA_TYPE, false),
        ]));

        assert!(!fields_has_equal_types(&schema, &UNCOMPRESSED_SCHEMA.0))
    }
}
