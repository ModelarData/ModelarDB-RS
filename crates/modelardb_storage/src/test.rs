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

//! Implementation of functions to get [`ModelTableMetadata`], used throughout ModelarDB's tests.

use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use modelardb_common::test::{
    ERROR_BOUND_FIVE, ERROR_BOUND_ONE, ERROR_BOUND_ZERO, MODEL_TABLE_NAME,
};
use modelardb_types::types::{ArrowTimestamp, ArrowValue, ErrorBound};
use std::sync::Arc;

use crate::metadata::model_table_metadata::ModelTableMetadata;

/// Return [`ModelTableMetadata`] for a model table with a schema containing a tag column, a
/// timestamp column, and two field columns.
pub fn model_table_metadata() -> ModelTableMetadata {
    let query_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("field_1", ArrowValue::DATA_TYPE, false),
        Field::new("field_2", ArrowValue::DATA_TYPE, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let error_bounds = vec![
        ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
        ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap(),
        ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
        ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
    ];

    let generated_columns = vec![None, None, None, None];

    ModelTableMetadata::try_new(
        MODEL_TABLE_NAME.to_owned(),
        query_schema,
        error_bounds,
        generated_columns,
    )
    .unwrap()
}

/// Return [`ModelTableMetadata`] in an [`Arc`] for a model table with a schema containing a tag
/// column, a timestamp column, and two field columns.
pub fn model_table_metadata_arc() -> Arc<ModelTableMetadata> {
    Arc::new(model_table_metadata())
}
