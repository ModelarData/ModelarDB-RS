/* Copyright 2022 The ModelarDB Contributors
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

//! Implementation of the type containing the metadata required to read from and
//! write to a model table.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Schema};

use crate::errors::ModelarDbError;
use crate::models::ErrorBound;
use crate::types::{ArrowTimestamp, ArrowValue};

/// Metadata required to ingest data into a model table and query a model table.
#[derive(Debug, Clone)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Schema of the data in the model table.
    pub schema: Arc<Schema>,
    /// Index of the timestamp column in the schema.
    pub timestamp_column_index: usize,
    /// Indices of the tag columns in the schema.
    pub tag_column_indices: Vec<usize>,
    /// Error bound of the field columns in the schema.
    pub error_bounds: Vec<ErrorBound>,
}

impl ModelTableMetadata {
    /// Create a new model table with the given metadata. If any of the following conditions are
    /// true, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned:
    /// * The timestamp or tag column indices does not match `schema`.
    /// * The types of the fields are not correct.
    /// * The timestamp column index is in the tag column indices.
    /// * The number of error bounds does not match the number of fields.
    /// * There are duplicates in the tag column indices.
    /// * There are more than 1024 columns.
    /// * There are no field columns.
    pub fn try_new(
        table_name: String,
        schema: Schema,
        timestamp_column_index: usize,
        tag_column_indices: Vec<usize>,
        error_bounds: Vec<ErrorBound>,
    ) -> Result<Self, ModelarDbError> {
        // If the timestamp index is in the tag indices, return an error.
        if tag_column_indices.contains(&timestamp_column_index) {
            return Err(ModelarDbError::ConfigurationError(
                "The timestamp column cannot be a tag column.".to_owned(),
            ));
        };

        if let Some(timestamp_field) = schema.fields.get(timestamp_column_index) {
            // If the field of the timestamp column is not of type ArrowTimestamp, return an error.
            if !timestamp_field
                .data_type()
                .equals_datatype(&ArrowTimestamp::DATA_TYPE)
            {
                return Err(ModelarDbError::ConfigurationError(format!(
                    "The timestamp column with index '{}' is not of type '{}'.",
                    timestamp_column_index,
                    ArrowTimestamp::DATA_TYPE
                )));
            }
        } else {
            // If the index of the timestamp column does not match the schema, return an error.
            return Err(ModelarDbError::ConfigurationError(format!(
                "The timestamp column index '{}' does not match a field in the schema.",
                timestamp_column_index
            )));
        }

        for tag_column_index in &tag_column_indices {
            if let Some(tag_field) = schema.fields.get(*tag_column_index) {
                // If the fields of the tag columns is not of type Utf8, return an error.
                if !tag_field.data_type().equals_datatype(&DataType::Utf8) {
                    return Err(ModelarDbError::ConfigurationError(format!(
                        "The tag column with index '{}' is not of type '{}'.",
                        tag_column_index,
                        DataType::Utf8
                    )));
                }
            } else {
                // If the indices for the tag columns does not match the schema, return an error.
                return Err(ModelarDbError::ConfigurationError(format!(
                    "The tag column index '{}' does not match a field in the schema.",
                    tag_column_index
                )));
            }
        }

        let field_column_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|index| {
                *index != timestamp_column_index && !tag_column_indices.contains(&index)
            })
            .collect();

        // If there are no field columns, return an error.
        if field_column_indices.is_empty() {
            return Err(ModelarDbError::ConfigurationError(
                "There needs to be at least one field column.".to_owned(),
            ));
        } else {
            for field_column_index in &field_column_indices {
                // unwrap() is safe to use since the indices are collected from the schema fields.
                let field = schema.fields.get(*field_column_index).unwrap();

                // If the fields of the field columns is not of type ArrowValue, return an error.
                if !field.data_type().equals_datatype(&ArrowValue::DATA_TYPE) {
                    return Err(ModelarDbError::ConfigurationError(format!(
                        "The field column with index '{}' is not of type '{}'.",
                        field_column_index,
                        ArrowValue::DATA_TYPE
                    )));
                }
            }
        }

        // If an error bound is not defined for each field, return an error.
        if field_column_indices.len() != error_bounds.len() {
            return Err(ModelarDbError::ConfigurationError(
                "An error bound must be defined for each field column.".to_owned(),
            ));
        }

        // If there are duplicate tag columns, return an error. HashSet.insert() can be used to check
        // for uniqueness since it returns true or false depending on if the inserted element already exists.
        let mut uniq = HashSet::new();
        if !tag_column_indices
            .clone()
            .into_iter()
            .all(|x| uniq.insert(x))
        {
            return Err(ModelarDbError::ConfigurationError(
                "The tag column indices cannot have duplicates.".to_owned(),
            ));
        }

        // If there are more than 1024 columns, return an error. This limitation is necessary
        // since 10 bits are used to identify the column index of the data in the 64-bit hash key.
        if schema.fields.len() > 1024 {
            return Err(ModelarDbError::ConfigurationError(
                "There cannot be more than 1024 columns in the model table.".to_owned(),
            ));
        }

        Ok(Self {
            name: table_name,
            schema: Arc::new(schema.clone()),
            tag_column_indices,
            timestamp_column_index,
            error_bounds,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use crate::types::{ArrowTimestamp, ArrowValue};

    // Tests for ModelTableMetadata.
    #[test]
    fn test_can_create_model_table_metadata() {
        let (schema, error_bounds) = get_model_table_schema_and_error_bounds();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 2],
            error_bounds,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_timestamp_index_in_tag_indices() {
        let (schema, error_bounds) = get_model_table_schema_and_error_bounds();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            0,
            vec![0, 1, 2],
            error_bounds,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_timestamp_index() {
        let (schema, error_bounds) = get_model_table_schema_and_error_bounds();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            10,
            vec![0, 1, 2],
            error_bounds,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_timestamp_type() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", DataType::UInt8, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_tag_index() {
        let (schema, error_bounds) = get_model_table_schema_and_error_bounds();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 10],
            error_bounds,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_tag_type() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::UInt8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_no_fields() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_field_type() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", DataType::UInt8, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    /// Return metadata for a model table with one tag column and the timestamp column at index 1.
    fn create_simple_model_table_metadata(
        schema: Schema,
    ) -> Result<ModelTableMetadata, ModelarDbError> {
        ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            1,
            vec![0],
            vec![ErrorBound::try_new(0.0).unwrap()],
        )
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_duplicate_tag_indices() {
        let (schema, error_bounds) = get_model_table_schema_and_error_bounds();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 1],
            error_bounds,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_missing_or_too_many_error_bounds() {
        let (schema, _error_bounds) = get_model_table_schema_and_error_bounds();
        let result =
            ModelTableMetadata::try_new("table_name".to_owned(), schema, 3, vec![0, 1, 2], vec![]);

        assert!(result.is_err());
    }

    fn get_model_table_schema_and_error_bounds() -> (Schema, Vec<ErrorBound>) {
        (
            Schema::new(vec![
                Field::new("location", DataType::Utf8, false),
                Field::new("install_year", DataType::Utf8, false),
                Field::new("model", DataType::Utf8, false),
                Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
                Field::new("power_output", ArrowValue::DATA_TYPE, false),
                Field::new("wind_speed", ArrowValue::DATA_TYPE, false),
                Field::new("temperature", ArrowValue::DATA_TYPE, false),
            ]),
            vec![
                ErrorBound::try_new(0.0).unwrap(),
                ErrorBound::try_new(0.0).unwrap(),
                ErrorBound::try_new(0.0).unwrap(),
            ],
        )
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_too_many_fields() {
        // Create 1025 fields that can be used to initialize a schema.
        let fields = (0..1025)
            .map(|i| Field::new(format!("field_{}", i).as_str(), DataType::Float32, false))
            .collect::<Vec<Field>>();

        let error_bounds = vec![
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
        ];

        let table_name = "table_name".to_owned();
        let result = ModelTableMetadata::try_new(
            table_name,
            Schema::new(fields),
            3,
            vec![0, 1, 2],
            error_bounds,
        );

        assert!(result.is_err());
    }
}
