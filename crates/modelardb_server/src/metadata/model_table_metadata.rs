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

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::logical_expr::expr::Expr;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::{ArrowTimestamp, ArrowValue};
use modelardb_compression::models::ErrorBound;

/// Metadata required to ingest data into a model table and query a model table.
#[derive(Debug, Clone)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Full schema of the data in the model table.
    pub schema: Arc<Schema>,
    /// Reduced schema of the data in the model table without generated column.
    pub ingestion_schema: Arc<Schema>,
    /// Index of the timestamp column in the schema.
    pub timestamp_column_index: usize,
    /// Indices of the tag columns in the schema.
    pub tag_column_indices: Vec<usize>,
    /// Error bounds of the columns in the schema. It can only be non-zero for field columns.
    pub error_bounds: Vec<ErrorBound>,
    /// Expressions to generate the values of the columns in the schema. Only field columns can be
    /// generated by [`Expr`], so [`None`] is stored for timestamp, tag, and stored field columns.
    pub generated_columns: Vec<Option<GeneratedColumn>>,
}

impl ModelTableMetadata {
    /// Create a new model table with the given metadata. If any of the following conditions are
    /// true, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned:
    /// * The timestamp or tag column indices does not match `schema`.
    /// * The types of the fields are not correct.
    /// * The timestamp column index is in the tag column indices.
    /// * The number of error bounds does not match the number of columns.
    /// * The number of generation expressions does not match the number of columns.
    /// * There are duplicates in the tag column indices.
    /// * There are more than 1024 columns.
    /// * There are no field columns.
    pub fn try_new(
        table_name: String,
        schema: Schema,
        timestamp_column_index: usize,
        tag_column_indices: Vec<usize>,
        error_bounds: Vec<ErrorBound>,
        generated_columns: Vec<Option<GeneratedColumn>>,
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
                "The timestamp column index '{timestamp_column_index}' does not match a field in the schema."
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
                    "The tag column index '{tag_column_index}' does not match a field in the schema."
                )));
            }
        }

        let field_column_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|index| *index != timestamp_column_index && !tag_column_indices.contains(index))
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

        // If an error bound is not defined for each column, return an error.
        if schema.fields().len() != error_bounds.len() {
            return Err(ModelarDbError::ConfigurationError(
                "An error bound must be defined for each column.".to_owned(),
            ));
        }

        // If an generated column or None is not defined for each column, return an error.
        if schema.fields().len() != generated_columns.len() {
            return Err(ModelarDbError::ConfigurationError(
                "An generated column or None must be defined for each column.".to_owned(),
            ));
        }

        // If a generated field column depends on other generated field columns, return an error.
        for generated_column in generated_columns.iter().flatten() {
            for source_column in &generated_column.source_columns {
                if generated_columns[*source_column].is_some() {
                    return Err(ModelarDbError::ConfigurationError(
                        "A generated field column cannot depend on generated field columns."
                            .to_owned(),
                    ));
                }
            }
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

        // If there are more than 1024 columns, return an error. This limitation is necessary since
        // 10 bits are used to identify the column index of the data in the 64-bit univariate id.
        if schema.fields.len() > 1024 {
            return Err(ModelarDbError::ConfigurationError(
                "There cannot be more than 1024 columns in the model table.".to_owned(),
            ));
        }

        // Remove the generated field columns from the ingestion schema as these should never be
        // provided as part of the record batches when inserting data points into the model table.
        let schema = Arc::new(schema);
        let ingestion_schema = if generated_columns.iter().any(Option::is_some) {
            // schema.fields() and generated_columns are known to be of equal length.
            let fields = schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(field_index, field)| {
                    if generated_columns[field_index].is_none() {
                        Some(field.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<Arc<Field>>>();

            Arc::new(Schema::new(fields))
        } else {
            schema.clone()
        };

        Ok(Self {
            name: table_name,
            schema,
            ingestion_schema,
            tag_column_indices,
            timestamp_column_index,
            error_bounds,
            generated_columns,
        })
    }
}

/// Column that is generated by a [`Expr`] using zero or more columns as input.
#[derive(Clone, Debug)]
pub struct GeneratedColumn {
    /// Logical expression that computes the values of the column.
    pub expr: Expr,
    /// Indices of the columns used by `expr` to compute the column's values.
    pub source_columns: Vec<usize>,
    /// Original representation of `expr`. It is copied from the SQL statement so it can be stored
    /// in the metadata database as `expr` does not implement serialization and deserialization.
    pub original_expr: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use modelardb_common::types::{ArrowTimestamp, ArrowValue};

    // Tests for ModelTableMetadata.
    #[test]
    fn test_can_create_model_table_metadata() {
        let (schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 2],
            error_bounds,
            generated_columns,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_timestamp_index_in_tag_indices() {
        let (schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            0,
            vec![0, 1, 2],
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_timestamp_index() {
        let (schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            10,
            vec![0, 1, 2],
            error_bounds,
            generated_columns,
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
        let (schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 10],
            error_bounds,
            generated_columns,
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
            vec![None],
        )
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_duplicate_tag_indices() {
        let (schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 1],
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_missing_or_too_many_error_bounds() {
        let (schema, _error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 2],
            vec![],
            generated_columns,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_missing_or_too_many_generated_columns() {
        let (schema, error_bounds, _generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 2],
            error_bounds,
            vec![],
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_generated_columns_using_generated_columns() {
        let (schema, error_bounds, mut generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();

        generated_columns[5] = Some(GeneratedColumn {
            expr: Expr::Wildcard,
            source_columns: vec![],
            original_expr: None,
        });

        generated_columns[6] = Some(GeneratedColumn {
            expr: Expr::Wildcard,
            source_columns: vec![5],
            original_expr: None,
        });

        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            3,
            vec![0, 1, 2],
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }

    fn model_table_schema_error_bounds_and_generated_columns(
    ) -> (Schema, Vec<ErrorBound>, Vec<Option<GeneratedColumn>>) {
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
                ErrorBound::try_new(0.0).unwrap(),
                ErrorBound::try_new(0.0).unwrap(),
                ErrorBound::try_new(0.0).unwrap(),
                ErrorBound::try_new(0.0).unwrap(),
            ],
            vec![None, None, None, None, None, None, None],
        )
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_too_many_fields() {
        // Create 1025 fields that can be used to initialize a schema.
        let fields = (0..1025)
            .map(|i| Field::new(format!("field_{i}").as_str(), DataType::Float32, false))
            .collect::<Vec<Field>>();

        let error_bounds = vec![
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
        ];

        let generated_columns = vec![None, None, None];

        let table_name = "table_name".to_owned();
        let result = ModelTableMetadata::try_new(
            table_name,
            Schema::new(fields),
            3,
            vec![0, 1, 2],
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }
}
