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

use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Schema};
use datafusion::logical_expr::expr::Expr;

use crate::errors::ModelarDbError;
use crate::types::{ArrowTimestamp, ArrowValue, ErrorBound};

/// Metadata required to ingest data into a model table and query a model table.
#[derive(Debug, Clone)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Schema of the data that can be written to the model table.
    pub schema: Arc<Schema>,
    /// Index of the timestamp column in `schema`.
    pub timestamp_column_index: usize,
    /// Indices of the field columns in `schema`.
    pub field_column_indices: Vec<usize>,
    /// Indices of the tag columns in `schema`.
    pub tag_column_indices: Vec<usize>,
    /// Error bounds of the columns in `schema`. It can only be non-zero for field columns.
    pub error_bounds: Vec<ErrorBound>,
    /// Schema of the data that can be compressed in the model table.
    pub uncompressed_schema: Arc<Schema>,
    /// Schema of the data that can be read from the model table.
    pub query_schema: Arc<Schema>,
    /// Projection that changes `query_schema` to `schema`.
    pub query_schema_to_schema: Vec<usize>,
    /// Expressions to create generated columns in the `query_schema`. Only field columns can be
    /// generated by [`Expr`], so [`None`] is stored for timestamp, tag, and stored field columns.
    pub generated_columns: Vec<Option<GeneratedColumn>>,
}

impl ModelTableMetadata {
    /// Create a new model table with the given metadata. If any of the following conditions are
    /// true, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned:
    /// * The number of error bounds does not match the number of columns.
    /// * The number of potentially generated columns does not match the number of columns.
    /// * A generated column includes another generated column in its expression.
    /// * There are more than 1024 columns.
    /// * The `query_schema` does not include a single timestamp column.
    /// * The `query_schema` does not include at least one stored field column.
    pub fn try_new(
        name: String,
        query_schema: Arc<Schema>,
        error_bounds: Vec<ErrorBound>,
        generated_columns: Vec<Option<GeneratedColumn>>,
    ) -> Result<Self, ModelarDbError> {
        // If an error bound is not defined for each column, return an error.
        if query_schema.fields().len() != error_bounds.len() {
            return Err(ModelarDbError::ConfigurationError(
                "An error bound must be defined for each column.".to_owned(),
            ));
        }

        // If a generated column or None is not defined for each column, return an error.
        if query_schema.fields().len() != generated_columns.len() {
            return Err(ModelarDbError::ConfigurationError(
                "A generated column or None must be defined for each column.".to_owned(),
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

        // If there are more than 1024 columns, return an error. This limitation is necessary since
        // 10 bits are used to identify the column index of the data in the 64-bit univariate id.
        if query_schema.fields.len() > 1024 {
            return Err(ModelarDbError::ConfigurationError(
                "There cannot be more than 1024 columns in the model table.".to_owned(),
            ));
        }

        // Remove the generated field columns from the query schema and the error bounds as these
        // columns should never be provided when inserting data points into the model table.
        let mut fields_without_generated = Vec::with_capacity(query_schema.fields().len());
        let mut field_indices_without_generated = Vec::with_capacity(query_schema.fields().len());
        let mut error_bounds_without_generated = Vec::with_capacity(error_bounds.len());
        for (index, generated_column) in generated_columns.iter().enumerate() {
            if generated_column.is_none() {
                fields_without_generated.push(query_schema.fields[index].clone());
                field_indices_without_generated.push(index);
                error_bounds_without_generated.push(error_bounds[index]);
            }
        }

        let schema_without_generated =
            if query_schema.fields.len() != fields_without_generated.len() {
                Arc::new(Schema::new(fields_without_generated))
            } else {
                query_schema.clone()
            };

        // Schema containing timestamps and stored field columns for use by uncompressed buffers.
        let uncompressed_schema = Arc::new(
            schema_without_generated
                .project(&compute_indices_of_columns_without_data_type(
                    &schema_without_generated,
                    DataType::Utf8,
                ))
                .unwrap(),
        );

        // A model table must only contain one stored timestamp column, one or more stored field
        // columns, zero or more generated field columns, and zero or more stored tag columns.
        let timestamp_column_indices = compute_indices_of_columns_with_data_type(
            &schema_without_generated,
            ArrowTimestamp::DATA_TYPE,
        );

        if timestamp_column_indices.len() != 1 {
            return Err(ModelarDbError::ConfigurationError(
                "There needs to be exactly one timestamp column.".to_owned(),
            ));
        }

        let field_column_indices = compute_indices_of_columns_with_data_type(
            &schema_without_generated,
            ArrowValue::DATA_TYPE,
        );

        if field_column_indices.is_empty() {
            return Err(ModelarDbError::ConfigurationError(
                "There needs to be at least one field column.".to_owned(),
            ));
        }

        let tag_column_indices =
            compute_indices_of_columns_with_data_type(&schema_without_generated, DataType::Utf8);

        Ok(Self {
            name,
            schema: schema_without_generated,
            timestamp_column_index: timestamp_column_indices[0],
            field_column_indices,
            tag_column_indices,
            error_bounds: error_bounds_without_generated,
            uncompressed_schema,
            query_schema,
            query_schema_to_schema: field_indices_without_generated,
            generated_columns,
        })
    }
}

/// Compute the indices of all columns in `schema` with `data_type`.
fn compute_indices_of_columns_with_data_type(schema: &Schema, data_type: DataType) -> Vec<usize> {
    let fields = schema.fields();
    (0..fields.len())
        .filter(|index| *fields[*index].data_type() == data_type)
        .collect()
}

/// Compute the indices of all columns in `schema` without `data_type`.
fn compute_indices_of_columns_without_data_type(
    schema: &Schema,
    data_type: DataType,
) -> Vec<usize> {
    let fields = schema.fields();
    (0..fields.len())
        .filter(|index| *fields[*index].data_type() != data_type)
        .collect()
}

/// Column that is generated by a [`Expr`] using zero or more stored columns as input.
#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedColumn {
    /// Logical expression that computes the values of the column.
    pub expr: Expr,
    /// Indices of the stored columns used by `expr` to compute the column's values.
    pub source_columns: Vec<usize>,
    /// Original representation of `expr`. It is copied from the SQL statement, so it can be stored
    /// in the metadata Delta Lake as `expr` does not implement serialization and deserialization.
    pub original_expr: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use crate::test::ERROR_BOUND_ZERO;

    // Tests for ModelTableMetadata.
    #[test]
    fn test_can_create_model_table_metadata() {
        let (query_schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            query_schema,
            error_bounds,
            generated_columns,
        );

        assert!(result.is_ok());
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
        query_schema: Schema,
    ) -> Result<ModelTableMetadata, ModelarDbError> {
        ModelTableMetadata::try_new(
            "table_name".to_owned(),
            Arc::new(query_schema),
            vec![ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap()],
            vec![None],
        )
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_missing_or_too_many_error_bounds() {
        let (query_schema, _error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            query_schema,
            vec![],
            generated_columns,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_missing_or_too_many_generated_columns() {
        let (query_schema, error_bounds, _generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            query_schema,
            error_bounds,
            vec![],
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_generated_columns_using_generated_columns() {
        let (query_schema, error_bounds, mut generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();

        generated_columns[5] = Some(GeneratedColumn {
            expr: Expr::Wildcard { qualifier: None },
            source_columns: vec![],
            original_expr: None,
        });

        generated_columns[6] = Some(GeneratedColumn {
            expr: Expr::Wildcard { qualifier: None },
            source_columns: vec![5],
            original_expr: None,
        });

        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            query_schema,
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }

    fn model_table_schema_error_bounds_and_generated_columns(
    ) -> (Arc<Schema>, Vec<ErrorBound>, Vec<Option<GeneratedColumn>>) {
        (
            Arc::new(Schema::new(vec![
                Field::new("location", DataType::Utf8, false),
                Field::new("install_year", DataType::Utf8, false),
                Field::new("model", DataType::Utf8, false),
                Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
                Field::new("power_output", ArrowValue::DATA_TYPE, false),
                Field::new("wind_speed", ArrowValue::DATA_TYPE, false),
                Field::new("temperature", ArrowValue::DATA_TYPE, false),
            ])),
            vec![
                ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
                ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
                ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
                ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
                ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
                ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
                ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
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
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
        ];

        let generated_columns = vec![None, None, None];

        let table_name = "table_name".to_owned();
        let result = ModelTableMetadata::try_new(
            table_name,
            Arc::new(Schema::new(fields)),
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }
}
