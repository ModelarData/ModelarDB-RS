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

use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Schema};
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Expr;
use modelardb_types::schemas::COMPRESSED_SCHEMA;
use modelardb_types::types::{ArrowTimestamp, ArrowValue, ErrorBound, TimestampArray, ValueArray};

use crate::error::{ModelarDbStorageError, Result};
use crate::parser::tokenize_and_parse_sql_expression;

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
    /// Schema of the data that can be read from the model table.
    pub query_schema: Arc<Schema>,
    /// Projection that changes `query_schema` to `schema`.
    pub query_schema_to_schema: Vec<usize>,
    /// Expressions to create generated columns in the `query_schema`. Only field columns can be
    /// generated by [`Expr`], so [`None`] is stored for timestamp, tag, and stored field columns.
    pub generated_columns: Vec<Option<GeneratedColumn>>,
    /// Schema of the compressed segments that are stored in the model table.
    pub compressed_schema: Arc<Schema>,
}

impl ModelTableMetadata {
    /// Create a new model table with the given metadata. If any of the following conditions are
    /// true, [`ModelarDbStorageError`] is returned:
    /// * The number of error bounds does not match the number of columns.
    /// * The number of potentially generated columns does not match the number of columns.
    /// * A generated column includes another generated column in its expression.
    /// * The `query_schema` does not include a single timestamp column.
    /// * The `query_schema` does not include at least one stored field column.
    pub fn try_new(
        name: String,
        query_schema: Arc<Schema>,
        error_bounds: Vec<ErrorBound>,
        generated_columns: Vec<Option<GeneratedColumn>>,
    ) -> Result<Self> {
        // If an error bound is not defined for each column, return an error.
        if query_schema.fields().len() != error_bounds.len() {
            return Err(ModelarDbStorageError::InvalidArgument(
                "An error bound must be defined for each column.".to_owned(),
            ));
        }

        // If a generated column or None is not defined for each column, return an error.
        if query_schema.fields().len() != generated_columns.len() {
            return Err(ModelarDbStorageError::InvalidArgument(
                "A generated column or None must be defined for each column.".to_owned(),
            ));
        }

        // If a generated field column depends on other generated field columns, return an error.
        for generated_column in generated_columns.iter().flatten() {
            for source_column in &generated_column.source_columns {
                if generated_columns[*source_column].is_some() {
                    return Err(ModelarDbStorageError::InvalidArgument(
                        "A generated field column cannot depend on generated field columns."
                            .to_owned(),
                    ));
                }
            }
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

        // A model table must only contain one stored timestamp column, one or more stored field
        // columns, zero or more generated field columns, and zero or more stored tag columns.
        let timestamp_column_indices = compute_indices_of_columns_with_data_type(
            &schema_without_generated,
            ArrowTimestamp::DATA_TYPE,
        );

        if timestamp_column_indices.len() != 1 {
            return Err(ModelarDbStorageError::InvalidArgument(
                "There needs to be exactly one timestamp column.".to_owned(),
            ));
        }

        let field_column_indices = compute_indices_of_columns_with_data_type(
            &schema_without_generated,
            ArrowValue::DATA_TYPE,
        );

        if field_column_indices.is_empty() {
            return Err(ModelarDbStorageError::InvalidArgument(
                "There needs to be at least one field column.".to_owned(),
            ));
        }

        let tag_column_indices =
            compute_indices_of_columns_with_data_type(&schema_without_generated, DataType::Utf8);

        // Add the tag columns to the base schema for compressed segments.
        let mut compressed_schema_fields = COMPRESSED_SCHEMA.0.fields.clone().to_vec();
        for index in &tag_column_indices {
            compressed_schema_fields.push(Arc::new(schema_without_generated.field(*index).clone()));
        }

        let compressed_schema = Arc::new(Schema::new(compressed_schema_fields));

        Ok(Self {
            name,
            schema: schema_without_generated,
            timestamp_column_index: timestamp_column_indices[0],
            field_column_indices,
            tag_column_indices,
            error_bounds: error_bounds_without_generated,
            query_schema,
            query_schema_to_schema: field_indices_without_generated,
            generated_columns,
            compressed_schema,
        })
    }

    /// Return `true` if the column at `index` is the timestamp column.
    pub fn is_timestamp(&self, index: usize) -> bool {
        index == self.timestamp_column_index
    }

    /// Return `true` if the column at `index` is a field column or a generated field column.
    pub fn is_field(&self, index: usize) -> bool {
        !self.is_timestamp(index) && !self.is_tag(index)
    }

    /// Return `true` if the column at `index` is a tag column.
    pub fn is_tag(&self, index: usize) -> bool {
        self.tag_column_indices.contains(&index)
    }

    /// Return the column arrays for the timestamp, field, and tag columns in `record_batch`. If
    /// `record_batch` does not contain the required columns, return [`ModelarDbStorageError`].
    pub fn column_arrays<'a>(
        &self,
        record_batch: &'a RecordBatch,
    ) -> Result<(
        &'a TimestampArray,
        Vec<&'a ValueArray>,
        Vec<&'a StringArray>,
    )> {
        if record_batch.schema() != self.schema {
            return Err(ModelarDbStorageError::InvalidArgument(
                "The record batch does not match the schema of the model table.".to_owned(),
            ));
        }

        let timestamp_column_array =
            modelardb_types::array!(record_batch, self.timestamp_column_index, TimestampArray);

        let field_column_arrays: Vec<_> = self
            .field_column_indices
            .iter()
            .map(|index| modelardb_types::array!(record_batch, *index, ValueArray))
            .collect();

        let tag_column_arrays: Vec<_> = self
            .tag_column_indices
            .iter()
            .map(|index| modelardb_types::array!(record_batch, *index, StringArray))
            .collect();

        Ok((
            timestamp_column_array,
            field_column_arrays,
            tag_column_arrays,
        ))
    }
}

/// Compute the indices of all columns in `schema` with `data_type`.
fn compute_indices_of_columns_with_data_type(schema: &Schema, data_type: DataType) -> Vec<usize> {
    let fields = schema.fields();
    (0..fields.len())
        .filter(|index| *fields[*index].data_type() == data_type)
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
    pub original_expr: String,
}

impl GeneratedColumn {
    /// Create a [`GeneratedColumn`] from a SQL expression and a [`DFSchema`]. If the SQL expression
    /// is not valid or refers to columns that are not in the [`DFSchema`],
    /// a [`ModelarDbStorageError`] is returned.
    pub fn try_from_sql_expr(sql_expr: &str, df_schema: &DFSchema) -> Result<Self> {
        let expr = tokenize_and_parse_sql_expression(sql_expr, df_schema)?;

        let source_columns: StdResult<Vec<usize>, DataFusionError> = expr
            .column_refs()
            .iter()
            .map(|column| df_schema.index_of_column(column))
            .collect();

        Ok(Self {
            expr,
            source_columns: source_columns?,
            original_expr: sql_expr.to_owned(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ToDFSchema;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::expr::WildcardOptions;
    use modelardb_common::test::ERROR_BOUND_ZERO;

    use crate::test;

    // Tests for ModelTableMetadata.
    #[test]
    fn test_can_create_model_table_metadata() {
        let (query_schema, error_bounds, generated_columns) =
            model_table_schema_error_bounds_and_generated_columns();
        let result = ModelTableMetadata::try_new(
            test::MODEL_TABLE_NAME.to_owned(),
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
    fn create_simple_model_table_metadata(query_schema: Schema) -> Result<ModelTableMetadata> {
        ModelTableMetadata::try_new(
            test::MODEL_TABLE_NAME.to_owned(),
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
            test::MODEL_TABLE_NAME.to_owned(),
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
            test::MODEL_TABLE_NAME.to_owned(),
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

        let wild_card_options = Box::new(WildcardOptions {
            ilike: None,
            exclude: None,
            except: None,
            replace: None,
            rename: None,
        });

        generated_columns[5] = Some(GeneratedColumn {
            expr: Expr::Wildcard {
                qualifier: None,
                options: wild_card_options.clone(),
            },
            source_columns: vec![],
            original_expr: "".to_owned(),
        });

        generated_columns[6] = Some(GeneratedColumn {
            expr: Expr::Wildcard {
                qualifier: None,
                options: wild_card_options,
            },
            source_columns: vec![5],
            original_expr: "".to_owned(),
        });

        let result = ModelTableMetadata::try_new(
            test::MODEL_TABLE_NAME.to_owned(),
            query_schema,
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }

    fn model_table_schema_error_bounds_and_generated_columns()
    -> (Arc<Schema>, Vec<ErrorBound>, Vec<Option<GeneratedColumn>>) {
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

        let result = ModelTableMetadata::try_new(
            test::MODEL_TABLE_NAME.to_owned(),
            Arc::new(Schema::new(fields)),
            error_bounds,
            generated_columns,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_is_timestamp() {
        let model_table_metadata = test::model_table_metadata();

        assert!(model_table_metadata.is_timestamp(0));
        assert!(!model_table_metadata.is_timestamp(1));
        assert!(!model_table_metadata.is_timestamp(2));
        assert!(!model_table_metadata.is_timestamp(3));
    }

    #[test]
    fn test_is_field() {
        let model_table_metadata = test::model_table_metadata();

        assert!(!model_table_metadata.is_field(0));
        assert!(model_table_metadata.is_field(1));
        assert!(model_table_metadata.is_field(2));
        assert!(!model_table_metadata.is_field(3));
    }

    #[test]
    fn test_is_tag() {
        let model_table_metadata = test::model_table_metadata();

        assert!(!model_table_metadata.is_tag(0));
        assert!(!model_table_metadata.is_tag(1));
        assert!(!model_table_metadata.is_tag(2));
        assert!(model_table_metadata.is_tag(3));
    }

    #[test]
    fn test_column_arrays() {
        let model_table_metadata = test::model_table_metadata();
        let record_batch = test::uncompressed_model_table_record_batch(1);

        let (timestamp_column_array, field_column_arrays, tag_column_arrays) =
            model_table_metadata.column_arrays(&record_batch).unwrap();

        assert_eq!(
            modelardb_types::array!(record_batch, 0, TimestampArray),
            timestamp_column_array
        );
        assert_eq!(
            modelardb_types::array!(record_batch, 1, ValueArray),
            field_column_arrays[0]
        );
        assert_eq!(
            modelardb_types::array!(record_batch, 2, ValueArray),
            field_column_arrays[1]
        );
        assert_eq!(
            modelardb_types::array!(record_batch, 3, StringArray),
            tag_column_arrays[0]
        );
    }

    #[test]
    fn test_column_arrays_with_invalid_schema() {
        let model_table_metadata = test::model_table_metadata();
        let record_batch = test::normal_table_record_batch();

        let result = model_table_metadata.column_arrays(&record_batch);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The record batch does not match the schema of the model table."
        );
    }

    // Tests for GeneratedColumn.
    #[test]
    fn test_can_create_generated_column() {
        let schema = Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("generated_column", ArrowValue::DATA_TYPE, false),
        ]);

        let sql_expr = "field_1 + field_2";
        let expected_generated_column = GeneratedColumn {
            expr: col("field_1") + col("field_2"),
            source_columns: vec![0, 1],
            original_expr: sql_expr.to_owned(),
        };

        let df_schema = schema.to_dfschema().unwrap();
        let mut result = GeneratedColumn::try_from_sql_expr(sql_expr, &df_schema).unwrap();

        // Sort the source columns to ensure the order is consistent.
        result.source_columns.sort();
        assert_eq!(expected_generated_column, result);
    }

    #[test]
    fn test_cannot_create_generated_column_with_invalid_sql_expr() {
        let schema = Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("generated_column", ArrowValue::DATA_TYPE, false),
        ]);

        let df_schema = schema.to_dfschema().unwrap();
        let result = GeneratedColumn::try_from_sql_expr("field_1 + field_2", &df_schema);

        assert!(result.is_err());
    }
}
