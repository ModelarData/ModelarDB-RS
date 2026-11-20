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

//! The types used throughout the system.
//!
//! Use declarations are purposely not used to minimize the chance of accidentally defining
//! incorrect aliases if types from different modules use the same name. It is assumed that each set
//! of aliases are all for the same underlying type.

use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{ArrowPrimitiveType, Schema};
use clap::Args;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::Expr;

use crate::error::{ModelarDbTypesError, Result};

// Types used for a single timestamp.
pub type Timestamp = std::primitive::i64; // It is signed to match TimestampMicrosecondType.
pub type ArrowTimestamp = arrow::datatypes::TimestampMicrosecondType;

// Types used for a collection of timestamps.
pub type TimestampBuilder = arrow::array::PrimitiveBuilder<ArrowTimestamp>;
pub type TimestampArray = arrow::array::PrimitiveArray<ArrowTimestamp>;

// Types used for a single value.
pub type Value = std::primitive::f32;
pub type ArrowValue = arrow::datatypes::Float32Type;

// Types used for a collection of values.
pub type ValueBuilder = arrow::array::PrimitiveBuilder<ArrowValue>;
pub type ValueArray = arrow::array::PrimitiveArray<ArrowValue>;

/// Maximum period in seconds that data can be retained in ModelarDB before it is deleted by a
/// VACUUM operation. The period is equal to the maximum value of an i64 in milliseconds.
/// The limitation is imposed by the use of `chrono::TimeDelta` when vacuuming data, which uses
/// an i64 internally to represent time in milliseconds.
pub const MAX_RETENTION_PERIOD_IN_SECONDS: u64 = (i64::MAX / 1000) as u64;

/// Metadata required to write data to a table and read data from a table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    /// Name of the table.
    pub name: String,
    /// Schema of the data that can be read from the table.
    pub query_schema: Arc<Schema>,
    /// Schema of the data that is written to and stored in the table. It is `query_schema` without
    /// the generated columns.
    pub schema: Arc<Schema>,
    /// Projection that changes `query_schema` to `schema`.
    pub query_schema_to_schema: Vec<usize>,
    /// Error bounds of the columns in `schema`. Only Float32 columns can have a lossy error bound.
    pub error_bounds: Vec<ErrorBound>,
    /// Expressions that generate the columns in `query_schema` that are not stored. [`None`] is
    /// stored for the columns that are in `schema`.
    pub generated_columns: Vec<Option<GeneratedColumn>>,
}

impl TableMetadata {
    /// Create metadata for a table. `error_bounds` and `generated_columns` must have an element for
    /// each column in `query_schema`. If any of the following conditions are true,
    /// [`ModelarDbTypesError`] is returned:
    /// * The number of error bounds does not match the number of columns.
    /// * The number of potentially generated columns does not match the number of columns.
    /// * A generated column has a lossy error bound.
    /// * A column that is not of type Float32 has a lossy error bound.
    /// * A generated column includes another generated column in its expression.
    /// * The `query_schema` does not include at least one stored column.
    pub fn try_new(
        name: String,
        query_schema: Arc<Schema>,
        error_bounds: Vec<ErrorBound>,
        generated_columns: Vec<Option<GeneratedColumn>>,
    ) -> Result<Self> {
        let number_of_columns = query_schema.fields().len();

        // If an error bound is not defined for each column, return an error.
        if error_bounds.len() != number_of_columns {
            return Err(ModelarDbTypesError::InvalidArgument(
                "An error bound must be defined for each column.".to_owned(),
            ));
        }

        // If a generated column or None is not defined for each column, return an error.
        if generated_columns.len() != number_of_columns {
            return Err(ModelarDbTypesError::InvalidArgument(
                "A generated column or None must be defined for each column.".to_owned(),
            ));
        }

        // If a lossy error bound is defined for a generated column or a column that is not of type
        // Float32, return an error. Values are only rewritten in stored Float32 columns.
        for ((field, error_bound), generated_column) in query_schema
            .fields()
            .iter()
            .zip(&error_bounds)
            .zip(&generated_columns)
        {
            if *error_bound == ErrorBound::Lossless {
                continue;
            }

            if generated_column.is_some() {
                return Err(ModelarDbTypesError::InvalidArgument(format!(
                    "The generated column '{}' cannot have an error bound.",
                    field.name()
                )));
            }

            if field.data_type() != &ArrowValue::DATA_TYPE {
                return Err(ModelarDbTypesError::InvalidArgument(format!(
                    "The column '{}' cannot have an error bound since its type '{}' is not Float32.",
                    field.name(),
                    field.data_type()
                )));
            }
        }

        // If a generated column depends on other generated columns, return an error.
        for generated_column in generated_columns.iter().flatten() {
            for source_column in &generated_column.source_columns {
                if generated_columns[*source_column].is_some() {
                    return Err(ModelarDbTypesError::InvalidArgument(
                        "A generated column cannot depend on generated columns.".to_owned(),
                    ));
                }
            }
        }

        // Remove the generated columns from the query schema and the error bounds since these
        // columns are never provided when writing data to the table.
        let query_schema_to_schema: Vec<usize> = generated_columns
            .iter()
            .enumerate()
            .filter_map(|(index, generated_column)| generated_column.is_none().then_some(index))
            .collect();

        if query_schema_to_schema.is_empty() {
            return Err(ModelarDbTypesError::InvalidArgument(
                "There needs to be at least one stored column.".to_owned(),
            ));
        }

        let schema = if query_schema_to_schema.len() == number_of_columns {
            query_schema.clone()
        } else {
            Arc::new(query_schema.project(&query_schema_to_schema)?)
        };

        let error_bounds = query_schema_to_schema
            .iter()
            .map(|index| error_bounds[*index])
            .collect();

        Ok(Self {
            name,
            query_schema,
            schema,
            query_schema_to_schema,
            error_bounds,
            generated_columns,
        })
    }
}

/// Absolute, relative, or lossless per-value error bound.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ErrorBound {
    /// An error bound that guarantees each value cannot deviate more than the [`Value`].
    Absolute(Value),
    /// An error bound that guarantees each value cannot deviate more than 0.0% to 100.0%.
    Relative(f32),
    /// An error bound that guarantees each value is stored losslessly.
    Lossless,
}

impl ErrorBound {
    /// Return an [`ErrorBound::Absolute`] with `value` as its absolute per-value bound. A
    /// [`ModelarDbTypesError`] is returned if a non-positive or non-normal value is passed.
    pub fn try_new_absolute(value: f32) -> Result<Self> {
        if !value.is_finite() || value <= 0.0 {
            Err(ModelarDbTypesError::InvalidArgument(
                "An absolute error bound must be a positive finite value.".to_owned(),
            ))
        } else {
            Ok(Self::Absolute(value))
        }
    }

    /// Return an [`ErrorBound::Relative`] with `percentage` as its relative per-value bound. A
    /// [`ModelarDbTypesError`] is returned if a value that is 0% or below is passed or a value
    /// above 100% is passed.
    pub fn try_new_relative(percentage: f32) -> Result<Self> {
        if !(0.0 < percentage && percentage <= 100.0) {
            Err(ModelarDbTypesError::InvalidArgument(
                "A relative error bound must be a positive value that is at most 100.0%."
                    .to_owned(),
            ))
        } else {
            Ok(Self::Relative(percentage))
        }
    }
}

/// Column that is generated by an [`Expr`] using zero or more stored columns as input.
#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedColumn {
    /// Logical expression that computes the values of the column.
    pub expr: Expr,
    /// Indices of the stored columns used by `expr` to compute the column's values.
    pub source_columns: Vec<usize>,
}

impl GeneratedColumn {
    /// Return a new [`GeneratedColumn`] with the given expression if it only references columns in
    /// [`DFSchema`], otherwise return [`ModelarDbTypesError`].
    pub fn try_from_expr(expr: Expr, df_schema: &DFSchema) -> Result<Self> {
        let source_columns: StdResult<Vec<usize>, DataFusionError> = expr
            .column_refs()
            .iter()
            .map(|column| df_schema.index_of_column(column))
            .collect();

        Ok(Self {
            expr,
            source_columns: source_columns?,
        })
    }
}

/// A single ModelarDB server. The node can either be an edge node or a cloud node.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// Apache Arrow Flight URL for the node. This URL uniquely identifies the node.
    pub url: String,
    /// The mode the node was started in.
    pub mode: ServerMode,
}

impl Node {
    pub fn new(url: String, mode: ServerMode) -> Self {
        Self { url, mode }
    }
}

/// The different possible modes of a ModelarDB server, assigned when the server is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMode {
    Cloud,
    Edge,
}

impl FromStr for ServerMode {
    type Err = ModelarDbTypesError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "cloud" => Ok(ServerMode::Cloud),
            "edge" => Ok(ServerMode::Edge),
            _ => Err(ModelarDbTypesError::InvalidArgument(format!(
                "'{value}' is not a valid value for ServerMode."
            ))),
        }
    }
}

impl fmt::Display for ServerMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServerMode::Cloud => write!(f, "cloud"),
            ServerMode::Edge => write!(f, "edge"),
        }
    }
}

/// Cloud credentials extracted from either a subcommand's flags or the corresponding environment
/// variables.
#[derive(Args, Default)]
pub struct CloudCredentials {
    /// Amazon S3 endpoint URL.
    #[arg(long, env = "AWS_ENDPOINT")]
    pub aws_endpoint: Option<String>,

    /// Amazon S3 access key ID.
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub aws_access_key_id: Option<String>,

    /// Amazon S3 secret access key.
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub aws_secret_access_key: Option<String>,

    /// Azure Blob Storage account name.
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
    pub azure_storage_account_name: Option<String>,

    /// Azure Blob Storage access key.
    #[arg(long, env = "AZURE_STORAGE_ACCESS_KEY")]
    pub azure_storage_access_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field};
    use datafusion::common::ToDFSchema;
    use datafusion::prelude::{col, lit};
    use proptest::property_test;

    use modelardb_test::{ERROR_BOUND_ONE, ERROR_BOUND_ZERO};

    const TABLE_NAME: &str = "test_table";

    // Tests for TableMetadata.
    #[test]
    fn test_can_create_table_metadata() {
        let query_schema = query_schema();
        let error_bounds = error_bounds();

        let table_metadata = TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            query_schema.clone(),
            error_bounds.clone(),
            generated_columns(&query_schema),
        )
        .unwrap();

        assert_eq!(table_metadata.query_schema, query_schema);
        assert_eq!(
            *table_metadata.schema,
            query_schema.project(&[0, 1, 3]).unwrap()
        );
        assert_eq!(table_metadata.query_schema_to_schema, vec![0, 1, 3]);
        assert_eq!(
            table_metadata.error_bounds,
            vec![error_bounds[0], error_bounds[1], error_bounds[3]]
        );
        assert_eq!(
            table_metadata.generated_columns,
            generated_columns(&query_schema)
        );
    }

    #[test]
    fn test_can_create_table_metadata_without_error_bounds_and_generated_columns() {
        let query_schema = query_schema();
        let number_of_columns = query_schema.fields().len();

        let table_metadata = TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            query_schema.clone(),
            vec![ErrorBound::Lossless; number_of_columns],
            vec![None; number_of_columns],
        )
        .unwrap();

        assert_eq!(table_metadata.schema, query_schema);
        assert_eq!(table_metadata.query_schema_to_schema, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_cannot_create_table_metadata_with_missing_or_too_many_error_bounds() {
        let query_schema = query_schema();

        assert_table_metadata_error(
            query_schema.clone(),
            vec![],
            generated_columns(&query_schema),
            "An error bound must be defined for each column.",
        );
    }

    #[test]
    fn test_cannot_create_table_metadata_with_missing_or_too_many_generated_columns() {
        assert_table_metadata_error(
            query_schema(),
            error_bounds(),
            vec![],
            "A generated column or None must be defined for each column.",
        );
    }

    #[test]
    fn test_cannot_create_table_metadata_with_error_bound_on_non_float32_column() {
        let query_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));

        assert_table_metadata_error(
            query_schema,
            vec![ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap()],
            vec![None],
            "The column 'value' cannot have an error bound since its type 'Float64' is not Float32.",
        );
    }

    #[test]
    fn test_cannot_create_table_metadata_with_error_bound_on_generated_column() {
        let query_schema = query_schema();
        let mut error_bounds = error_bounds();
        error_bounds[2] = ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap();

        assert_table_metadata_error(
            query_schema.clone(),
            error_bounds,
            generated_columns(&query_schema),
            "The generated column 'generated_value' cannot have an error bound.",
        );
    }

    #[test]
    fn test_cannot_create_table_metadata_with_generated_columns_using_generated_columns() {
        let query_schema = query_schema();
        let mut error_bounds = error_bounds();
        let mut generated_columns = generated_columns(&query_schema);

        error_bounds[3] = ErrorBound::Lossless;
        generated_columns[3] = Some(generated_column(col("generated_value"), &query_schema));

        assert_table_metadata_error(
            query_schema,
            error_bounds,
            generated_columns,
            "A generated column cannot depend on generated columns.",
        );
    }

    #[test]
    fn test_cannot_create_table_metadata_without_stored_columns() {
        let query_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowValue::DATA_TYPE,
            false,
        )]));
        let generated_columns = vec![Some(generated_column(lit(1.0_f32), &query_schema))];

        assert_table_metadata_error(
            query_schema,
            vec![ErrorBound::Lossless],
            generated_columns,
            "There needs to be at least one stored column.",
        );
    }

    fn assert_table_metadata_error(
        query_schema: Arc<Schema>,
        error_bounds: Vec<ErrorBound>,
        generated_columns: Vec<Option<GeneratedColumn>>,
        expected_message: &str,
    ) {
        let result = TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            query_schema,
            error_bounds,
            generated_columns,
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {expected_message}")
        );
    }

    fn query_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("absolute_value", ArrowValue::DATA_TYPE, false),
            Field::new("generated_value", ArrowValue::DATA_TYPE, false),
            Field::new("relative_value", ArrowValue::DATA_TYPE, false),
        ]))
    }

    fn error_bounds() -> Vec<ErrorBound> {
        vec![
            ErrorBound::Lossless,
            ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap(),
            ErrorBound::Lossless,
            ErrorBound::try_new_relative(ERROR_BOUND_ONE).unwrap(),
        ]
    }

    fn generated_columns(query_schema: &Arc<Schema>) -> Vec<Option<GeneratedColumn>> {
        vec![
            None,
            None,
            Some(generated_column(
                col("absolute_value") + col("relative_value"),
                query_schema,
            )),
            None,
        ]
    }

    fn generated_column(expr: Expr, query_schema: &Arc<Schema>) -> GeneratedColumn {
        let df_schema = query_schema.clone().to_dfschema().unwrap();
        GeneratedColumn::try_from_expr(expr, &df_schema).unwrap()
    }

    // Tests for ErrorBound.
    #[test]
    fn test_absolute_error_bound_cannot_be_zero() {
        assert_absolute_error_bound_error(ERROR_BOUND_ZERO);
    }

    #[property_test]
    fn test_absolute_error_bound_can_be_any_positive_value(
        #[strategy = f32::MIN_POSITIVE..=f32::MAX] value: f32,
    ) {
        assert!(ErrorBound::try_new_absolute(value).is_ok())
    }

    #[property_test]
    fn test_absolute_error_bound_cannot_be_negative(#[strategy = f32::MIN..0.0] value: f32) {
        assert_absolute_error_bound_error(value);
    }

    #[test]
    fn test_absolute_error_bound_cannot_be_positive_infinity() {
        assert_absolute_error_bound_error(f32::INFINITY);
    }

    #[test]
    fn test_absolute_error_bound_cannot_be_negative_infinity() {
        assert_absolute_error_bound_error(f32::NEG_INFINITY);
    }

    #[test]
    fn test_absolute_error_bound_cannot_be_nan() {
        assert_absolute_error_bound_error(f32::NAN);
    }

    fn assert_absolute_error_bound_error(value: f32) {
        let result = ErrorBound::try_new_absolute(value);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: An absolute error bound must be a positive finite value."
        );
    }

    #[test]
    fn test_relative_error_bound_cannot_be_zero() {
        assert_relative_error_bound_error(ERROR_BOUND_ZERO);
    }

    #[property_test]
    fn test_relative_error_bound_can_be_positive_if_less_than_one_hundred(
        #[strategy = 0.0..=f32::MAX] percentage: f32,
    ) {
        if percentage <= 100.0 {
            assert!(ErrorBound::try_new_relative(percentage).is_ok())
        } else {
            assert_relative_error_bound_error(percentage);
        }
    }

    #[property_test]
    fn test_relative_error_bound_cannot_be_negative(#[strategy = f32::MIN..0.0] percentage: f32) {
        assert_relative_error_bound_error(percentage);
    }

    #[test]
    fn test_relative_error_bound_cannot_be_positive_infinity() {
        assert_relative_error_bound_error(f32::INFINITY);
    }

    #[test]
    fn test_relative_error_bound_cannot_be_negative_infinity() {
        assert_relative_error_bound_error(f32::NEG_INFINITY);
    }

    #[test]
    fn test_relative_error_bound_cannot_be_nan() {
        assert_relative_error_bound_error(f32::NAN);
    }

    fn assert_relative_error_bound_error(value: f32) {
        let result = ErrorBound::try_new_relative(value);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: A relative error bound must be a positive value that is at most 100.0%."
        );
    }
}
