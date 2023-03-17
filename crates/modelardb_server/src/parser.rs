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

//! Methods for tokenizing and parsing SQL commands. They are tokenized and
//! parsed using [sqlparser] as it is already used by Apache Arrow DataFusion.
//!
//! [sqlparser]: https://crates.io/crates/sqlparser

use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL_DEFAULT_SCALE,
};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use modelardb_common::types::ArrowTimestamp;
use modelardb_compression::models::ErrorBound;
use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, ExactNumberInfo,
    HiveDistributionStyle, HiveFormat, Ident, ObjectName, Statement, TimezoneInfo,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::{Keyword, ALL_KEYWORDS};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::metadata::{model_table_metadata::ModelTableMetadata, MetadataManager};

/// Constant specifying that a model table should be created.
pub const CREATE_MODEL_TABLE_ENGINE: &str = "ModelTable";

/// SQL dialect that extends `sqlparsers's` [`GenericDialect`] with support for
/// parsing CREATE MODEL TABLE table_name DDL commands.
#[derive(Debug)]
struct ModelarDbDialect {
    /// Dialect to use for identifying identifiers.
    dialect: GenericDialect,
}

impl ModelarDbDialect {
    fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Return [`true`] if the token stream starts with CREATE MODEL TABLE,
    /// otherwise [`false`] is returned. The method does not consume tokens.
    fn next_tokens_are_create_model_table(&self, parser: &Parser) -> bool {
        // CREATE.
        if let Token::Word(word) = parser.peek_nth_token(0).token {
            if word.keyword == Keyword::CREATE {
                // MODEL.
                if let Token::Word(word) = parser.peek_nth_token(1).token {
                    if word.value.to_uppercase() == "MODEL" {
                        // TABLE.
                        if let Token::Word(word) = parser.peek_nth_token(2).token {
                            if word.keyword == Keyword::TABLE {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// Parse CREATE MODEL TABLE table_name DDL commands to a
    /// [`Statement::CreateTable`]. A [`ParserError`] is returned if the column
    /// names and the column types cannot be parsed.
    fn parse_create_model_table(&self, parser: &mut Parser) -> Result<Statement, ParserError> {
        // CREATE MODEL TABLE.
        parser.expect_keyword(Keyword::CREATE)?;
        self.expect_word_value(parser, "MODEL")?;
        parser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_word_value(parser)?;

        // Check that the table name is not a restricted keyword.
        let table_name_uppercase = table_name.to_uppercase();
        for keyword in ALL_KEYWORDS {
            if &table_name_uppercase == keyword {
                return Err(ParserError::ParserError(format!(
                    "Reserved keyword '{}' cannot be used as a table name.",
                    table_name
                )));
            }
        }

        // (column name and column type*).
        let columns = self.parse_columns(parser)?;

        // Return Statement::CreateTable with the extracted information.
        let name = ObjectName(vec![Ident::new(table_name)]);
        Ok(ModelarDbDialect::new_create_model_table_statement(
            name, columns,
        ))
    }

    /// Parse (column name and type*) to a [`Vec<ColumnDef>`]. A [`ParserError`]
    /// is returned if the column names and the column types cannot be parsed.
    fn parse_columns(&self, parser: &mut Parser) -> Result<Vec<ColumnDef>, ParserError> {
        let mut columns = vec![];
        let mut parsed_all_columns = false;
        parser.expect_token(&Token::LParen)?;

        while !parsed_all_columns {
            let name = self.parse_word_value(parser)?;
            let data_type = self.parse_word_value(parser)?;
            let mut options = vec![];

            let data_type = match data_type.to_uppercase().as_str() {
                "TIMESTAMP" => SQLDataType::Timestamp(None, TimezoneInfo::None),
                "FIELD" => {
                    // An error bound may also be specified for field columns.
                    let error_bound = if parser.peek_token() == Token::LParen {
                        parser.expect_token(&Token::LParen)?;
                        let error_bound = self.parse_positive_literal_f32(parser)?;
                        parser.expect_token(&Token::RParen)?;
                        error_bound
                    } else {
                        0.0
                    };

                    // An error bound column option does not exist, so
                    // ColumnOption::Comment is used as a substitute.
                    options.push(ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Comment(error_bound.to_string()),
                    });

                    SQLDataType::Real
                }
                "TAG" => SQLDataType::Text,
                column_type => {
                    return Err(ParserError::ParserError(format!(
                        "Expected TIMESTAMP, FIELD, or TAG, found: {column_type}."
                    )))
                }
            };

            columns.push(ModelarDbDialect::new_column_def(
                name.as_str(),
                data_type,
                options,
            ));

            if parser.peek_token() == Token::RParen {
                parser.expect_token(&Token::RParen)?;
                parsed_all_columns = true;
            } else {
                parser.expect_token(&Token::Comma)?;
            }
        }

        Ok(columns)
    }

    /// Return [`Ok`] if the next [`Token`] is a [`Token::Word`] with the value
    /// `expected`, otherwise a [`ParserError`] is returned.
    fn expect_word_value(&self, parser: &mut Parser, expected: &str) -> Result<(), ParserError> {
        if let Ok(string) = self.parse_word_value(parser) {
            if string.to_uppercase() == expected.to_uppercase() {
                return Ok(());
            }
        }
        parser.expected(expected, parser.peek_token())
    }

    /// Return its value as a [`String`] if the next [`Token`] is a
    /// [`Token::Word`], otherwise a [`ParserError`] is returned.
    fn parse_word_value(&self, parser: &mut Parser) -> Result<String, ParserError> {
        let token_with_location = parser.next_token();
        match token_with_location.token {
            Token::Word(word) => Ok(word.value),
            _ => parser.expected("literal string", token_with_location),
        }
    }

    /// Return its value as a [`f32`] if the next [`Token`] is a
    /// [`Token::Number`], otherwise a [`ParserError`] is returned.
    fn parse_positive_literal_f32(&self, parser: &mut Parser) -> Result<f32, ParserError> {
        let token_with_location = parser.next_token();
        match token_with_location.token {
            Token::Number(maybe_f32, _) => maybe_f32.parse::<f32>().map_err(|error| {
                ParserError::ParserError(format!(
                    "Failed to parse '{maybe_f32}' into a positive f32 due to: {error}"
                ))
            }),
            _ => parser.expected("literal float", token_with_location),
        }
    }

    /// Create a new [`ColumnDef`] with the provided `column_name`, `data_type`,
    /// and `options`.
    fn new_column_def(
        column_name: &str,
        data_type: SQLDataType,
        options: Vec<ColumnOptionDef>,
    ) -> ColumnDef {
        ColumnDef {
            name: Ident::new(column_name),
            data_type,
            collation: None,
            options,
        }
    }

    /// Create a new [`Statement::CreateTable`] with the provided `table_name`
    /// and `columns`, and with `engine` set to [`CREATE_MODEL_TABLE_ENGINE`].
    fn new_create_model_table_statement(
        table_name: ObjectName,
        columns: Vec<ColumnDef>,
    ) -> Statement {
        // Designed to match the Statement::CreateTable created by sqlparser for
        // CREATE TABLE as closely as possible so semantic checks can be shared.
        Statement::CreateTable {
            or_replace: false,
            temporary: false,
            external: false,
            global: None,
            if_not_exists: false,
            transient: false,
            name: table_name,
            columns,
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: Some(HiveFormat {
                row_format: None,
                storage: None,
                location: None,
            }),
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: Some(CREATE_MODEL_TABLE_ENGINE.to_owned()),
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
            order_by: None,
        }
    }
}

impl Dialect for ModelarDbDialect {
    /// Return [`true`] if a character is a valid start character for an
    /// unquoted identifier, otherwise [`false`] is returned.
    fn is_identifier_start(&self, c: char) -> bool {
        self.dialect.is_identifier_start(c)
    }

    /// Return [`true`] if a character is a valid unquoted identifier character,
    /// otherwise [`false`] is returned.
    fn is_identifier_part(&self, c: char) -> bool {
        self.dialect.is_identifier_part(c)
    }

    /// Check if the next tokens are CREATE MODEL TABLE, if so, attempt to parse
    /// the token stream as a CREATE MODEL TABLE DDL command. If parsing
    /// succeeds, a [`Statement`] is returned, and if not, a [`ParserError`] is
    /// returned. If the next tokens are not CREATE MODEL TABLE, [`None`] is
    /// returned so sqlparser uses its parsing methods for all other commands.
    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if self.next_tokens_are_create_model_table(parser) {
            Some(self.parse_create_model_table(parser))
        } else {
            None
        }
    }
}

/// Tokenize and parse the SQL command in `sql` and return its parsed
/// representation in the form of [`Statements`](Statement).
pub fn tokenize_and_parse_sql(sql: &str) -> Result<Statement, ParserError> {
    let mut statements = Parser::parse_sql(&ModelarDbDialect::new(), sql)?;

    // Check that the sql contained a parseable statement.
    if statements.is_empty() {
        Err(ParserError::ParserError(
            "An empty string cannot be tokenized and parsed.".to_owned(),
        ))
    } else if statements.len() > 1 {
        Err(ParserError::ParserError(
            "Multiple SQL commands are not supported.".to_owned(),
        ))
    } else {
        Ok(statements.remove(0))
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, UPDATE, etc.) that have been
/// tokenized, parsed, and for which semantics checks have verified that it is
/// compatible with ModelarDB. CREATE TABLE and CREATE MODEL TABLE is supported.
pub enum ValidStatement {
    /// CREATE TABLE.
    CreateTable { name: String, schema: Schema },
    /// CREATE MODEL TABLE.
    CreateModelTable(ModelTableMetadata),
}

/// Perform semantic checks to ensure that the CREATE TABLE and CREATE MODEL
/// TABLE command in `statement` was correct. A [`ParserError`] is returned if
/// `statement` is not a [`Statement::CreateTable`] or a semantic check fails.
/// If all semantics checks are successful a [`ValidStatement`] is returned.
pub fn semantic_checks_for_create_table(
    statement: &Statement,
) -> Result<ValidStatement, ParserError> {
    // Ensure it is a create table and only supported features are enabled.
    check_unsupported_features_are_disabled(statement)?;

    // An else-clause is not required as statement's type was checked above.
    if let Statement::CreateTable {
        name,
        columns,
        engine,
        ..
    } = statement
    {
        // Extract the table name from the Statement::CreateTable.
        if name.0.len() > 1 {
            let message = "Multi-part table names are not supported.";
            return Err(ParserError::ParserError(message.to_owned()));
        }

        // Check if the table name contains whitespace, e.g., spaces or tabs.
        let normalized_name = MetadataManager::normalize_name(&name.0[0].value);
        if normalized_name.contains(char::is_whitespace) {
            let message = "Table name cannot contain whitespace.";
            return Err(ParserError::ParserError(message.to_owned()));
        }

        // Check if the table name is a valid object_store path and database table name.
        object_store::path::Path::parse(&normalized_name)
            .map_err(|error| ParserError::ParserError(error.to_string()))?;

        // Check if the columns can be converted to a schema.
        let schema = column_defs_to_schema(columns)
            .map_err(|error| ParserError::ParserError(error.to_string()))?;

        // Create a ValidStatement with the information for creating the table.
        let _expected_engine = CREATE_MODEL_TABLE_ENGINE.to_owned();
        if let Some(_expected_engine) = engine {
            Ok(ValidStatement::CreateModelTable(
                semantic_checks_for_create_model_table(normalized_name, columns)?,
            ))
        } else {
            Ok(ValidStatement::CreateTable {
                name: normalized_name,
                schema,
            })
        }
    } else {
        let message = "Expected CREATE TABLE or CREATE MODEL TABLE.";
        Err(ParserError::ParserError(message.to_owned()))
    }
}

/// Perform additional semantic checks to ensure that the CREATE MODEL TABLE
/// command from which `name` and `column_defs` was extracted was correct. A
/// [`ParserError`] is returned if any of the additional semantic checks fails.
fn semantic_checks_for_create_model_table(
    name: String,
    column_defs: &Vec<ColumnDef>,
) -> Result<ModelTableMetadata, ParserError> {
    // Convert column definitions to a schema.
    let schema = column_defs_to_schema(column_defs)
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

    // Check that one timestamp column exists.
    let timestamp_column_indices = compute_indices_of_columns_with_data_type(
        column_defs,
        SQLDataType::Timestamp(None, TimezoneInfo::None),
    );

    if timestamp_column_indices.len() != 1 {
        return Err(ParserError::ParserError(
            "A model table must contain one timestamp column.".to_owned(),
        ));
    }

    // Compute the indices of the tag columns.
    let tag_column_indices =
        compute_indices_of_columns_with_data_type(column_defs, SQLDataType::Text);

    // Extract the error bounds for all columns.
    let error_bounds = extract_error_bounds_for_all_columns(column_defs)?;

    // Return the metadata required to create a model table.
    ModelTableMetadata::try_new(
        name,
        schema,
        timestamp_column_indices[0],
        tag_column_indices,
        error_bounds,
    )
    .map_err(|error| ParserError::ParserError(error.to_string()))
}

/// Return [`ParserError`] if [`Statement`] is not a [`Statement::CreateTable`]
/// or if an unsupported feature is set.
fn check_unsupported_features_are_disabled(statement: &Statement) -> Result<(), ParserError> {
    if let Statement::CreateTable {
        or_replace,
        temporary,
        external,
        global,
        if_not_exists,
        transient,
        name: _name,
        columns: _columns,
        constraints,
        hive_distribution,
        hive_formats,
        table_properties,
        with_options,
        file_format,
        location,
        query,
        without_rowid,
        like,
        clone,
        engine: _engine,
        default_charset,
        collation,
        on_commit,
        on_cluster,
        order_by,
    } = statement
    {
        check_unsupported_feature_is_disabled(*or_replace, "OR REPLACE")?;
        check_unsupported_feature_is_disabled(*temporary, "TEMPORARY")?;
        check_unsupported_feature_is_disabled(*external, "EXTERNAL")?;
        check_unsupported_feature_is_disabled(global.is_some(), "GLOBAL")?;
        check_unsupported_feature_is_disabled(*if_not_exists, "IF NOT EXISTS")?;
        check_unsupported_feature_is_disabled(*transient, "TRANSIENT")?;
        check_unsupported_feature_is_disabled(!constraints.is_empty(), "CONSTRAINTS")?;
        check_unsupported_feature_is_disabled(
            hive_distribution != &HiveDistributionStyle::NONE,
            "Hive distribution",
        )?;
        check_unsupported_feature_is_disabled(
            *hive_formats
                != Some(HiveFormat {
                    row_format: None,
                    storage: None,
                    location: None,
                }),
            "Hive formats",
        )?;
        check_unsupported_feature_is_disabled(!table_properties.is_empty(), "Table properties")?;
        check_unsupported_feature_is_disabled(!with_options.is_empty(), "OPTIONS")?;
        check_unsupported_feature_is_disabled(file_format.is_some(), "File format")?;
        check_unsupported_feature_is_disabled(location.is_some(), "Location")?;
        check_unsupported_feature_is_disabled(query.is_some(), "Query")?;
        check_unsupported_feature_is_disabled(*without_rowid, "Without ROWID")?;
        check_unsupported_feature_is_disabled(like.is_some(), "LIKE")?;
        check_unsupported_feature_is_disabled(clone.is_some(), "CLONE")?;
        check_unsupported_feature_is_disabled(default_charset.is_some(), "Charset")?;
        check_unsupported_feature_is_disabled(collation.is_some(), "Collation")?;
        check_unsupported_feature_is_disabled(on_commit.is_some(), "ON COMMIT")?;
        check_unsupported_feature_is_disabled(on_cluster.is_some(), "ON CLUSTER")?;
        check_unsupported_feature_is_disabled(order_by.is_some(), "ORDER BY")?;
        Ok(())
    } else {
        let message = "Expected CREATE TABLE or CREATE MODEL TABLE.";
        Err(ParserError::ParserError(message.to_owned()))
    }
}

/// Return [`ParserError`] specifying that the functionality with the name
/// `feature` is not supported if `enabled` is [`true`].
fn check_unsupported_feature_is_disabled(enabled: bool, feature: &str) -> Result<(), ParserError> {
    if enabled {
        let message = format!("{feature} is not supported.");
        Err(ParserError::ParserError(message))
    } else {
        Ok(())
    }
}

/// Compute the indices of all columns in `column_defs` with `data_type`.
fn compute_indices_of_columns_with_data_type(
    column_defs: &[ColumnDef],
    data_type: SQLDataType,
) -> Vec<usize> {
    column_defs
        .iter()
        .enumerate()
        .filter(|(_index, column_def)| column_def.data_type == data_type)
        .map(|(index, _column_def)| index)
        .collect()
}

/// Return [`Schema`] if the types of the `column_defs` are supported, otherwise
/// a [`DataFusionError`] is returned.
fn column_defs_to_schema(column_defs: &Vec<ColumnDef>) -> Result<Schema, DataFusionError> {
    let mut fields = Vec::with_capacity(column_defs.len());

    for column_def in column_defs {
        let data_type = if column_def.data_type == SQLDataType::Timestamp(None, TimezoneInfo::None)
        {
            // TIMESTAMP is manually converted as planner uses TimeUnit::Nanosecond.
            ArrowTimestamp::DATA_TYPE
        } else {
            convert_simple_data_type(&column_def.data_type)?
        };

        fields.push(Field::new(
            MetadataManager::normalize_name(&column_def.name.value),
            data_type,
            false,
        ));
    }

    Ok(Schema::new(fields))
}

/* Start of code copied from datafusion-sql v14.0.0/v15.0.0/v20.0.0. */
// The following two functions have been copied from datafusion-sql v14.0.0/v15.0.0/v20.0.0 as
// parser.rs used the version of convert_simple_data_type() implemented as a free function in
// datafusion-sql v14.0.0 to convert from sqlparser::ast::DataType (SQLDataType) to
// datafusion::arrow::datatypes::DataType and it was changed to a private instance method in
// datafusion-sql v15.0.0. As Apache Arrow DataFusion no longer seems to provide an API for
// converting from SQLDataType to DataType, the version of convert_simple_data_type() in
// datafusion-sql v14.0.0 has been copied to parser.rs and has been updated according to the changes
// made in datafusion-sql v15.0.0 and datafusion-sql v20.0.0 to the greatest degree possible. As the
// private function make_decimal_type() is used by convert_simple_data_type() it has also been
// copied from datafusion-sql v15.0.0. As these functions have been copied from datafusion-sql they
// should be updated whenever a new version of datafusion-sql is released. Also significant effort
// should be made to try and replace these two functions with calls to Apache Arrow DataFusion's
// public API whenever a new version of Apache Arrow DataFusion is released.

/// Convert a simple [`SQLDataType`] to the relational representation of the [`DataType`]. This
/// function is copied from [datafusion-sql v14.0.0] and updated with the changes in [datafusion-sql
/// v15.0.0] and [datafusion-sql v20.0.0] as it was changed from a public function to a private
/// method in [datafusion-sql v15.0.0] and extended in [datafusion-sql v20.0.0]. All versions of
/// datafusion-sql were released under version 2.0 of the Apache License.
///
/// [datafusion-sql v14.0.0]: https://github.com/apache/arrow-datafusion/blob/14.0.0/datafusion/sql/src/planner.rs#L2812
/// [datafusion-sql v15.0.0]: https://github.com/apache/arrow-datafusion/blob/15.0.0/datafusion/sql/src/planner.rs#L2790
/// [datafusion-sql v20.0.0]: https://github.com/apache/arrow-datafusion/blob/20.0.0/datafusion/sql/src/planner.rs#L235
pub fn convert_simple_data_type(sql_type: &SQLDataType) -> DataFusionResult<DataType> {
    match sql_type {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::TinyInt(_) => Ok(DataType::Int8),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) | SQLDataType::Integer(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
        SQLDataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
        SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => Ok(DataType::UInt32),
        SQLDataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double | SQLDataType::DoublePrecision => Ok(DataType::Float64),
        SQLDataType::Char(_)
        | SQLDataType::Varchar(_)
        | SQLDataType::Text
        | SQLDataType::String => Ok(DataType::Utf8),
        SQLDataType::Timestamp(None, tz_info) => {
            let tz = if matches!(tz_info, TimezoneInfo::Tz)
                || matches!(tz_info, TimezoneInfo::WithTimeZone)
            {
                Some("+00:00".to_owned())
            } else {
                None
            };
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz))
        }
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time(None, tz_info) => {
            if matches!(tz_info, TimezoneInfo::None)
                || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            } else {
                // We don't support TIMETZ and TIME WITH TIME ZONE for now.
                Err(DataFusionError::NotImplemented(format!(
                    "Unsupported SQL type {sql_type:?}"
                )))
            }
        }
        SQLDataType::Numeric(exact_number_info)
        | SQLDataType::Decimal(exact_number_info) => {
            let (precision, scale) = match *exact_number_info {
                ExactNumberInfo::None => (None, None),
                ExactNumberInfo::Precision(precision) => (Some(precision), None),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (Some(precision), Some(scale))
                }
            };
            make_decimal_type(precision, scale)
        }
        SQLDataType::Bytea => Ok(DataType::Binary),
        // Explicitly list all other types so that if sqlparser
        // adds/changes the `SQLDataType` the compiler will tell us on upgrade
        // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059.
        SQLDataType::Nvarchar(_)
        | SQLDataType::JSON
        | SQLDataType::Uuid
        | SQLDataType::Binary(_)
        | SQLDataType::Varbinary(_)
        | SQLDataType::Blob(_)
        | SQLDataType::Datetime(_)
        | SQLDataType::Interval
        | SQLDataType::Regclass
        | SQLDataType::Custom(_, _)
        | SQLDataType::Array(_)
        | SQLDataType::Enum(_)
        | SQLDataType::Set(_)
        | SQLDataType::MediumInt(_)
        | SQLDataType::UnsignedMediumInt(_)
        | SQLDataType::Character(_)
        | SQLDataType::CharacterVarying(_)
        | SQLDataType::CharVarying(_)
        | SQLDataType::CharacterLargeObject(_)
        | SQLDataType::CharLargeObject(_)
        // Precision is not supported.
        | SQLDataType::Timestamp(Some(_), _)
        // Precision is not supported.
        | SQLDataType::Time(Some(_), _)
        | SQLDataType::Dec(_)
        | SQLDataType::BigNumeric(_)
        | SQLDataType::BigDecimal(_)
        | SQLDataType::Clob(_) => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL type {sql_type:?}"
        ))),
    }
}

/// Return a validated [`DataType`] for the specified `precision` and `scale`. This function is
/// copied from [datafusion-sql v15.0.0] which was released under version 2.0 of the Apache License.
///
/// [datafusion-sql v15.0.0]: https://github.com/apache/arrow-datafusion/blob/15.0.0/datafusion/sql/src/utils.rs#L506
fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> DataFusionResult<DataType> {
    // PostgreSQL like behavior.
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            return Err(DataFusionError::Internal(
                "Cannot specify only scale for decimal data type.".to_owned(),
            ))
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Apache Arrow decimal is i128 meaning 38 maximum decimal digits.
    if precision == 0 || precision > DECIMAL128_MAX_PRECISION || scale.unsigned_abs() > precision {
        Err(DataFusionError::Internal(format!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 38`, and `scale <= precision`."
        )))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}
/* End of code copied from datafusion-sql v14.0.0/v15.0.0. */

/// Extract the error bounds from the columns in `column_defs`. The error bound for the timestamp
/// and tag columns will be zero so the error bound of each column can be accessed using its index.
fn extract_error_bounds_for_all_columns(
    column_defs: &[ColumnDef],
) -> Result<Vec<ErrorBound>, ParserError> {
    let mut error_bounds = vec![];

    for column_def in column_defs {
        let error_bound_value = if column_def.data_type == SQLDataType::Real {
            match &column_def.options[0].option {
                ColumnOption::Comment(error_bound_string) => {
                    error_bound_string.parse::<f32>().unwrap()
                }
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "No error bound is defined for {}.",
                        column_def.name.value
                    )));
                }
            }
        } else {
            0.0
        };

        let error_bound = ErrorBound::try_new(error_bound_value)
            .map_err(|error| ParserError::ParserError(error.to_string()))?;

        error_bounds.push(error_bound);
    }

    Ok(error_bounds)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for tokenize_and_parse_sql().
    #[test]
    fn test_tokenize_and_parse_empty_sql() {
        assert!(tokenize_and_parse_sql("").is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table() {
        let sql = "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field_one FIELD, field_two FIELD(10.5), tag TAG)";
        if let Statement::CreateTable { name, columns, .. } = tokenize_and_parse_sql(sql).unwrap() {
            assert_eq!(name, new_object_name("table_name"));
            let expected_columns = vec![
                ModelarDbDialect::new_column_def(
                    "timestamp",
                    SQLDataType::Timestamp(None, TimezoneInfo::None),
                    vec![],
                ),
                ModelarDbDialect::new_column_def(
                    "field_one",
                    SQLDataType::Real,
                    new_column_option_def_error_bound(0.0),
                ),
                ModelarDbDialect::new_column_def(
                    "field_two",
                    SQLDataType::Real,
                    new_column_option_def_error_bound(10.5),
                ),
                ModelarDbDialect::new_column_def("tag", SQLDataType::Text, vec![]),
            ];
            assert!(columns == expected_columns);
        } else {
            panic!("CREATE TABLE DDL did not parse to a Statement::CreateTable.");
        }
    }

    fn new_object_name(name: &str) -> ObjectName {
        ObjectName(vec![Ident::new(name)])
    }

    fn new_column_option_def_error_bound(error_bound: f32) -> Vec<ColumnOptionDef> {
        vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::Comment(error_bound.to_string()),
        }]
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_create() {
        assert!(tokenize_and_parse_sql(
            "MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_create_model_space() {
        assert!(tokenize_and_parse_sql(
            "CREATEMODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_model() {
        // Checks if sqlparser can parse fields/tags without ModelarDbDialect.
        assert!(tokenize_and_parse_sql(
            "CREATE TABLE table_name(timestamp TIMESTAMP, field FIELD, field FIELD(10.5), tag TAG)",
        )
        .is_ok());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_model_table_space() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODELTABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_table_name() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_table_table_name_space() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLEtable_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_start_parentheses() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_option() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP PRIMARY KEY, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_sql_types() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field REAL, tag VARCHAR)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_column_name() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(TIMESTAMP, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_column_type() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_comma() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_without_end_parentheses() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_two_create_model_table_commands() {
        let error = tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG);
             CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        );

        assert!(error.is_err());

        let expected_error =
            ParserError::ParserError("Multiple SQL commands are not supported.".to_string());
        assert_eq!(error.unwrap_err(), expected_error);
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_lowercase_keyword_as_table_name() {
        for keyword in ALL_KEYWORDS {
            // END-EXEC cannot be parsed by the SQL parser because of the hyphen, and is therefore
            // skipped in this test.
            if keyword == &"END-EXEC" {
                continue;
            }
            let keyword_lowercase = keyword.to_lowercase();
            let error = tokenize_and_parse_sql(
                format!(
                    "CREATE MODEL TABLE {}(timestamp TIMESTAMP, field FIELD, tag TAG",
                    keyword_lowercase
                )
                .as_str(),
            );

            assert!(error.is_err());

            assert_eq!(
                error.unwrap_err(),
                ParserError::ParserError(
                    format!(
                        "Reserved keyword '{}' cannot be used as a table name.",
                        keyword_lowercase
                    )
                    .to_string()
                )
            );
        }
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_uppercase_keyword_as_table_name() {
        for keyword in ALL_KEYWORDS {
            // END-EXEC cannot be parsed by the SQL parser because of the hyphen, and is therefore
            // skipped in this test.
            if keyword == &"END-EXEC" {
                continue;
            }
            let keyword_uppercase = keyword.to_uppercase();
            let error = tokenize_and_parse_sql(
                format!(
                    "CREATE MODEL TABLE {}(timestamp TIMESTAMP, field FIELD, tag TAG",
                    keyword_uppercase
                )
                .as_str(),
            );

            assert!(error.is_err());

            assert_eq!(
                error.unwrap_err(),
                ParserError::ParserError(
                    format!(
                        "Reserved keyword '{}' cannot be used as a table name.",
                        keyword_uppercase
                    )
                    .to_string()
                )
            );
        }
    }
}
