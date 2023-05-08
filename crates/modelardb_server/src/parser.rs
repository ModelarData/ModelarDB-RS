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

use std::sync::Arc;

use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL_DEFAULT_SCALE,
};
use datafusion::common::{DFSchema, DataFusionError, Result as DataFusionResult, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{AggregateUDF, Expr as DFExpr, ScalarUDF, TableSource};
use datafusion::physical_expr::planner;
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::sql::TableReference;
use modelardb_common::types::ArrowTimestamp;
use modelardb_compression::models::ErrorBound;
use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, ExactNumberInfo,
    GeneratedAs, HiveDistributionStyle, HiveFormat, Ident, ObjectName, Statement, TimezoneInfo,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::{Keyword, ALL_KEYWORDS};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use crate::metadata::MetadataManager;

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

        // (column name and column type*).
        let columns = self.parse_columns(parser)?;

        // Return Statement::CreateTable with the extracted information.
        let name = ObjectName(vec![Ident::new(table_name)]);
        Ok(Self::new_create_model_table_statement(name, columns))
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
                    if parser.peek_token() == Token::LParen {
                        // An error bound is given for the field column.
                        parser.expect_token(&Token::LParen)?;
                        let error_bound = self.parse_positive_literal_f32(parser)?;
                        parser.expect_token(&Token::RParen)?;
                        options.push(Self::new_error_bound_column_option_def(error_bound));
                    } else if let Token::Word(_) = parser.peek_nth_token(0).token {
                        // An expression to generate the field is given.
                        self.expect_word_value(parser, "AS")?;

                        let option = ColumnOption::Generated {
                            generated_as: GeneratedAs::Always,
                            sequence_options: None,
                            generation_expr: Some(parser.parse_expr()?),
                        };

                        options.push(Self::new_error_bound_column_option_def(0.0));
                        options.push(ColumnOptionDef { name: None, option });
                    } else {
                        // Neither an error bound nor an expression is given.
                        options.push(Self::new_error_bound_column_option_def(0.0));
                    }

                    SQLDataType::Real
                }
                "TAG" => SQLDataType::Text,
                column_type => {
                    return Err(ParserError::ParserError(format!(
                        "Expected TIMESTAMP, FIELD, or TAG, found: {column_type}."
                    )))
                }
            };

            columns.push(Self::new_column_def(name.as_str(), data_type, options));

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

    /// Create a new [`ColumnOptionDef`] with the provided `error_bound`.
    fn new_error_bound_column_option_def(error_bound: f32) -> ColumnOptionDef {
        // An error bound column option does not exist so ColumnOption::Comment is used.
        ColumnOptionDef {
            name: None,
            option: ColumnOption::Comment(error_bound.to_string()),
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
#[derive(Debug)]
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

        // Check if the table name is a restricted keyword.
        let table_name_uppercase = normalized_name.to_uppercase();
        for keyword in ALL_KEYWORDS {
            if &table_name_uppercase == keyword {
                return Err(ParserError::ParserError(format!(
                    "Reserved keyword '{}' cannot be used as a table name.",
                    name
                )));
            }
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
            let model_table_metadata =
                semantic_checks_for_create_model_table(normalized_name, columns)?;
            Ok(ValidStatement::CreateModelTable(model_table_metadata))
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

    // Extract the error bounds for all columns. It is here to keep the parser types in the parser.
    let error_bounds = extract_error_bounds_for_all_columns(column_defs)?;

    // Extract the expressions for all columns. It is here to keep the parser types in the parser.
    let generated_columns = extract_generation_exprs_for_all_columns(column_defs)
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

    // Return the metadata required to create a model table.
    let model_table_metadata = ModelTableMetadata::try_new(
        name,
        Arc::new(schema),
        error_bounds,
        generated_columns,
    )
    .map_err(|error| ParserError::ParserError(error.to_string()))?;

    Ok(model_table_metadata)
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

/* Start of code copied from datafusion-sql v14.0.0/v15.0.0/v20.0.0/23.0.0. */
// The following two functions have been copied from datafusion-sql v14.0.0/v15.0.0/v20.0.0/23.0.0
// as parser.rs used the version of convert_simple_data_type() implemented as a free function in
// datafusion-sql v14.0.0 to convert from sqlparser::ast::DataType (SQLDataType) to
// datafusion::arrow::datatypes::DataType and it was changed to a private instance method in
// datafusion-sql v15.0.0. As Apache Arrow DataFusion no longer seems to provide an API for
// converting from SQLDataType to DataType, the version of convert_simple_data_type() in
// datafusion-sql v14.0.0 has been copied to parser.rs and has been updated according to the
// changes made in datafusion-sql v15.0.0, datafusion-sql v20.0.0, and datafusion-sql v23.0.0 to
// the greatest degree possible. As the private function make_decimal_type() is used by
// convert_simple_data_type() it has also been copied from datafusion-sql v15.0.0. As these
// functions have been copied from datafusion-sql they should be updated whenever a new version of
// datafusion-sql is released. Also significant effort should be made to try and replace these two
// functions with calls to Apache Arrow DataFusion's public API whenever a new version of Apache
// Arrow DataFusion is released.

/// Convert a simple [`SQLDataType`] to the relational representation of the [`DataType`]. This
/// function is copied from [datafusion-sql v14.0.0] and updated with the changes in
/// [datafusion-sql v15.0.0], [datafusion-sql v20.0.0], and [datafusion-sql v23.0.0] as it was
/// changed from a public function to a private method in [datafusion-sql v15.0.0] and extended in
/// [datafusion-sql v20.0.0] and [datafusion-sql v23.0.0]. All versions of datafusion-sql were
/// released under version 2.0 of the Apache License.
///
/// [datafusion-sql v14.0.0]: https://github.com/apache/arrow-datafusion/blob/14.0.0/datafusion/sql/src/planner.rs#L2812
/// [datafusion-sql v15.0.0]: https://github.com/apache/arrow-datafusion/blob/15.0.0/datafusion/sql/src/planner.rs#L2790
/// [datafusion-sql v20.0.0]: https://github.com/apache/arrow-datafusion/blob/20.0.0/datafusion/sql/src/planner.rs#L235
/// [datafusion-sql v23.0.0]: https://github.com/apache/arrow-datafusion/blob/23.0.0/datafusion/sql/src/planner.rs#L304
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
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)))
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
    let mut error_bounds = Vec::with_capacity(column_defs.len());

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

/// Extract the [`GeneratedColumn`] from the field columns in `column_defs`. The [`GeneratedColumn`]
/// for the timestamp columns, stored field columns, tag columns will be [`None`] so the
/// [`GeneratedColumn`] of each generated field column can be accessed using its column index.
fn extract_generation_exprs_for_all_columns(
    column_defs: &[ColumnDef],
) -> Result<Vec<Option<GeneratedColumn>>, DataFusionError> {
    let context_provider = EmptyContextProvider {
        options: ConfigOptions::default(),
    };
    let sql_to_rel = SqlToRel::new(&context_provider);
    let schema = sql_to_rel.build_schema(column_defs.to_vec())?;
    let df_schema = schema.clone().to_dfschema()?;
    let mut planner_context = PlannerContext::new();
    let execution_props = ExecutionProps::new();

    let mut generated_columns = Vec::with_capacity(column_defs.len());

    for column_def in column_defs {
        if column_def.data_type == SQLDataType::Real {
            if let Some(generated) = column_def.options.get(1) {
                match &generated.option {
                    ColumnOption::Generated {
                        generated_as: _,
                        sequence_options: _,
                        generation_expr,
                    } => {
                        // The expression is saved as a string so it can be stored in the metadata
                        // database, it is not in ModelTableMetadata as it not used for queries.
                        let sql_expr = generation_expr.as_ref().unwrap();
                        let original_expr = Some(sql_expr.to_string());

                        // Ensure the parsed sqlparser expression can be converted to a logical
                        // Apache Arrow DataFusion expression within the context of schema.
                        let expr = sql_to_rel.sql_to_expr(
                            sql_expr.clone(),
                            &df_schema,
                            &mut planner_context,
                        )?;

                        // Ensure the logical Apache Arrow DataFusion expression can be converted to
                        // a physical Apache Arrow DataFusion expression within the context of
                        // schema. This is done to ensure the user-defined expression is correct.
                        let _physical_expr = planner::create_physical_expr(
                            &expr,
                            &df_schema,
                            &schema,
                            &execution_props,
                        )?;

                        // unwrap() is safe as the loop iterates over the columns in the schema.
                        let source_columns = expr
                            .to_columns()?
                            .iter()
                            .map(|column| df_schema.index_of_column(column).unwrap())
                            .collect();

                        generated_columns.push(Some(GeneratedColumn {
                            expr,
                            source_columns,
                            original_expr,
                        }));
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "The second option is not an generation expression for {}.",
                            column_def.name.value
                        )));
                    }
                }
            } else {
                // A second option is not provided for this column.
                generated_columns.push(None);
            }
        } else {
            // column_def is a timestamp column or a tag column.
            generated_columns.push(None);
        }
    }

    Ok(generated_columns)
}

/// Parse `sql_expr` into a [`DFExpr`] if it is a correctly formatted SQL arithmetic expression
/// that only references columns in [`DFSchema`], otherwise [`ParserError`] is returned.
pub fn parse_sql_expression(df_schema: &DFSchema, sql_expr: &str) -> Result<DFExpr, ParserError> {
    let context_provider = EmptyContextProvider {
        options: ConfigOptions::default(),
    };
    let sql_to_rel = SqlToRel::new(&context_provider);
    let mut planner_context = PlannerContext::new();

    let dialect = ModelarDbDialect::new();
    let mut parser = Parser::new(&dialect).try_with_sql(sql_expr)?;
    let parsed_sql_expr = parser.parse_expr()?;
    sql_to_rel
        .sql_to_expr(parsed_sql_expr, df_schema, &mut planner_context)
        .map_err(|error| ParserError::ParserError(error.to_string()))
}

/// Empty context provider required for converting [`Expr`](sqlparser::ast::Expr) to [`DFExpr`]. It
/// is implemented based on [rewrite_expr.rs] in the Apache Arrow DataFusion GitHub repository
/// which were released under version 2.0 of the Apache License.
///
/// [rewrite_expr.rs]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/rewrite_expr.rs
struct EmptyContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for EmptyContextProvider {
    fn get_table_provider(
        &self,
        _name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        Err(DataFusionError::Plan(
            "The table was not found.".to_string(),
        ))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::metadata::test_util;

    // Tests for tokenize_and_parse_sql().
    #[test]
    fn test_tokenize_and_parse_empty_sql() {
        assert!(tokenize_and_parse_sql("").is_err());
    }

    #[test]
    fn test_tokenize_parse_semantic_check_create_model_table() {
        let sql = "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field_one FIELD, field_two FIELD(10.5),
                         field_three FIELD AS SIN(CAST(field_one AS DOUBLE) * PI() / 180.0), tag TAG)";

        let statement = tokenize_and_parse_sql(sql).unwrap();
        if let Statement::CreateTable { name, columns, .. } = &statement {
            assert_eq!(*name, new_object_name("table_name"));
            let expected_columns = vec![
                ModelarDbDialect::new_column_def(
                    "timestamp",
                    SQLDataType::Timestamp(None, TimezoneInfo::None),
                    vec![],
                ),
                ModelarDbDialect::new_column_def(
                    "field_one",
                    SQLDataType::Real,
                    new_column_option_def_error_bound_and_generation_expr(0.0, None),
                ),
                ModelarDbDialect::new_column_def(
                    "field_two",
                    SQLDataType::Real,
                    new_column_option_def_error_bound_and_generation_expr(10.5, None),
                ),
                ModelarDbDialect::new_column_def(
                    "field_three",
                    SQLDataType::Real,
                    new_column_option_def_error_bound_and_generation_expr(
                        0.0,
                        Some("SIN(CAST(field_one AS DOUBLE) * PI() / 180.0)"),
                    ),
                ),
                ModelarDbDialect::new_column_def("tag", SQLDataType::Text, vec![]),
            ];
            assert_eq!(*columns, expected_columns);

            // unwrap() asserts that the semantic check have all passed as it otherwise panics.
            semantic_checks_for_create_table(&statement).unwrap();
        } else {
            panic!("CREATE TABLE DDL did not parse to a Statement::CreateTable.");
        }
    }

    fn new_object_name(name: &str) -> ObjectName {
        ObjectName(vec![Ident::new(name)])
    }

    fn new_column_option_def_error_bound_and_generation_expr(
        error_bound: f32,
        maybe_sql_expr: Option<&str>,
    ) -> Vec<ColumnOptionDef> {
        let mut column_option_defs = vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::Comment(error_bound.to_string()),
        }];

        if let Some(sql_expr) = maybe_sql_expr {
            let dialect = ModelarDbDialect::new();
            let mut parser = Parser::new(&dialect).try_with_sql(sql_expr).unwrap();
            let generation_expr = ColumnOptionDef {
                name: None,
                option: ColumnOption::Generated {
                    generated_as: GeneratedAs::Always,
                    sequence_options: None,
                    generation_expr: Some(parser.parse_expr().unwrap()),
                },
            };

            column_option_defs.push(generation_expr);
        };

        column_option_defs
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
    fn test_tokenize_and_parse_create_model_table_with_generated_timestamps() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP AS 37, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_tags() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG AS 37)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_fields_with_error_bound() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD(1.0) AS 37, tag TAG)",
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
    fn test_tokenize_and_parse_create_table_with_lowercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_lowercase,
            "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
        )
    }

    #[test]
    fn test_tokenize_and_parse_create_table_with_uppercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_uppercase,
            "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
        )
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_lowercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_lowercase,
            "CREATE MODEL TABLE {}(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_uppercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_uppercase,
            "CREATE MODEL TABLE {}(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
    }

    fn parse_and_assert_that_keywords_are_restricted_in_table_name(
        keyword_to_table_name: fn(&str) -> String,
        sql: &str,
    ) {
        for keyword in ALL_KEYWORDS {
            // END-EXEC cannot be parsed by the SQL parser because of the hyphen, and is therefore
            // skipped in this test.
            if keyword == &"END-EXEC" {
                continue;
            }
            let statement = tokenize_and_parse_sql(
                sql.replace("{}", keyword_to_table_name(keyword).as_str())
                    .as_str(),
            );

            let result = semantic_checks_for_create_table(&statement.unwrap());

            assert!(result.is_err());

            assert_eq!(
                result.unwrap_err(),
                ParserError::ParserError(format!(
                    "Reserved keyword '{}' cannot be used as a table name.",
                    keyword_to_table_name(keyword)
                ))
            );
        }
    }

    #[test]
    fn test_tokenize_and_parse_can_reconstruct_sql_from_parsed_sql_expressions() {
        let generation_exprs_sql = vec![
            "37",
            "field_1 + 73",
            "field_1 + field_2",
            "cos(field_1 * pi() / 180)",
            "sin(field_1 * pi() / 180)",
            "tan(field_1 * pi() / 180)",
        ];

        let schemaref = test_util::model_table_metadata().schema;
        let df_schema = Arc::try_unwrap(schemaref).unwrap().to_dfschema().unwrap();

        let dialect = ModelarDbDialect::new();
        for generation_expr in generation_exprs_sql {
            // Assert that the generation expressions can be reconstructed to a SQL expression.
            let mut parser = Parser::new(&dialect).try_with_sql(generation_expr).unwrap();
            assert_eq!(generation_expr, parser.parse_expr().unwrap().to_string());

            // Assert that the expression can be parsed to a Apache Arrow DataFusion expression.
            parse_sql_expression(&df_schema, generation_expr).unwrap();
        }
    }

    #[test]
    fn test_semantic_checks_for_create_model_table_ensure_generated_as_expression_are_correct() {
        let statement = tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD AS field_1 + 3773.0, tag TAG)",
        ).unwrap();

        assert!(semantic_checks_for_create_table(&statement).is_err());
    }
}
