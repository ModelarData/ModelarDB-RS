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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::common::{DFSchema, DataFusionError, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::execution::context::ExecutionProps;
use datafusion::functions;
use datafusion::logical_expr::{AggregateUDF, Expr as DFExpr, ScalarUDF, TableSource, WindowUDF};
use datafusion::physical_expr::planner;
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::sql::TableReference;
use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, GeneratedAs,
    HiveDistributionStyle, HiveFormat, Ident, ObjectName, Statement, TimezoneInfo,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::{Keyword, ALL_KEYWORDS};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use crate::metadata::normalize_name;
use crate::types::{ArrowTimestamp, ArrowValue, ErrorBound};

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
                        let is_relative = parser.consume_token(&Token::Mod);
                        parser.expect_token(&Token::RParen)?;

                        // The error bound is zero by default so there is no need to store zero.
                        if error_bound > 0.0 {
                            options.push(Self::new_error_bound_column_option_def(
                                error_bound,
                                is_relative,
                            ));
                        }
                    } else if let Token::Word(_) = parser.peek_nth_token(0).token {
                        // An expression to generate the field is given.
                        self.expect_word_value(parser, "AS")?;

                        parser.expect_token(&Token::LParen)?;
                        let option = ColumnOption::Generated {
                            generated_as: GeneratedAs::Always,
                            sequence_options: None,
                            generation_expr: Some(parser.parse_expr()?),
                            generation_expr_mode: None,
                            generated_keyword: false,
                        };
                        parser.expect_token(&Token::RParen)?;

                        options.push(ColumnOptionDef { name: None, option });
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
    fn new_error_bound_column_option_def(error_bound: f32, is_relative: bool) -> ColumnOptionDef {
        // An error bound column option does not exist so ColumnOption::DialectSpecific and
        // Token::Number is used. Token::Number's bool should be the number when cast to a bool.
        ColumnOptionDef {
            name: None,
            option: ColumnOption::DialectSpecific(vec![Token::Number(
                error_bound.to_string(),
                is_relative,
            )]),
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
                serde_properties: None,
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
            comment: None,
            auto_increment_offset: None,
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
            order_by: None,
            partition_by: None,
            cluster_by: None,
            options: None,
            strict: false,
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
    statement: Statement,
) -> Result<ValidStatement, ParserError> {
    // Ensure it is a create table and only supported features are enabled.
    check_unsupported_features_are_disabled(&statement)?;

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
        let normalized_name = normalize_name(&name.0[0].value);
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

        // Create a ValidStatement with the information for creating the table of the specified type.
        let _expected_engine = CREATE_MODEL_TABLE_ENGINE.to_owned();
        if let Some(_expected_engine) = engine {
            // Create a model table for time series that only supports TIMESTAMP, FIELD, and TAG.
            let model_table_metadata =
                semantic_checks_for_create_model_table(normalized_name, columns)?;

            Ok(ValidStatement::CreateModelTable(model_table_metadata))
        } else {
            // Create a table that supports all columns types supported by Apache Arrow DataFusion.
            let context_provider = ParserContextProvider::new();
            let sql_to_rel = SqlToRel::new(&context_provider);
            let schema = sql_to_rel
                .build_schema(columns)
                .map_err(|error| ParserError::ParserError(error.to_string()))?;

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
    column_defs: Vec<ColumnDef>,
) -> Result<ModelTableMetadata, ParserError> {
    // Extract the error bounds for all columns. It is here to keep the parser types in the parser.
    let error_bounds = extract_error_bounds_for_all_columns(&column_defs)?;

    // Extract the expressions for all columns. It is here to keep the parser types in the parser.
    let generated_columns = extract_generation_exprs_for_all_columns(&column_defs)
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

    // Convert column definitions to a schema.
    let query_schema = column_defs_to_model_table_query_schema(column_defs)
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

    // Return the metadata required to create a model table.
    let model_table_metadata = ModelTableMetadata::try_new(
        name,
        Arc::new(query_schema),
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
        comment,
        auto_increment_offset,
        default_charset,
        collation,
        on_commit,
        on_cluster,
        order_by,
        partition_by,
        cluster_by,
        options,
        strict,
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
                    serde_properties: None,
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
        check_unsupported_feature_is_disabled(comment.is_some(), "Comment")?;
        check_unsupported_feature_is_disabled(auto_increment_offset.is_some(), "AUTO INCREMENT")?;
        check_unsupported_feature_is_disabled(default_charset.is_some(), "Charset")?;
        check_unsupported_feature_is_disabled(collation.is_some(), "Collation")?;
        check_unsupported_feature_is_disabled(on_commit.is_some(), "ON COMMIT")?;
        check_unsupported_feature_is_disabled(on_cluster.is_some(), "ON CLUSTER")?;
        check_unsupported_feature_is_disabled(order_by.is_some(), "ORDER BY")?;
        check_unsupported_feature_is_disabled(partition_by.is_some(), "PARTITION BY")?;
        check_unsupported_feature_is_disabled(cluster_by.is_some(), "CLUSTER BY")?;
        check_unsupported_feature_is_disabled(options.is_some(), "NAME=VALUE")?;
        check_unsupported_feature_is_disabled(*strict, "STRICT")?;
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

/// Return [`Schema`] if the types of the `column_defs` are supported by model tables, otherwise a
/// [`DataFusionError`] is returned.
fn column_defs_to_model_table_query_schema(
    column_defs: Vec<ColumnDef>,
) -> Result<Schema, DataFusionError> {
    let mut fields = Vec::with_capacity(column_defs.len());

    // Manually convert TIMESTAMP, FIELD, and TAG columns to Apache Arrow DataFusion types.
    for column_def in column_defs {
        let normalized_name = normalize_name(&column_def.name.value);

        let field = match column_def.data_type {
            SQLDataType::Timestamp(None, TimezoneInfo::None) => {
                Field::new(normalized_name, ArrowTimestamp::DATA_TYPE, false)
            }
            SQLDataType::Real => {
                let mut metadata: HashMap<String, String> = HashMap::with_capacity(2);
                for column_option_def in column_def.options {
                    match column_option_def.option {
                        ColumnOption::DialectSpecific(dialect_specific_tokens) => {
                            let (error_bound, is_relative) =
                                tokens_to_error_bound(&dialect_specific_tokens)?;
                            let suffix = if is_relative { "%" } else { "" };
                            metadata
                                .insert("Error Bound".to_owned(), error_bound.to_string() + suffix);
                        }
                        ColumnOption::Generated {
                            generated_as: _,
                            sequence_options: _,
                            generation_expr,
                            generation_expr_mode: _,
                            generated_keyword: _,
                        } => {
                            metadata.insert(
                                "Generated As".to_owned(),
                                generation_expr.unwrap().to_string(),
                            );
                        }
                        option => {
                            return Err(DataFusionError::Internal(format!(
                                "{option} is not supported in model tables."
                            )))
                        }
                    }
                }

                Field::new(normalized_name, ArrowValue::DATA_TYPE, false).with_metadata(metadata)
            }
            SQLDataType::Text => Field::new(normalized_name, DataType::Utf8, false),
            data_type => {
                return Err(DataFusionError::Internal(format!(
                    "{data_type} is not supported in model tables."
                )))
            }
        };

        fields.push(field);
    }

    Ok(Schema::new(fields))
}

/// Extract the error bounds from the columns in `column_defs`. The error bound for the timestamp
/// and tag columns will be zero so the error bound of each column can be accessed using its index.
fn extract_error_bounds_for_all_columns(
    column_defs: &[ColumnDef],
) -> Result<Vec<ErrorBound>, ParserError> {
    let mut error_bounds = Vec::with_capacity(column_defs.len());

    for column_def in column_defs {
        let mut error_bound_value = 0.0;
        let mut is_relative = false;

        for column_def_option in &column_def.options {
            if let ColumnOption::DialectSpecific(dialect_specific_tokens) =
                &column_def_option.option
            {
                (error_bound_value, is_relative) = tokens_to_error_bound(dialect_specific_tokens)?;
            }
        }

        let error_bound = if !is_relative {
            ErrorBound::try_new_absolute(error_bound_value)
        } else {
            ErrorBound::try_new_relative(error_bound_value)
        }
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

        error_bounds.push(error_bound);
    }

    Ok(error_bounds)
}

/// Return the value of an error bound and a [`bool`] indicating if it is relative if it is the only
/// token in `dialect_specific_tokens`, otherwise [`ParserError`] is returned. Assumes the tokens
/// have been extracted from a [`ColumnOption::DialectSpecific`].
fn tokens_to_error_bound(dialect_specific_tokens: &[Token]) -> Result<(f32, bool), ParserError> {
    if dialect_specific_tokens.len() != 1 {
        return Err(ParserError::ParserError(
            "Error bounds are currently the only supported dialect specific options.".to_owned(),
        ));
    }

    if let Token::Number(error_bound_string, is_relative) = &dialect_specific_tokens[0] {
        let error_bound_value = error_bound_string
            .parse::<f32>()
            .map_err(|_error| ParserError::ParserError("Error bound is not a float".to_owned()))?;
        Ok((error_bound_value, *is_relative))
    } else {
        Err(ParserError::ParserError(
            "Dialect specific tokens must be an error bound.".to_owned(),
        ))
    }
}

/// Extract the [`GeneratedColumn`] from the field columns in `column_defs`. The [`GeneratedColumn`]
/// for the timestamp columns, stored field columns, tag columns will be [`None`] so the
/// [`GeneratedColumn`] of each generated field column can be accessed using its column index.
fn extract_generation_exprs_for_all_columns(
    column_defs: &[ColumnDef],
) -> Result<Vec<Option<GeneratedColumn>>, DataFusionError> {
    let context_provider = ParserContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let schema = sql_to_rel.build_schema(column_defs.to_vec())?;
    let df_schema = schema.clone().to_dfschema()?;
    let mut planner_context = PlannerContext::new();
    let execution_props = ExecutionProps::new();

    let mut generated_columns = Vec::with_capacity(column_defs.len());

    for column_def in column_defs {
        let mut generated_column = None;

        for column_def_option in &column_def.options {
            if let ColumnOption::Generated {
                generated_as: _,
                sequence_options: _,
                generation_expr,
                generation_expr_mode: _,
                generated_keyword: _,
            } = &column_def_option.option
            {
                // The expression is saved as a string, so it can be stored in the metadata database,
                // it is not stored in ModelTableMetadata as it not used for during query execution.
                let sql_expr = generation_expr.as_ref().unwrap();
                let original_expr = Some(sql_expr.to_string());

                // Ensure that the parsed sqlparser expression can be converted to a logical Apache
                // Arrow DataFusion expression within the context of schema to check it for errors.
                let expr =
                    sql_to_rel.sql_to_expr(sql_expr.clone(), &df_schema, &mut planner_context)?;

                // Ensure the logical Apache Arrow DataFusion expression can be converted to a
                // physical Apache Arrow DataFusion expression within the context of schema. This is
                // to improve error messages if the user-defined expression has semantic errors.
                let _physical_expr =
                    planner::create_physical_expr(&expr, &df_schema, &execution_props)?;

                // unwrap() is safe as the loop iterates over the columns in the schema.
                let source_columns = expr
                    .to_columns()?
                    .iter()
                    .map(|column| df_schema.index_of_column(column).unwrap())
                    .collect();

                generated_column = Some(GeneratedColumn {
                    expr,
                    source_columns,
                    original_expr,
                });
            }
        }

        generated_columns.push(generated_column);
    }

    Ok(generated_columns)
}

/// Parse `sql_expr` into a [`DFExpr`] if it is a correctly formatted SQL arithmetic expression
/// that only references columns in [`DFSchema`], otherwise [`ParserError`] is returned.
pub fn parse_sql_expression(df_schema: &DFSchema, sql_expr: &str) -> Result<DFExpr, ParserError> {
    let context_provider = ParserContextProvider::new();
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
/// which was released under version 2.0 of the Apache License.
///
/// [rewrite_expr.rs]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/rewrite_expr.rs
struct ParserContextProvider {
    options: ConfigOptions,
    udfs: HashMap<String, Arc<ScalarUDF>>,
}

impl ParserContextProvider {
    fn new() -> Self {
        let mut functions = functions::all_default_functions();

        let udfs = functions
            .drain(0..)
            .map(|udf| (udf.name().to_owned(), udf))
            .collect::<HashMap<String, Arc<ScalarUDF>>>();

        ParserContextProvider {
            options: ConfigOptions::default(),
            udfs,
        }
    }
}

impl ContextProvider for ParserContextProvider {
    fn get_table_source(
        &self,
        _name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        Err(DataFusionError::Plan(
            "The table was not found.".to_string(),
        ))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        self.udfs.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }

    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
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
    fn test_tokenize_parse_semantic_check_create_model_table() {
        let sql = "CREATE MODEL TABLE table_name(timestamp TIMESTAMP,
                         field_one FIELD, field_two FIELD(10.5), field_three FIELD(1%),
                         field_four FIELD AS (CAST(SIN(CAST(field_one AS DOUBLE) * PI() / 180.0) AS REAL)),
                         tag TAG)";

        let statement = tokenize_and_parse_sql(sql).unwrap();
        if let Statement::CreateTable { name, columns, .. } = &statement {
            assert_eq!(*name, new_object_name("table_name"));
            let expected_columns = vec![
                ModelarDbDialect::new_column_def(
                    "timestamp",
                    SQLDataType::Timestamp(None, TimezoneInfo::None),
                    vec![],
                ),
                ModelarDbDialect::new_column_def("field_one", SQLDataType::Real, vec![]),
                ModelarDbDialect::new_column_def(
                    "field_two",
                    SQLDataType::Real,
                    new_column_option_def_error_bound_and_generation_expr(
                        Some((10.5, false)),
                        None,
                    ),
                ),
                ModelarDbDialect::new_column_def(
                    "field_three",
                    SQLDataType::Real,
                    new_column_option_def_error_bound_and_generation_expr(Some((1.0, true)), None),
                ),
                ModelarDbDialect::new_column_def(
                    "field_four",
                    SQLDataType::Real,
                    new_column_option_def_error_bound_and_generation_expr(
                        None,
                        Some("CAST(SIN(CAST(field_one AS DOUBLE) * PI() / 180.0) AS REAL)"),
                    ),
                ),
                ModelarDbDialect::new_column_def("tag", SQLDataType::Text, vec![]),
            ];
            assert_eq!(*columns, expected_columns);

            // unwrap() asserts that the semantic check have all passed as it otherwise panics.
            semantic_checks_for_create_table(statement).unwrap();
        } else {
            panic!("CREATE TABLE DDL did not parse to a Statement::CreateTable.");
        }
    }

    fn new_object_name(name: &str) -> ObjectName {
        ObjectName(vec![Ident::new(name)])
    }

    fn new_column_option_def_error_bound_and_generation_expr(
        maybe_error_bound: Option<(f32, bool)>,
        maybe_sql_expr: Option<&str>,
    ) -> Vec<ColumnOptionDef> {
        let mut column_option_defs = vec![];

        if let Some((error_bound, is_relative)) = maybe_error_bound {
            let dialect_specific_tokens = vec![Token::Number(error_bound.to_string(), is_relative)];
            let error_bound = ColumnOptionDef {
                name: None,
                option: ColumnOption::DialectSpecific(dialect_specific_tokens),
            };

            column_option_defs.push(error_bound);
        }

        if let Some(sql_expr) = maybe_sql_expr {
            let dialect = ModelarDbDialect::new();
            let mut parser = Parser::new(&dialect).try_with_sql(sql_expr).unwrap();
            let generation_expr = ColumnOptionDef {
                name: None,
                option: ColumnOption::Generated {
                    generated_as: GeneratedAs::Always,
                    sequence_options: None,
                    generation_expr: Some(parser.parse_expr().unwrap()),
                    generation_expr_mode: None,
                    generated_keyword: false,
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
        // Track if sqlparser at some point can parse fields/tags without ModelarDbDialect.
        assert!(tokenize_and_parse_sql(
            "CREATE TABLE table_name(timestamp TIMESTAMP, field FIELD, field_one FIELD(10.5),
                                     field_two FIELD(1%), tag TAG)",
        )
        .is_err());
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
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP AS (37), field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_tags() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG AS (37))",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_fields_without_parentheses() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD AS 37, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_fields_without_start_parentheses()
    {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD AS 37), tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_fields_without_end_parentheses() {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD AS (37, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_fields_with_absolute_error_bound()
    {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD(1.0) AS (37), tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_model_table_with_generated_fields_with_relative_error_bound()
    {
        assert!(tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field FIELD(1.0%) AS (37), tag TAG)",
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

            let result = semantic_checks_for_create_table(statement.unwrap());

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
            "COS(field_1 * PI() / 180)",
            "SIN(field_1 * PI() / 180)",
            "TAN(field_1 * PI() / 180)",
        ];

        let df_schema = Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("tag_1", DataType::Utf8, false),
        ])
        .to_dfschema()
        .unwrap();

        let dialect = ModelarDbDialect::new();
        for generation_expr in generation_exprs_sql {
            // Assert that the generation expressions can be reconstructed to a SQL expression.
            let mut parser = Parser::new(&dialect).try_with_sql(generation_expr).unwrap();
            assert_eq!(generation_expr, parser.parse_expr().unwrap().to_string());

            // Assert that the expression can be parsed to an Apache Arrow DataFusion expression.
            parse_sql_expression(&df_schema, generation_expr).unwrap();
        }
    }

    #[test]
    fn test_semantic_checks_for_create_model_table_check_correct_generated_expression() {
        let statement = tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD AS (COS(field_1 * PI() / 180)), tag TAG)",
        ).unwrap();

        assert!(semantic_checks_for_create_table(statement).is_ok());
    }

    #[test]
    fn test_semantic_checks_for_create_model_table_check_wrong_generated_expression() {
        let statement = tokenize_and_parse_sql(
            "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD AS (COS(field_3 * PI() / 180)), tag TAG)",
        ).unwrap();

        assert!(semantic_checks_for_create_table(statement).is_err());
    }
}
