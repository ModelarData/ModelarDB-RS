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

use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use datafusion::common::DataFusionError;
use datafusion_sql::planner;
use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType, HiveDistributionStyle, HiveFormat, Ident,
    ObjectName, Statement, TimezoneInfo,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::metadata::{model_table_metadata::ModelTableMetadata, MetadataManager};
use crate::models::ErrorBound;
use crate::types::ArrowTimestamp;

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
        if let Token::Word(word) = parser.peek_nth_token(0) {
            if word.keyword == Keyword::CREATE {
                // MODEL.
                if let Token::Word(word) = parser.peek_nth_token(1) {
                    if word.value.to_uppercase() == "MODEL" {
                        // TABLE.
                        if let Token::Word(word) = parser.peek_nth_token(2) {
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
                "TIMESTAMP" => DataType::Timestamp(TimezoneInfo::None),
                "FIELD" => {
                    // An error bound may also be specified for field columns.
                    let error_bound = if parser.peek_token() == Token::LParen {
                        parser.expect_token(&Token::LParen)?;
                        let error_bound = parser.parse_literal_uint()?;
                        parser.expect_token(&Token::RParen)?;
                        error_bound
                    } else {
                        0
                    };

                    // An error bound column option does not exist, so
                    // ColumnOption::Comment is used as a substitute.
                    options.push(ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Comment(error_bound.to_string()),
                    });

                    DataType::Real
                }
                "TAG" => DataType::Text,
                column_type => {
                    return Err(ParserError::ParserError(format!(
                        "Expected TIMESTAMP, FIELD, or TAG, found: {}.",
                        column_type
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
    /// `expected`, otherwise a [`ParseError`] is returned.
    fn expect_word_value(&self, parser: &mut Parser, expected: &str) -> Result<(), ParserError> {
        if let Ok(string) = self.parse_word_value(parser) {
            if string.to_uppercase() == expected.to_uppercase() {
                return Ok(());
            }
        }
        parser.expected(format!("{}", &expected).as_str(), parser.peek_token())
    }

    /// Return its value as a [`String`] if the next [`Token`] is a
    /// [`Token::Word`], otherwise a [`ParseError`] is returned.
    fn parse_word_value(&self, parser: &mut Parser) -> Result<String, ParserError> {
        match parser.next_token() {
            Token::Word(word) => Ok(word.value),
            unexpected => parser.expected("literal string", unexpected),
        }
    }

    /// Create a new [`ColumnDef`] with the provided `column_name`, `data_type`,
    /// and `options`.
    fn new_column_def(
        column_name: &str,
        data_type: DataType,
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
        }
    }
}

impl Dialect for ModelarDbDialect {
    /// Return [`True`] if a character is a valid start character for an
    /// unquoted identifier, otherwise [`False`] is returned.
    fn is_identifier_start(&self, c: char) -> bool {
        self.dialect.is_identifier_start(c)
    }

    /// Return [`True`] if a character is a valid unquoted identifier character,
    /// otherwise [`False`] is returned.
    fn is_identifier_part(&self, c: char) -> bool {
        self.dialect.is_identifier_part(c)
    }

    /// Check if the next tokens are CREATE TABLE TABLE, if so, attempt to parse
    /// the token stream as a CREATE MODEL TABLE DDL command. If parsing
    /// succeeds, a [`Statement`] is returned, and if not, a [`ParseError`] is
    /// returned. If the next tokens are not CREATE TABLE TABLE, [`None`] is
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
        let schema = column_defs_to_schema(&columns)
            .map_err(|error| ParserError::ParserError(error.to_string()))?;

        // Create a ValidStatement with the information for creating the table.
        let _expected_engine = CREATE_MODEL_TABLE_ENGINE.to_owned();
        if let Some(_expected_engine) = engine {
            return Ok(ValidStatement::CreateModelTable(
                semantic_checks_for_create_model_table(normalized_name, columns)?,
            ));
        } else {
            return Ok(ValidStatement::CreateTable {
                name: normalized_name,
                schema,
            });
        }
    } else {
        let message = "Expected CREATE TABLE or CREATE MODEL TABLE.";
        return Err(ParserError::ParserError(message.to_owned()));
    };
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
        &column_defs,
        DataType::Timestamp(TimezoneInfo::None),
    );

    if timestamp_column_indices.len() != 1 {
        return Err(ParserError::ParserError(
            "A model table must contain one timestamp column.".to_owned(),
        ));
    }

    // Compute the indices of the tag columns.
    let tag_column_indices =
        compute_indices_of_columns_with_data_type(&column_defs, DataType::Text);

    // Extract the error bounds for the field columns.
    let error_bounds = extract_error_bounds_for_field_columns(column_defs)?;

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
    } = statement
    {
        check_unsupported_feature_is_disabled(*or_replace, "OR REPLACE")?;
        check_unsupported_feature_is_disabled(*temporary, "TEMPORARY")?;
        check_unsupported_feature_is_disabled(*external, "EXTERNAL")?;
        check_unsupported_feature_is_disabled(global.is_some(), "GLOBAL")?;
        check_unsupported_feature_is_disabled(*if_not_exists, "IF NOT EXISTS")?;
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
        Ok(())
    } else {
        let message = "Expected CREATE TABLE or CREATE MODEL TABLE.";
        Err(ParserError::ParserError(message.to_owned()))
    }
}

/// Return [`ParserError`] specifying that the functionality with the name
/// `feature` is not supported if `enabled` is [`True`].
fn check_unsupported_feature_is_disabled(enabled: bool, feature: &str) -> Result<(), ParserError> {
    if enabled {
        let message = format!("{} is not supported.", feature);
        Err(ParserError::ParserError(message))
    } else {
        Ok(())
    }
}

/// Compute the indices of all columns in `column_defs` with `data_type`.
fn compute_indices_of_columns_with_data_type(
    column_defs: &Vec<ColumnDef>,
    data_type: DataType,
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
        let data_type = if column_def.data_type == DataType::Timestamp(TimezoneInfo::None) {
            // TIMESTAMP is manually converted as planner uses TimeUnit::Nanosecond.
            ArrowTimestamp::DATA_TYPE
        } else {
            planner::convert_simple_data_type(&column_def.data_type)?
        };

        fields.push(Field::new(
            &MetadataManager::normalize_name(&column_def.name.value),
            data_type,
            false,
        ));
    }

    Ok(Schema::new(fields))
}

/// Extract the error bounds from the fields columns in `column_defs`.
fn extract_error_bounds_for_field_columns(
    column_defs: &Vec<ColumnDef>,
) -> Result<Vec<ErrorBound>, ParserError> {
    let field_column_indices =
        compute_indices_of_columns_with_data_type(column_defs, DataType::Real);
    let mut error_bounds = vec![];

    for field_column_index in field_column_indices {
        let column_def = &column_defs[field_column_index];

        let error_bound_value = match &column_def.options[0].option {
            ColumnOption::Comment(error_bound_string) => error_bound_string.parse::<f32>().unwrap(),
            _ => {
                return Err(ParserError::ParserError(format!(
                    "No error bound is defined for {}.",
                    column_def.name.value
                )));
            }
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
        let sql = "CREATE MODEL TABLE table_name(timestamp TIMESTAMP, field_one FIELD, field_two FIELD(10), tag TAG)";
        if let Statement::CreateTable { name, columns, .. } = tokenize_and_parse_sql(&sql).unwrap()
        {
            assert!(name == new_object_name("table_name"));
            let expected_columns = vec![
                ModelarDbDialect::new_column_def(
                    "timestamp",
                    DataType::Timestamp(TimezoneInfo::None),
                    vec![],
                ),
                ModelarDbDialect::new_column_def(
                    "field_one",
                    DataType::Real,
                    new_column_option_def_error_bound(0),
                ),
                ModelarDbDialect::new_column_def(
                    "field_two",
                    DataType::Real,
                    new_column_option_def_error_bound(10),
                ),
                ModelarDbDialect::new_column_def("tag", DataType::Text, vec![]),
            ];
            assert!(columns == expected_columns);
        } else {
            panic!("CREATE TABLE DDL did not parse to a Statement::CreateTable.");
        }
    }

    fn new_object_name(name: &str) -> ObjectName {
        ObjectName(vec![Ident::new(name)])
    }

    fn new_column_option_def_error_bound(error_bound: usize) -> Vec<ColumnOptionDef> {
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
            "CREATE TABLE table_name(timestamp TIMESTAMP, field FIELD, field FIELD(10), tag TAG)",
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
            ParserError::ParserError("Multiple SQL commands are not supported.".to_owned());
        assert!(error.unwrap_err() == expected_error);
    }
}
