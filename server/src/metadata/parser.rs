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

use sqlparser::ast::{
    ColumnDef, DataType, HiveDistributionStyle, Ident, ObjectName, Statement, TimezoneInfo,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};

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

        // (column name and column type*)
        let columns = self.parse_columns(parser)?;

        // Return Statement::CreateTable with the extracted information.
        let name = ObjectName(vec![Ident::new(table_name)]);
        Ok(ModelarDbDialect::new_create_table_statement(name, columns))
    }

    /// Parse (column name and type*) to a [`Vec<ColumnDef>`]. A [`ParserError`]
    /// is returned if the column names and the column types cannot be parsed.
    fn parse_columns(&self, parser: &mut Parser) -> Result<Vec<ColumnDef>, ParserError> {
        let mut columns = vec![];
        let mut parsed_all_columns = false;
        parser.expect_token(&Token::LParen)?;

        while !parsed_all_columns {
            let column_name = self.parse_word_value(parser)?;
            let column_type = self.parse_word_value(parser)?;

            let data_type = match column_type.to_uppercase().as_str() {
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

                    DataType::Float(Some(error_bound))
                }
                "TAG" => DataType::Text,
                column_type => {
                    return Err(ParserError::ParserError(format!(
                        "Expected TIMESTAMP, FIELD, or TAG, found: {}",
                        column_type
                    )))
                }
            };

            columns.push(ModelarDbDialect::new_column_def(column_name, data_type));

            if parser.peek_token() == Token::RParen {
                parser.expect_token(&Token::RParen)?;
                parsed_all_columns = true;
            } else {
                parser.expect_token(&Token::Comma)?;
            }
        }

        Ok(columns)
    }

    /// Return [`()`] if the next [`Token`] is a [`Token::Word`] with the value
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

    /// Create a new [`ColumnDef`] with the provided `column_name` and
    /// `data_type`.
    fn new_column_def(column_name: String, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident::new(column_name),
            data_type,
            collation: None,
            options: vec![],
        }
    }

    /// Create a new [`Statement::CreateTable`] with the provided `table_name`
    /// and `columns`.
    fn new_create_table_statement(table_name: ObjectName, columns: Vec<ColumnDef>) -> Statement {
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
            hive_formats: None,
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: None,
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
    /// the token stream as a CREATE MODEL TABLE DDL command. If parsing is
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

/// Tokenize and parse the first SQL command in `sql` and return its parsed
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
                new_column_def("timestamp", DataType::Timestamp(TimezoneInfo::None)),
                new_column_def("field_one", DataType::Float(Some(0))),
                new_column_def("field_two", DataType::Float(Some(10))),
                new_column_def("tag", DataType::Text),
            ];
            assert!(columns == expected_columns);
        } else {
            panic!("CREATE TABLE DDL did not parse to a Statement::CreateTable");
        }
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
    fn test_tokenize_and_parse_create_model_table_without_table__table_name_space() {
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

    fn new_object_name(name: &str) -> ObjectName {
        ObjectName(vec![Ident::new(name)])
    }

    fn new_column_def(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident::new(name),
            data_type,
            collation: None,
            options: vec![],
        }
    }
}
