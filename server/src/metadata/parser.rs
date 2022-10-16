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

//! Methods for parsing Data Definition Language (DDL) expressions for creating
//! tables and model tables. The DDL expressions are tokenized and parsed using
//! [sqlparser] as it is already used by Apache Arrow DataFusion. Afterwards,
//! semantics checks are performed to ensure that the DDL expressions match what
//! is expected by the system and that they create a table or model table.
//!
//! [sqlparser]: https://crates.io/crates/sqlparser

use sqlparser::ast::{ColumnDef, DataType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

/// Parse the `sql` DDL expression, performs semantic checks, and return an AST.
/// Return the AST as a [`Vec<Statement>`] If `sql` can be parsed and the
/// semantic checks succeed, otherwise return a [`ParserError`].
pub fn parse_data_definition_language_expression(sql: &str) -> Result<Vec<Statement>, ParserError> {
    if sql.is_empty() {
        return Err(ParserError::TokenizerError(
            "Expected CREATE TABLE but received an empty string.".to_owned(),
        ));
    }

    let ast = Parser::parse_sql(&GenericDialect {}, sql)?;

    //if is_create_model_table
    Ok(ast)
}

/// Return [`true`] if `ast` contains one [`Statement`] and it is a CREATE TABLE
/// expression that creates a model table, otherwise [`false`].
pub fn ast_is_create_model_table(ast: &Vec<Statement>) -> bool {
    if ast.len() == 1 {
        if let Statement::CreateTable { columns, .. } = ast.get(0).unwrap() {
            return columns
                .iter()
                .all(|column_def| column_def_is_a_timestamp_field_or_tag(column_def));
        }
    }
    false
}

/// Return [`true`] if `column_def` is a timestamp, field, or a tag, otherwise
/// [`false`].
fn column_def_is_a_timestamp_field_or_tag(column_def: &ColumnDef) -> bool {
    match &column_def.data_type {
        DataType::Timestamp(_timezone) => true,
        DataType::Custom(object_name) => {
            let identifier = &object_name.0;
            if identifier.len() == 1 {
                let normalized_data_type_name = identifier.get(0).unwrap().value.to_lowercase();
                normalized_data_type_name == "field" || normalized_data_type_name == "tag"
            } else {
                false
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for ast_is_crate_model_table().
    #[test]
    fn test_ast_is_create_model_table_empty() {
        let ast = parse_data_definition_language_expression("");
        assert!(!ast_is_create_model_table(&ast));
    }

    #[test]
    fn test_ast_is_create_model_table_wrong_types() {
        let ast = parse_data_definition_language_expression(
            "CREATE TABLE table_name(timestamp TIMESTAMP, field REAL, tag VARCHAR)",
        );
        assert!(!ast_is_create_model_table(&ast));
    }

    #[test]
    fn test_ast_is_create_model_table_correct_lowercase_types() {
        let ast = parse_data_definition_language_expression(
            "CREATE TABLE table_name(timestamp timestamp, field field, tag tag)",
        );
        assert!(ast_is_create_model_table(&ast));
    }

    #[test]
    fn test_ast_is_create_model_table_correct_uppercase_types() {
        let ast = parse_data_definition_language_expression(
            "CREATE TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        );
        assert!(ast_is_create_model_table(&ast));
    }

    fn parse_data_definition_language_expression(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&GenericDialect {}, sql).unwrap()
    }
}
