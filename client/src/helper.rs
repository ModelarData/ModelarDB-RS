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
use rustyline::completion::extract_word;
use rustyline::completion::Completer;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::Context;
use rustyline::Helper;

pub struct ClientHelper {
    completion_candidates: Vec<String>,
}

impl ClientHelper {
    pub fn new(table_names: Vec<String>) -> Self {
        ClientHelper {
            completion_candidates: vec![
                "AVG",
                "COUNT",
                "CROSS JOIN",
                "DISTINCT",
                "EXCEPT ALL",
                "EXCEPT",
                "FROM",
                "FULL JOIN",
                "FULL OUTER JOIN",
                "GROUP BY",
                "HAVING",
                "INNER JOIN",
                "INTERSECT ALL",
                "INTERSECT",
                "JOIN",
                "LEFT JOIN",
                "LEFT OUTER JOIN",
                "LIMIT",
                "MAX",
                "MIN",
                "ORDER BY",
                "RIGHT JOIN",
                "RIGHT OUTER JOIN",
                "SELECT",
                "SUM",
                "UNION ALL",
                "UNION",
                "WHERE",
            ]
            .iter()
            .map(|s| String::from(*s))
            .chain(table_names)
            .collect(),
        }
    }
}

impl Helper for ClientHelper {}

impl Completer for ClientHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> std::result::Result<(usize, Vec<Self::Candidate>), ReadlineError> {
        let (start, prefix) = extract_word(line, pos, None, " ".as_bytes());
        let uppercase_prefix = prefix.to_uppercase();
        let candidates: Vec<String> = self
            .completion_candidates
            .iter()
            .filter(|result| result.to_uppercase().starts_with(&uppercase_prefix))
            .map(String::from)
            .collect();
        Ok((start, candidates))
    }
}

impl Highlighter for ClientHelper {}

impl Hinter for ClientHelper {
    type Hint = String;
}

impl Validator for ClientHelper {}
