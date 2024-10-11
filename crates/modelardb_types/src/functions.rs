/* Copyright 2024 The ModelarDB Contributors
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

//! Implementation of helper functions to operate on the types used through ModelarDB.

/// Extract the first 54-bits from `univariate_id` which is a hash computed from tags.
pub fn univariate_id_to_tag_hash(univariate_id: u64) -> u64 {
    univariate_id & 18446744073709550592
}

/// Extract the last 10-bits from `univariate_id` which is the index of the time series column.
pub fn univariate_id_to_column_index(univariate_id: u64) -> u16 {
    (univariate_id & 1023) as u16
}

/// Normalize `name` to allow direct comparisons between names.
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for normalize_name().
    #[test]
    fn test_normalize_table_name_lowercase_no_effect() {
        assert_eq!("table_name", normalize_name("table_name"));
    }

    #[test]
    fn test_normalize_table_name_uppercase() {
        assert_eq!("table_name", normalize_name("TABLE_NAME"));
    }

    #[test]
    fn test_normalize_table_name_mixed_case() {
        assert_eq!("table_name", normalize_name("Table_Name"));
    }
}
