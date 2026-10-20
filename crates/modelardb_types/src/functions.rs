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

use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{IpcMessage, SchemaAsIpc};

use crate::error::{ModelarDbTypesError, Result};

/// Normalize `name` to allow direct comparisons between names.
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
}

/// Return the Apache Arrow schema as bytes if `schema` can be converted to [`Vec<u8>`], otherwise
/// return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn try_convert_schema_to_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(schema, &options);

    let ipc_message: IpcMessage = schema_as_ipc.try_into()?;

    Ok(ipc_message.0.to_vec())
}

/// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow schema, otherwise
/// return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn try_convert_bytes_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
    let ipc_message = IpcMessage(schema_bytes.into());
    Schema::try_from(ipc_message).map_err(|error| error.into())
}

/// Parse a value followed by a percentage or a unit (B, K, KB, KiB, M, MB, MiB, G, GB, GiB, T, TB,
/// TiB, case-insensitive).
/// - If `input` is a percentage and a total is given, that percentage of the total in bytes is returned.
/// - If `input` has a unit as a suffix, the specified amount of `input` in bytes is returned.
/// - If a parse error occurs it is returned.
pub fn parse_size_argument(input: &str, maybe_total: Option<u64>) -> Result<u64> {
    let (value, suffix) = split_at_unit(input)?;
    let bytes = if let Some(total) = maybe_total
        && suffix == "%"
    {
        ((total as f64 / 100.0) * value as f64) as u64
    } else {
        value * byte_unit_multiplier(suffix)?
    };

    Ok(bytes)
}

/// Return the number of bytes a single unit of `unit` represents if `unit` is a supported byte unit
/// (B, K, KB, KiB, M, MB, MiB, G, GB, GiB, T, TB, TiB, case-insensitive), otherwise
/// [`ModelarDbTypesError`] is returned.
pub fn byte_unit_multiplier(unit: &str) -> Result<u64> {
    match unit.to_uppercase().as_str() {
        "" => Ok(1),
        "B" => Ok(1),
        "KB" => Ok(1000),
        "K" | "KIB" => Ok(1024),
        "MB" => Ok(1000 * 1000),
        "M" | "MIB" => Ok(1024 * 1024),
        "GB" => Ok(1000 * 1000 * 1000),
        "G" | "GIB" => Ok(1024 * 1024 * 1024),
        "TB" => Ok(1000 * 1000 * 1000 * 1000),
        "T" | "TIB" => Ok(1024 * 1024 * 1024 * 1024),
        _ => Err(ModelarDbTypesError::InvalidArgument(format!(
            "Unit must be B, K, KB, KiB, M, MB, MiB, G, GB, GiB, T, TB, or TiB, not '{unit}'."
        ))),
    }
}

/// Parse a value followed by a unit (S, M, H, and D case-insensitive). If a parse error occurs it
/// is returned.
pub fn parse_time_argument(input: &str) -> Result<u64> {
    let (value, unit) = split_at_unit(input)?;
    let seconds = value * time_unit_multiplier(unit)?;
    Ok(seconds)
}

/// Return the number of seconds a single unit of `unit` represents if `unit` is a supported time
/// unit (S, M, H, or D, case-insensitive), otherwise [`ModelarDbTypesError`] is returned.
pub fn time_unit_multiplier(unit: &str) -> Result<u64> {
    match unit.to_uppercase().as_str() {
        "" => Ok(1),
        "S" => Ok(1),
        "M" => Ok(60),
        "H" => Ok(60 * 60),
        "D" => Ok(60 * 60 * 24),
        _ => Err(ModelarDbTypesError::InvalidArgument(format!(
            "Unit must be S, M, H, or D, not '{unit}'."
        ))),
    }
}

/// Split `input` into its value and unit.
fn split_at_unit(input: &str) -> Result<(u64, &str)> {
    let input = input.trim();
    let unit_start = input
        .char_indices()
        .find_map(|(index, character)| (!character.is_numeric()).then_some(index))
        .unwrap_or(input.len());

    let value: u64 = input[0..unit_start].parse::<u64>()?;
    let unit = input[unit_start..input.len()].trim();

    Ok((value, unit))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::ArrowPrimitiveType;
    use arrow::datatypes::Field;

    use crate::types::ArrowValue;

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

    // Tests for try_convert_schema_to_bytes() and try_convert_bytes_to_schema().
    #[test]
    fn test_schema_to_bytes_and_bytes_to_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]));

        // Serialize the schema to bytes.
        let bytes = try_convert_schema_to_bytes(&schema).unwrap();

        // Deserialize the bytes to the schema.
        let bytes_schema = try_convert_bytes_to_schema(bytes).unwrap();
        assert_eq!(*schema, bytes_schema);
    }

    #[test]
    fn test_invalid_bytes_to_schema() {
        let result = try_convert_bytes_to_schema(vec![1, 2, 4, 8]);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Arrow Error: Parser error: The buffer length (0) is less than the encapsulated message's reported length (134480385)"
        );
    }

    // Tests for parse_size_argument().
    #[test]
    fn test_parse_size_argument_empty_string() {
        assert_eq!(
            parse_size_argument("", Some(10240))
                .unwrap_err()
                .to_string(),
            "Parse Int Error: cannot parse integer from empty string"
        )
    }

    #[test]
    fn test_parse_size_argument_no_suffix() {
        assert_eq!(parse_size_argument("37", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_no_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_size_argument("  37  ", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_byte_suffix() {
        assert_eq!(parse_size_argument("37B", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_byte_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_size_argument(" 37   B  ", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_byte_suffix_with_lowercase() {
        assert_eq!(parse_size_argument("37b", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_kilobyte_suffix() {
        assert_eq!(parse_size_argument("37KB", Some(10240)).unwrap(), 37000)
    }

    #[test]
    fn test_parse_size_argument_kibibyte_suffix() {
        assert_eq!(parse_size_argument("37KiB", Some(10240)).unwrap(), 37888)
    }

    #[test]
    fn test_parse_size_argument_kibibyte_short_suffix() {
        assert_eq!(parse_size_argument("37K", None).unwrap(), 37888);
    }

    #[test]
    fn test_parse_size_argument_megabyte_suffix() {
        assert_eq!(parse_size_argument("37MB", Some(10240)).unwrap(), 37000000)
    }

    #[test]
    fn test_parse_size_argument_mebibyte_suffix() {
        assert_eq!(parse_size_argument("37MiB", Some(10240)).unwrap(), 38797312)
    }

    #[test]
    fn test_parse_size_argument_mebibyte_short_suffix() {
        assert_eq!(parse_size_argument("37M", None).unwrap(), 38797312);
    }

    #[test]
    fn test_parse_size_argument_gigabyte_suffix() {
        assert_eq!(
            parse_size_argument("37GB", Some(10240)).unwrap(),
            37000000000
        )
    }

    #[test]
    fn test_parse_size_argument_gibibyte_suffix() {
        assert_eq!(
            parse_size_argument("37GiB", Some(10240)).unwrap(),
            39728447488
        )
    }

    #[test]
    fn test_parse_size_argument_gibibyte_short_suffix() {
        assert_eq!(parse_size_argument("37G", None).unwrap(), 39728447488);
    }

    #[test]
    fn test_parse_size_argument_terabyte_suffix() {
        assert_eq!(
            parse_size_argument("37TB", Some(10240)).unwrap(),
            37000000000000
        )
    }

    #[test]
    fn test_parse_size_argument_tebibyte_suffix() {
        assert_eq!(
            parse_size_argument("37TiB", Some(10240)).unwrap(),
            40681930227712
        )
    }

    #[test]
    fn test_parse_size_argument_tebibyte_short_suffix() {
        assert_eq!(parse_size_argument("37T", None).unwrap(), 40681930227712);
    }

    #[test]
    fn test_parse_size_argument_percentage() {
        assert_eq!(parse_size_argument("10%", Some(10240)).unwrap(), 1024);
    }

    #[test]
    fn test_parse_size_argument_invalid_suffix() {
        assert_eq!(
            parse_size_argument("10#", Some(10240))
                .unwrap_err()
                .to_string(),
            "Invalid Argument Error: Unit must be B, K, KB, KiB, M, MB, MiB, G, GB, GiB, T, TB, or TiB, not '#'."
        )
    }

    #[test]
    fn test_parse_size_argument_invalid_space_in_suffix() {
        assert_eq!(
            parse_size_argument("10 M B", None).unwrap_err().to_string(),
            "Invalid Argument Error: Unit must be B, K, KB, KiB, M, MB, MiB, G, GB, GiB, T, TB, or TiB, not 'M B'."
        );
    }

    #[test]
    fn test_parse_size_argument_multiple_suffixes() {
        assert_eq!(
            parse_size_argument("10 MB GB", None)
                .unwrap_err()
                .to_string(),
            "Invalid Argument Error: Unit must be B, K, KB, KiB, M, MB, MiB, G, GB, GiB, T, TB, or TiB, not 'MB GB'."
        );
    }

    // Tests for parse_time_argument().
    #[test]
    fn test_parse_time_argument_empty_string() {
        assert_eq!(
            parse_time_argument("").unwrap_err().to_string(),
            "Parse Int Error: cannot parse integer from empty string"
        )
    }

    #[test]
    fn test_parse_time_argument_no_suffix() {
        assert_eq!(parse_time_argument("37").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_no_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_time_argument("  37  ").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_second_suffix() {
        assert_eq!(parse_time_argument("37S").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_second_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_time_argument(" 37   S  ").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_minute_suffix() {
        assert_eq!(parse_time_argument("37M").unwrap(), 2220)
    }

    #[test]
    fn test_parse_time_argument_hour_suffix() {
        assert_eq!(parse_time_argument("37H").unwrap(), 133200)
    }

    #[test]
    fn test_parse_time_argument_day_suffix() {
        assert_eq!(parse_time_argument("37D").unwrap(), 3196800)
    }

    #[test]
    fn test_parse_time_argument_invalid_suffix() {
        assert_eq!(
            parse_time_argument("10MS").unwrap_err().to_string(),
            "Invalid Argument Error: Unit must be S, M, H, or D, not 'MS'."
        )
    }
}
