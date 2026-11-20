/* Copyright 2025 The ModelarDB Contributors
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

//! Implementation of helper functions to encode and decode types to and from the protobuf messages
//! defined in `flight/protocol.proto`. The module also provides functions to serialize and
//! deserialize encoded messages to and from bytes.

use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::ToDFSchema;
use datafusion::logical_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use prost::Message;

use crate::error::{ModelarDbTypesError, Result};
use crate::flight::protocol::table_metadata::ColumnMetadata;
use crate::flight::protocol::table_metadata::column_metadata::Metadata;
use crate::functions::{try_convert_bytes_to_schema, try_convert_schema_to_bytes};
use crate::types::{ErrorBound, GeneratedColumn, Node, ServerMode, TableMetadata};

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/modelardb.flight.protocol.rs"));
}

/// Encode `table_metadata` into a [`TableMetadata`](protocol::TableMetadata) protobuf message and
/// serialize it. If the schema or a generated column's expression cannot be converted to bytes,
/// return [`ModelarDbTypesError`].
pub fn encode_and_serialize_table_metadata(table_metadata: &TableMetadata) -> Result<Vec<u8>> {
    // Error bounds are only defined for stored columns, so the next error bound belongs to the next
    // column that is not generated.
    let mut error_bounds = table_metadata.error_bounds.iter();
    let mut column_metadata = Vec::with_capacity(table_metadata.generated_columns.len());

    for maybe_generated_column in &table_metadata.generated_columns {
        let metadata = if let Some(generated_column) = maybe_generated_column {
            Some(Metadata::GeneratedColumnExpr(
                generated_column.expr.to_bytes()?.to_vec(),
            ))
        } else {
            match error_bounds.next() {
                Some(ErrorBound::Absolute(value)) => Some(Metadata::AbsoluteErrorBound(*value)),
                Some(ErrorBound::Relative(percentage)) => {
                    Some(Metadata::RelativeErrorBound(*percentage))
                }
                Some(ErrorBound::Lossless) | None => None,
            }
        };

        column_metadata.push(ColumnMetadata { metadata });
    }

    Ok(protocol::TableMetadata {
        name: table_metadata.name.clone(),
        schema: try_convert_schema_to_bytes(&table_metadata.query_schema)?,
        column_metadata,
    }
    .encode_to_vec())
}

/// Deserialize `bytes` into a [`TableMetadata`](protocol::TableMetadata) protobuf message and
/// extract a [`TableMetadata`]. If `bytes` cannot be deserialized or the metadata is invalid,
/// return [`ModelarDbTypesError`].
pub fn deserialize_and_extract_table_metadata(bytes: &[u8]) -> Result<TableMetadata> {
    let table_metadata = protocol::TableMetadata::decode(bytes)?;

    let query_schema = Arc::new(try_convert_bytes_to_schema(table_metadata.schema)?);
    let df_schema = query_schema.clone().to_dfschema()?;

    let mut error_bounds = Vec::with_capacity(table_metadata.column_metadata.len());
    let mut generated_columns = Vec::with_capacity(table_metadata.column_metadata.len());

    for column_metadata in table_metadata.column_metadata {
        let (error_bound, maybe_generated_column) = match column_metadata.metadata {
            Some(Metadata::AbsoluteErrorBound(value)) => {
                (ErrorBound::try_new_absolute(value)?, None)
            }
            Some(Metadata::RelativeErrorBound(percentage)) => {
                (ErrorBound::try_new_relative(percentage)?, None)
            }
            Some(Metadata::GeneratedColumnExpr(expr_bytes)) => {
                let expr = Expr::from_bytes(&expr_bytes)?;
                let generated_column = GeneratedColumn::try_from_expr(expr, &df_schema)?;
                (ErrorBound::Lossless, Some(generated_column))
            }
            None => (ErrorBound::Lossless, None),
        };

        error_bounds.push(error_bound);
        generated_columns.push(maybe_generated_column);
    }

    TableMetadata::try_new(
        table_metadata.name,
        query_schema,
        error_bounds,
        generated_columns,
    )
}

/// Encode `nodes` into a [`ClusterNodes`](protocol::ClusterNodes) protobuf message and serialize it.
pub fn encode_and_serialize_cluster_nodes(nodes: Vec<Node>) -> Vec<u8> {
    let cluster_nodes = protocol::ClusterNodes {
        nodes: nodes
            .into_iter()
            .map(|node| protocol::NodeMetadata {
                url: node.url,
                mode: node.mode.to_string(),
            })
            .collect(),
    };

    cluster_nodes.encode_to_vec()
}

/// Deserialize `bytes` into a [`ClusterNodes`](protocol::ClusterNodes) protobuf message and extract
/// a vector of [`Node`]. If `bytes` cannot be deserialized or a node has an invalid mode, return
/// [`ModelarDbTypesError`].
pub fn deserialize_and_extract_cluster_nodes(bytes: &[u8]) -> Result<Vec<Node>> {
    let cluster_nodes = protocol::ClusterNodes::decode(bytes)?;

    cluster_nodes
        .nodes
        .into_iter()
        .map(|node| Ok(Node::new(node.url, ServerMode::from_str(&node.mode)?)))
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use datafusion::prelude::col;

    use modelardb_test::ERROR_BOUND_ONE;

    use crate::types::{ArrowTimestamp, ArrowValue};

    // Tests for serializing and deserializing table metadata.
    #[test]
    fn test_serialize_and_deserialize_table_metadata() {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("speed", ArrowValue::DATA_TYPE, false),
            Field::new("direction", ArrowValue::DATA_TYPE, false),
            Field::new("speed_squared", ArrowValue::DATA_TYPE, false),
        ]));

        let df_schema = query_schema.clone().to_dfschema().unwrap();
        let speed_squared =
            GeneratedColumn::try_from_expr(col("speed") * col("speed"), &df_schema).unwrap();

        let expected_table_metadata = TableMetadata::try_new(
            "test_table".to_owned(),
            query_schema,
            vec![
                ErrorBound::Lossless,
                ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap(),
                ErrorBound::try_new_relative(ERROR_BOUND_ONE).unwrap(),
                ErrorBound::Lossless,
            ],
            vec![None, None, None, Some(speed_squared)],
        )
        .unwrap();

        let bytes = encode_and_serialize_table_metadata(&expected_table_metadata).unwrap();
        let table_metadata = deserialize_and_extract_table_metadata(&bytes).unwrap();

        assert_eq!(table_metadata, expected_table_metadata);
    }

    #[test]
    fn test_cannot_deserialize_table_metadata_with_column_index_outside_schema() {
        let schema = Schema::new(vec![Field::new("speed", ArrowValue::DATA_TYPE, false)]);

        let bytes = protocol::TableMetadata {
            name: "test_table".to_owned(),
            schema: try_convert_schema_to_bytes(&schema).unwrap(),
            column_metadata: vec![protocol::table_metadata::ColumnMetadata {
                column_index: 1,
                error_bound_value: Some(ERROR_BOUND_ONE),
                error_bound_is_relative: false,
                generated_column_expr: None,
            }],
        }
        .encode_to_vec();

        let result = deserialize_and_extract_table_metadata(&bytes);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The column index 1 is not in the schema."
        );
    }

    // Test for serializing and deserializing cluster nodes.
    #[test]
    fn test_serialize_and_deserialize_cluster_nodes() {
        let expected_nodes = vec![
            Node::new("grpc://127.0.0.1:9999".to_owned(), ServerMode::Edge),
            Node::new("grpc://127.0.0.1:9998".to_owned(), ServerMode::Cloud),
        ];

        let bytes = encode_and_serialize_cluster_nodes(expected_nodes.clone());
        let nodes = deserialize_and_extract_cluster_nodes(&bytes).unwrap();

        assert_eq!(nodes, expected_nodes);
    }
}
