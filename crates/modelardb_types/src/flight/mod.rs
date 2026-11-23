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

use crate::error::Result;
use crate::flight::protocol::table_metadata::column_metadata::Metadata;
use crate::functions::{try_convert_bytes_to_schema, try_convert_schema_to_bytes};
use crate::types::{ColumnMetadata, ErrorBound, GeneratedColumn, Node, ServerMode, TableMetadata};

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/modelardb.flight.protocol.rs"));
}

/// Encode `table_metadata` into a [`TableMetadata`](protocol::TableMetadata) protobuf message and
/// serialize it. If the schema or a generated column's expression cannot be converted to bytes,
/// return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn encode_and_serialize_table_metadata(table_metadata: &TableMetadata) -> Result<Vec<u8>> {
    let column_metadata = table_metadata
        .column_metadata
        .iter()
        .map(|column_metadata| {
            let metadata = match column_metadata {
                ColumnMetadata::Stored(ErrorBound::Absolute(value)) => {
                    Some(Metadata::AbsoluteErrorBound(*value))
                }
                ColumnMetadata::Stored(ErrorBound::Relative(percentage)) => {
                    Some(Metadata::RelativeErrorBound(*percentage))
                }
                ColumnMetadata::Stored(ErrorBound::Lossless) => None,
                ColumnMetadata::Generated(generated_column) => Some(Metadata::GeneratedColumnExpr(
                    generated_column.expr.to_bytes()?.to_vec(),
                )),
            };

            Ok(protocol::table_metadata::ColumnMetadata { metadata })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(protocol::TableMetadata {
        name: table_metadata.name.clone(),
        query_schema: try_convert_schema_to_bytes(&table_metadata.query_schema)?,
        column_metadata,
    }
    .encode_to_vec())
}

/// Deserialize `bytes` into a [`TableMetadata`](protocol::TableMetadata) protobuf message and
/// extract a [`TableMetadata`]. If `bytes` cannot be deserialized or the metadata is invalid,
/// return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn deserialize_and_extract_table_metadata(bytes: &[u8]) -> Result<TableMetadata> {
    let table_metadata = protocol::TableMetadata::decode(bytes)?;

    let query_schema = Arc::new(try_convert_bytes_to_schema(table_metadata.query_schema)?);
    let df_schema = query_schema.clone().to_dfschema()?;

    let column_metadata = table_metadata
        .column_metadata
        .into_iter()
        .map(|column_metadata| match column_metadata.metadata {
            Some(Metadata::AbsoluteErrorBound(value)) => {
                Ok(ColumnMetadata::Stored(ErrorBound::try_new_absolute(value)?))
            }
            Some(Metadata::RelativeErrorBound(percentage)) => Ok(ColumnMetadata::Stored(
                ErrorBound::try_new_relative(percentage)?,
            )),
            Some(Metadata::GeneratedColumnExpr(expr_bytes)) => {
                let expr = Expr::from_bytes(&expr_bytes)?;
                let generated_column = GeneratedColumn::try_from_expr(expr, &df_schema)?;
                Ok(ColumnMetadata::Generated(generated_column))
            }
            None => Ok(ColumnMetadata::Stored(ErrorBound::Lossless)),
        })
        .collect::<Result<Vec<_>>>()?;

    TableMetadata::try_new(table_metadata.name, query_schema, column_metadata)
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
/// [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
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

    use crate::types::{ArrowTimestamp, ArrowValue};

    const TABLE_NAME: &str = "test_table";
    const ERROR_BOUND_ONE: f32 = 1.0;

    // Tests for serializing and deserializing table metadata.
    #[test]
    fn test_serialize_and_deserialize_table_metadata() {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("absolute_value", ArrowValue::DATA_TYPE, false),
            Field::new("generated_value", ArrowValue::DATA_TYPE, false),
            Field::new("relative_value", ArrowValue::DATA_TYPE, false),
        ]));

        let df_schema = query_schema.clone().to_dfschema().unwrap();
        let generated_value = GeneratedColumn::try_from_expr(
            col("absolute_value") + col("relative_value"),
            &df_schema,
        )
        .unwrap();

        let expected_table_metadata = TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            query_schema,
            vec![
                ColumnMetadata::Stored(ErrorBound::Lossless),
                ColumnMetadata::Stored(ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap()),
                ColumnMetadata::Generated(generated_value),
                ColumnMetadata::Stored(ErrorBound::try_new_relative(ERROR_BOUND_ONE).unwrap()),
            ],
        )
        .unwrap();

        let bytes = encode_and_serialize_table_metadata(&expected_table_metadata).unwrap();
        let table_metadata = deserialize_and_extract_table_metadata(&bytes).unwrap();

        assert_eq!(table_metadata, expected_table_metadata);
    }

    #[test]
    fn test_cannot_deserialize_table_metadata_with_invalid_error_bound() {
        let schema = Schema::new(vec![Field::new("value", ArrowValue::DATA_TYPE, false)]);

        let bytes = protocol::TableMetadata {
            name: TABLE_NAME.to_owned(),
            query_schema: try_convert_schema_to_bytes(&schema).unwrap(),
            column_metadata: vec![protocol::table_metadata::ColumnMetadata {
                metadata: Some(Metadata::AbsoluteErrorBound(-ERROR_BOUND_ONE)),
            }],
        }
        .encode_to_vec();

        let result = deserialize_and_extract_table_metadata(&bytes);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: An absolute error bound must be a positive finite value."
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
