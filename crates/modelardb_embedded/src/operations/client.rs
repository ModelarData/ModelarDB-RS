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

//! Operations for interacting with ModelarDB Apache Arrow Flight servers.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::convert;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Criteria, FlightDescriptor, Ticket};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, stream};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::error::{ModelarDbEmbeddedError, Result};
use crate::operations::{
    ModelarDBType, Operations, generate_read_time_series_table_sql,
    try_new_time_series_table_metadata,
};
use crate::{Aggregate, TableType};

/// Client for connecting to ModelarDB Apache Arrow Flight servers.
#[derive(Clone)]
pub struct Client {
    /// Apache Arrow Flight client connected to the Apache Arrow Flight server of the ModelarDB node.
    pub(crate) flight_client: FlightServiceClient<Channel>,
}

impl Client {
    /// Create a new [`Client`] that is connected to the node with `url`. If a connection
    /// to the node could not be established, [`ModelarDbEmbeddedError`] is returned.
    pub async fn connect(url: &str) -> Result<Client> {
        let connection = Endpoint::new(url.to_owned())?.connect().await?;
        let flight_client = FlightServiceClient::new(connection);

        Ok(Client { flight_client })
    }
}

#[async_trait]
impl Operations for Client {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the type of the ModelarDB node that the client is connected to.
    async fn modelardb_type(&mut self) -> Result<ModelarDBType> {
        // Retrieve the node type from the ModelarDB node.
        let action = Action {
            r#type: "NodeType".to_owned(),
            body: vec![].into(),
        };

        let response = self.flight_client.do_action(Request::new(action)).await?;

        let message = response
            .into_inner()
            .message()
            .await?
            .expect("Flight message should exist.");

        ModelarDBType::from_str(str::from_utf8(&message.body)?)
    }

    /// Creates a table with the name in `table_name` and the information in `table_type`. If the
    /// table already exists or if the table could not be created, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn create(&mut self, table_name: &str, table_type: TableType) -> Result<()> {
        // Convert the table metadata to a protobuf message that can be sent to ModelarDB.
        let protobuf_bytes = match table_type {
            TableType::NormalTable(schema) => {
                modelardb_types::flight::encode_and_serialize_normal_table_metadata(
                    table_name, &schema,
                )?
            }
            TableType::TimeSeriesTable(schema, error_bounds, generated_columns) => {
                let time_series_table_metadata = try_new_time_series_table_metadata(
                    table_name,
                    schema.clone(),
                    error_bounds,
                    generated_columns,
                )?;

                modelardb_types::flight::encode_and_serialize_time_series_table_metadata(
                    &time_series_table_metadata,
                )?
            }
        };

        let action = Action {
            r#type: "CreateTable".to_owned(),
            body: protobuf_bytes.into(),
        };

        self.flight_client.do_action(action).await?;

        Ok(())
    }

    /// Returns the name of all the tables. If the table names could not be retrieved,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn tables(&mut self) -> Result<Vec<String>> {
        let criteria = Criteria {
            expression: vec![].into(),
        };
        let request = Request::new(criteria);
        let response = self.flight_client.list_flights(request).await?;

        if let Some(flight_info) = response.into_inner().message().await? {
            let mut table_names = vec![];
            if let Some(flight_descriptor) = flight_info.flight_descriptor {
                for table in flight_descriptor.path {
                    table_names.push(table);
                }
            }

            Ok(table_names)
        } else {
            Err(ModelarDbEmbeddedError::from(Status::internal(
                "Could not retrieve table names from the node.",
            )))
        }
    }

    /// Returns the schema of the table with the name in `table_name`. If the table does not exist,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn schema(&mut self, table_name: &str) -> Result<Schema> {
        let flight_descriptor = FlightDescriptor::new_path(vec![table_name.to_owned()]);
        let request = Request::new(flight_descriptor);

        let schema_result = self.flight_client.get_schema(request).await?.into_inner();
        Ok(convert::try_schema_from_ipc_buffer(&schema_result.schema)?)
    }

    /// Writes the data in `uncompressed_data` to the table with the table name in `table_name`. If
    /// the schema of `uncompressed_data` does not match the schema of the table, or the data could
    /// not be written to the table, [`ModelarDbEmbeddedError`] is returned.
    async fn write(&mut self, table_name: &str, uncompressed_data: RecordBatch) -> Result<()> {
        // Include the table name in the flight descriptor.
        let flight_descriptor = FlightDescriptor::new_path(vec![table_name.to_owned()]);

        let flight_data_encoder = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter(vec![Ok(uncompressed_data)]));

        let flight_data_stream = flight_data_encoder.map(|maybe_flight_data| {
            maybe_flight_data.expect("Flight data should be validated by FlightDataEncoderBuilder.")
        });

        self.flight_client.do_put(flight_data_stream).await?;

        Ok(())
    }

    /// Executes the SQL in `sql` and returns the result as a [`RecordBatchStream`]. If the SQL
    /// could not be executed, [`ModelarDbEmbeddedError`] is returned.
    async fn read(&mut self, sql: &str) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        let ticket = Ticket::new(sql.to_owned());
        let stream = self.flight_client.do_get(ticket).await?.into_inner();

        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            // Convert tonic::Status to FlightError.
            stream.map_err(|error| error.into()),
        );

        // The schema cannot be extracted from the record batch stream so we use an empty schema.
        // The schema can be empty since the schema is extracted from the record batches in
        // record_batch_stream_to_record_batch and not from the stream itself.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            record_batch_stream.map_err(|error| DataFusionError::Execution(error.to_string())),
        )))
    }

    /// Executes the SQL in `sql` and writes the result to the normal table with the name in
    /// `target_table_name` in `target`. Note that if copying data to a time series table, the
    /// data is compressed again. If the SQL could not be executed or the data could not be written
    /// to the table, [`ModelarDbEmbeddedError`] is returned.
    async fn copy(
        &mut self,
        sql: &str,
        target: &mut dyn Operations,
        target_table_name: &str,
    ) -> Result<()> {
        let mut record_batch_stream = self.read(sql).await?;

        while let Some(record_batch) = record_batch_stream.next().await {
            target.write(target_table_name, record_batch?).await?;
        }

        Ok(())
    }

    /// Reads data from the time series table with the table name in `table_name` and returns it as a
    /// [`RecordBatchStream`]. The remaining parameters optionally specify which subset of the data
    /// to read. If the data could not be read from the table, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn read_time_series_table(
        &mut self,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        let schema = self.schema(table_name).await?;

        let sql = generate_read_time_series_table_sql(
            table_name,
            &schema,
            columns,
            group_by,
            maybe_start_time,
            maybe_end_time,
            tags,
        );

        self.read(&sql).await
    }

    async fn copy_time_series_table(
        &self,
        _source_table_name: &str,
        _target: &dyn Operations,
        _target_table_name: &str,
        _maybe_start_time: Option<&str>,
        _maybe_end_time: Option<&str>,
        _tags: HashMap<String, String>,
    ) -> Result<()> {
        Err(ModelarDbEmbeddedError::Unimplemented(
            "The ModelarDB client does not support copying time series tables.".to_owned(),
        ))
    }

    async fn r#move(
        &mut self,
        _source_table_name: &str,
        _target: &dyn Operations,
        _target_table_name: &str,
    ) -> Result<()> {
        Err(ModelarDbEmbeddedError::Unimplemented(
            "The ModelarDB client does not support moving tables.".to_owned(),
        ))
    }

    /// Truncate the table with the name in `table_name`. If the table could not be truncated,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn truncate(&mut self, table_name: &str) -> Result<()> {
        let ticket = Ticket::new(format!("TRUNCATE {table_name}"));
        self.flight_client.do_get(ticket).await?;

        Ok(())
    }

    /// Drop the table with the name in `table_name`. If the table could not be dropped,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn drop(&mut self, table_name: &str) -> Result<()> {
        let ticket = Ticket::new(format!("DROP TABLE {table_name}"));
        self.flight_client.do_get(ticket).await?;

        Ok(())
    }

    /// Vacuum the table with the name in `table_name` by deleting stale files that are older than
    /// `maybe_retention_period_in_seconds` seconds. If a retention period is not given, the
    /// default retention period of 7 days is used. If the table does not exist, the table could
    /// not be vacuumed, or the retention period is larger than
    /// [`MAX_RETENTION_PERIOD_IN_SECONDS`](modelardb_types::types::MAX_RETENTION_PERIOD_IN_SECONDS),
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn vacuum(
        &mut self,
        table_name: &str,
        maybe_retention_period_in_seconds: Option<u64>,
    ) -> Result<()> {
        let sql = if let Some(retention_period_in_seconds) = maybe_retention_period_in_seconds {
            format!("VACUUM {table_name} RETAIN {retention_period_in_seconds}")
        } else {
            format!("VACUUM {table_name}")
        };

        let ticket = Ticket::new(sql);
        self.flight_client.do_get(ticket).await?;

        Ok(())
    }
}
