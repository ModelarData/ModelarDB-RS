/* Copyright 2021 The MiniModelarDB Contributors
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
use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::{array::ArrayRef, datatypes::SchemaRef, ipc::writer::IpcWriteOptions};
use futures::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::Context;

/// Start an Apache Arrow Flight server on 0.0.0.0:`port` with `context`
/// provided to the methods processing each request through the handler.
pub fn start_arrow_flight_server(context: Arc<Context>, port: i16) -> Result<(), Box<dyn Error>> {
    let localhost_with_port = "0.0.0.0:".to_string() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse()?;
    let handler = FlightServiceHandler {
        context: context.clone(),
        dictionaries_by_id: HashMap::new(),
    };
    let flight_service_server = FlightServiceServer::new(handler);
    info!("Starting Apache Arrow Flight on {}.", localhost_with_port);
    context
        .runtime
        .block_on(async {
            Server::builder()
                .add_service(flight_service_server)
                .serve(localhost_with_port)
                .await
        })
        .map_err(|e| e.into())
}

/// Handler for Apache Arrow Flight requests. The type is based on the [Apache
/// Arrow Flight examples] published under the Apache2 license.
///
/// [Apache Arrow Flight examples]: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples
struct FlightServiceHandler {
    context: Arc<Context>,
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl FlightServiceHandler {
    /// Return the schema of `table_name` if the table exists in the default
    /// catalog, otherwise a `Status` indicating at what level the lookup failed
    /// is returned.
    fn get_table_schema_from_default_catalog(&self, table_name: &str) -> Result<SchemaRef, Status> {
        let session = self.context.session.clone();
        if let Some(catalog) = session.catalog("datafusion") {
            // default catalog.
            if let Some(schema) = catalog.schema("public") {
                // default schema.
                if let Some(table) = schema.table(table_name) {
                    Ok(table.schema())
                } else {
                    Err(Status::not_found("table does not exist"))
                }
            } else {
                Err(Status::internal("schema does not exist"))
            }
        } else {
            Err(Status::internal("catalog does not exist"))
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceHandler {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let table_names = self.context.catalog.table_names();
        let fd = FlightDescriptor::new_path(table_names);
        let fi = FlightInfo::new(IpcMessage(vec![]), Some(fd), vec![], -1, -1);
        let output = futures::stream::once(async { Ok(fi) });
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        if let Some(table_name) = request.into_inner().path.get(0) {
            let session = self.context.session.clone();
            if let Some(catalog) = session.catalog("datafusion") {
                // default catalog.
                if let Some(schema) = catalog.schema("public") {
                    // default schema.
                    if let Some(table) = schema.table(table_name) {
                        let schema = &*table.schema();
                        let options = IpcWriteOptions::default();
                        let schema_as_ipc = SchemaAsIpc::new(schema, &options);
                        if let Ok(sr) = schema_as_ipc.try_into() {
                            Ok(Response::new(sr))
                        } else {
                            Err(Status::internal("unable to serialize schema"))
                        }
                    } else {
                        Err(Status::not_found("table does not exist"))
                    }
                } else {
                    Err(Status::internal("schema does not exist"))
                }
            } else {
                Err(Status::internal("catalog does not exist"))
            }
        } else {
            Err(Status::invalid_argument("no table was provided"))
        }
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Extract client query.
        let message = request.get_ref();
        let query = str::from_utf8(&message.ticket).map_err(error_to_invalid_argument)?;
        info!("Executing: {}.", query);

        // Executes client query.
        let session = self.context.session.clone();
        let df = session
            .sql(query)
            .await
            .map_err(error_to_invalid_argument)?;
        let results = df.collect().await.map_err(error_to_invalid_argument)?;

        // Transmits schema.
        let options = IpcWriteOptions::default();
        let schema_flight_data = SchemaAsIpc::new(&df.schema().clone().into(), &options).into();
        let mut flights: Vec<Result<FlightData, Status>> = vec![Ok(schema_flight_data)];

        //Transmits result set
        let mut batches: Vec<Result<FlightData, Status>> = results
            .iter()
            .flat_map(|result| {
                let (flight_dictionaries, flight_batch) =
                    flight_data_from_arrow_batch(result, &options);
                flight_dictionaries
                    .into_iter()
                    .chain(std::iter::once(flight_batch))
                    .map(Ok)
            })
            .collect();
        flights.append(&mut batches);
        let output = futures::stream::iter(flights);
        Ok(Response::new(Box::pin(output)))
    }

    // TODO:check schema when converting? https://docs.rs/arrow-flight/latest/arrow_flight/utils/index.html
    // TODO: add do_get span and do_put span, but be careful of futures.
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();

        // A `FlightDescriptor` is transferred in the first `FlightData`.
        let flight_data = request
            .next()
            .await
            .ok_or_else(|| none_to_invalid_argument("Missing FlightData."))??;

        debug_assert_eq!(flight_data.data_body.len(), 0);

        let flight_descriptor = flight_data
            .flight_descriptor
            .ok_or_else(|| none_to_invalid_argument("Missing FlightDescriptor."))?;

        let table_name = flight_descriptor
            .path
            .get(0)
            .ok_or_else(|| none_to_invalid_argument("No table name in FlightDescriptor.path."))?;

        let schema_or_status = self.get_table_schema_from_default_catalog(&table_name);
        let schema = if let Ok(schema) = schema_or_status {
            info!("Received RecordBatch for existing table {}.", table_name);
            schema
        } else {
            error!("Received RecordBatch for missing table {}.", table_name);
            schema_or_status?
        };

        // Rows are transferred in the remaining `FlightData`.
        while let Some(flight_data) = request.next().await {
            let flight_data = flight_data?;
            debug_assert_eq!(flight_data.flight_descriptor, None);
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &self.dictionaries_by_id)
                    .map_err(error_to_invalid_argument)?;
            info!(
                "Received RecordBatch with {} rows for table {}.",
                record_batch.num_rows(),
                table_name
            );
        }
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

/// Convert `error` to a `Status` indicating that one of the arguments provided
/// by the Apache Arrow Flight client was invalid.
fn error_to_invalid_argument(error: impl Error) -> Status {
    Status::invalid_argument(format!("{}", error))
}

/// Convert `none` to a `Status` indicating that one of the arguments provided
/// by the Apache Arrow Flight client was invalid.
fn none_to_invalid_argument(message: &str) -> Status {
    Status::invalid_argument(message)
}
