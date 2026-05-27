/* Copyright 2026 The ModelarDB Contributors
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

//! Tower middleware layer that enforces authentication and authorization on all incoming Apache
//! Arrow Flight requests. The layer runs before [`FlightServiceHandler`](super::FlightServiceHandler)
//! and checks the cluster key or bearer token before the request reaches the handler. For DoGet
//! requests the SQL ticket is decoded, and the required permission is determined from the parsed
//! statement.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_flight::Ticket;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use modelardb_auth::Permission;
use modelardb_auth::authenticator::Authenticator;
use modelardb_storage::parser::{self, ModelarDbStatement};
use prost::Message;
use sqlparser::ast::Statement;
use tokio::sync::RwLock;
use tonic::Status;
use tonic::body::Body;
use tonic::metadata::MetadataMap;
use tower::{Layer, Service};

use crate::ClusterMode;
use crate::configuration::ConfigurationManager;
use crate::remote::error_to_status_invalid_argument;

const LIST_FLIGHTS_PATH: &str = "/arrow.flight.protocol.FlightService/ListFlights";
const GET_FLIGHT_INFO_PATH: &str = "/arrow.flight.protocol.FlightService/GetFlightInfo";
const GET_SCHEMA_PATH: &str = "/arrow.flight.protocol.FlightService/GetSchema";
const DO_GET_PATH: &str = "/arrow.flight.protocol.FlightService/DoGet";
const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";
const DO_ACTION_PATH: &str = "/arrow.flight.protocol.FlightService/DoAction";
const LIST_ACTIONS_PATH: &str = "/arrow.flight.protocol.FlightService/ListActions";

/// [`Layer`] that enforces authentication and authorization on all incoming Apache Arrow Flight
/// requests.
#[derive(Clone)]
pub(super) struct AuthLayer {
    /// The [`Authenticator`] to use for authentication once the auth layer has determined the
    /// required permission for a client request.
    authenticator: Arc<dyn Authenticator>,
    /// The [`ConfigurationManager`] to use for cluster key validation for internal cluster requests.
    configuration_manager: Arc<RwLock<ConfigurationManager>>,
}

impl AuthLayer {
    pub(super) fn new(
        authenticator: Arc<dyn Authenticator>,
        configuration_manager: Arc<RwLock<ConfigurationManager>>,
    ) -> Self {
        Self {
            authenticator,
            configuration_manager,
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> AuthService<S> {
        AuthService {
            inner,
            authenticator: self.authenticator.clone(),
            configuration_manager: self.configuration_manager.clone(),
        }
    }
}

/// Middleware layer that enforces authentication and authorization on all incoming Apache Arrow
/// Flight requests.
#[derive(Clone)]
pub(super) struct AuthService<S> {
    /// The [`FlightServiceHandler`](super::FlightServiceHandler) that processes the request after
    /// authorization succeeds.
    inner: S,
    /// The [`Authenticator`] to use for authentication once the auth layer has determined the
    /// required permission for a client request.
    authenticator: Arc<dyn Authenticator>,
    /// The [`ConfigurationManager`] to use for cluster key validation for internal cluster requests.
    configuration_manager: Arc<RwLock<ConfigurationManager>>,
}

impl<S> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // A cloned service may not be ready and panics if called before poll_ready. Use mem::replace
        // to take the ready service out of self.inner and put a fresh clone in its place, which
        // will be polled for readiness on the next request.
        //
        // See https://docs.rs/tower/latest/tower/trait.Service.html#be-careful-when-cloning-inner-services.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let authenticator = self.authenticator.clone();
        let configuration_manager = self.configuration_manager.clone();

        Box::pin(async move {
            match authorize(request, &*authenticator, &configuration_manager).await {
                Ok(request) => inner.call(request).await,
                Err(status) => Ok(status.into_http()),
            }
        })
    }
}

/// Full authorization flow. Return the (possibly reconstructed) request on success, or a
/// [`Status`] on failure.
async fn authorize(
    request: Request<Body>,
    authenticator: &dyn Authenticator,
    configuration_manager: &RwLock<ConfigurationManager>,
) -> Result<Request<Body>, Status> {
    let path = request.uri().path().to_owned();
    let metadata = MetadataMap::from_headers(request.headers().clone());

    // Cluster key check must happen before the Authenticator since internal cluster requests bypass
    // auth entirely.
    if let Some(request_key) = metadata.get("x-cluster-key") {
        let configuration_manager = configuration_manager.read().await;
        return match configuration_manager.cluster_mode() {
            ClusterMode::MultiNode(cluster) if cluster.key() == request_key => Ok(request),
            ClusterMode::MultiNode(_) => Err(Status::internal("Invalid cluster key.")),
            _ => Err(Status::internal("Cluster key sent to single-node server.")),
        };
    }

    // ListActions is a public discovery endpoint.
    if path == LIST_ACTIONS_PATH {
        return Ok(request);
    }

    // Decode the ticket and parse the SQL to determine the required permission.
    if path == DO_GET_PATH {
        return authorize_do_get(request, authenticator, &metadata).await;
    }

    // For all other endpoints the path determines the permission.
    let required_permission = match path.as_str() {
        LIST_FLIGHTS_PATH | GET_FLIGHT_INFO_PATH | GET_SCHEMA_PATH => Permission::Read,
        DO_PUT_PATH => Permission::Write,
        DO_ACTION_PATH => Permission::Admin,
        _ => {
            return Err(Status::invalid_argument("Unknown path."));
        }
    };

    authenticator.authorize(&metadata, required_permission)?;

    Ok(request)
}

/// Buffer the DoGet body, decode the gRPC [`Ticket`] protobuf, parse the SQL, determine the
/// required permission, authorize, then reconstruct the request with the original bytes.
async fn authorize_do_get(
    request: Request<Body>,
    authenticator: &dyn Authenticator,
    metadata: &MetadataMap,
) -> Result<Request<Body>, Status> {
    let (parts, body) = request.into_parts();

    // Collect the full body.
    let bytes = body
        .collect()
        .await
        .map_err(|_| Status::invalid_argument("Failed to unpack request body."))?
        .to_bytes();

    // gRPC has a 1-byte compression flag, a 4-byte length, and an N bytes protobuf message.
    if bytes.len() < 5 {
        return Err(Status::invalid_argument(
            "Request body too short to be a valid gRPC message.",
        ));
    }

    let ticket = Ticket::decode(&bytes[5..]).map_err(error_to_status_invalid_argument)?;
    let sql = str::from_utf8(&ticket.ticket).map_err(error_to_status_invalid_argument)?;

    let statement =
        parser::tokenize_and_parse_sql_statement(sql).map_err(error_to_status_invalid_argument)?;

    authenticator.authorize(metadata, permission_for_statement(&statement))?;

    // Reconstruct the request with the original bytes so the server receives it intact.
    Ok(Request::from_parts(parts, Body::new(Full::new(bytes))))
}

/// Map a parsed [`ModelarDbStatement`] to the required [`Permission`].
fn permission_for_statement(statement: &ModelarDbStatement) -> Permission {
    match statement {
        ModelarDbStatement::CreateNormalTable { .. } => Permission::Admin,
        ModelarDbStatement::CreateTimeSeriesTable(_) => Permission::Admin,
        ModelarDbStatement::DropTable(_) => Permission::Admin,
        ModelarDbStatement::TruncateTable(_, _) => Permission::Admin,
        ModelarDbStatement::Vacuum(_, _, _) => Permission::Admin,
        ModelarDbStatement::IncludeSelect(_, _) => Permission::Read,
        ModelarDbStatement::Statement(statement) => match statement {
            Statement::Insert(_) => Permission::Write,
            Statement::Query(_) | Statement::Explain { .. } => Permission::Read,
            _ => Permission::Admin,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_storage::data_folder::DataFolder;
    use modelardb_types::types::{Node, ServerMode};
    use tempfile::TempDir;

    use crate::cluster::Cluster;

    fn empty_request(path: &str) -> Request<Body> {
        Request::builder().uri(path).body(Body::empty()).unwrap()
    }

    fn empty_request_with_cluster_key(path: &str, key: &str) -> Request<Body> {
        Request::builder()
            .uri(path)
            .header("x-cluster-key", key)
            .body(Body::empty())
            .unwrap()
    }

    async fn single_node_configuration_manager(
        temp_dir: &TempDir,
    ) -> Arc<RwLock<ConfigurationManager>> {
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        Arc::new(RwLock::new(
            ConfigurationManager::try_new(local_data_folder, ClusterMode::SingleNode)
                .await
                .unwrap(),
        ))
    }

    async fn multi_node_configuration_manager(
        temp_dir: &TempDir,
    ) -> (Arc<RwLock<ConfigurationManager>>, String) {
        let local_url = temp_dir.path().to_str().unwrap();

        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();
        let remote_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        let edge_node = Node::new("edge".to_owned(), ServerMode::Edge);
        let cluster = Cluster::try_new(edge_node, remote_data_folder)
            .await
            .unwrap();

        let key = cluster.key().to_str().unwrap().to_owned();

        let configuration_manager = Arc::new(RwLock::new(
            ConfigurationManager::try_new(
                local_data_folder,
                ClusterMode::MultiNode(Box::new(cluster)),
            )
            .await
            .unwrap(),
        ));

        (configuration_manager, key)
    }
}
