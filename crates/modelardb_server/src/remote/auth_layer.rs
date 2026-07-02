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

//! Tower middleware layer that enforces authentication and authorization for all incoming Apache
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
use tonic::Status;
use tonic::body::Body;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tower::{Layer, Service};

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
    /// The [`Authenticator`] to use once the required permission has been determined, or [`None`]
    /// if authentication is disabled and every non-cluster request should be allowed.
    maybe_authenticator: Option<Arc<dyn Authenticator>>,
    /// The cluster key to use for validating internal cluster requests or [`None`] if running a
    /// single-node server.
    maybe_cluster_key: Option<AsciiMetadataValue>,
}

impl AuthLayer {
    pub(super) fn new(
        maybe_authenticator: Option<Arc<dyn Authenticator>>,
        maybe_cluster_key: Option<AsciiMetadataValue>,
    ) -> Self {
        Self {
            maybe_authenticator,
            maybe_cluster_key,
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> AuthService<S> {
        AuthService {
            inner,
            maybe_authenticator: self.maybe_authenticator.clone(),
            maybe_cluster_key: self.maybe_cluster_key.clone(),
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
    /// The [`Authenticator`] to use once the required permission has been determined, or [`None`]
    /// if authentication is disabled and every non-cluster request should be allowed.
    maybe_authenticator: Option<Arc<dyn Authenticator>>,
    /// The cluster key to use for validating internal cluster requests or [`None`] if running a
    /// single-node server.
    maybe_cluster_key: Option<AsciiMetadataValue>,
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

        let maybe_authenticator = self.maybe_authenticator.clone();
        let maybe_cluster_key = self.maybe_cluster_key.clone();

        Box::pin(async move {
            match authorize(request, maybe_authenticator.as_deref(), &maybe_cluster_key).await {
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
    maybe_authenticator: Option<&dyn Authenticator>,
    maybe_cluster_key: &Option<AsciiMetadataValue>,
) -> Result<Request<Body>, Status> {
    let metadata = MetadataMap::from_headers(request.headers().clone());

    // Cluster key check must happen before the Authenticator since internal cluster requests bypass
    // auth entirely.
    if let Some(request_key) = metadata.get("x-cluster-key") {
        return match maybe_cluster_key {
            Some(key) if key == request_key => Ok(request),
            Some(_) => Err(Status::internal("Invalid cluster key.")),
            None => Err(Status::internal("Cluster key sent to single-node server.")),
        };
    }

    // When no Authenticator is configured, authentication is disabled and every non-cluster request
    // is allowed.
    let Some(authenticator) = maybe_authenticator else {
        return Ok(request);
    };

    // Decode the ticket and parse the SQL to determine the required permission.
    let path = request.uri().path().to_owned();
    if path == DO_GET_PATH {
        return authorize_do_get(request, authenticator, &metadata).await;
    }

    // For all other endpoints the path determines the permission.
    let required_permission = match path.as_str() {
        LIST_FLIGHTS_PATH | GET_FLIGHT_INFO_PATH | GET_SCHEMA_PATH => Permission::Read,
        DO_PUT_PATH => Permission::Write,
        DO_ACTION_PATH | LIST_ACTIONS_PATH => Permission::Admin,
        _ => {
            return Err(Status::invalid_argument("Unknown path."));
        }
    };

    authenticator
        .authorize(&metadata, required_permission)
        .await?;

    Ok(request)
}

/// Buffer the DoGet body, decode the gRPC [`Ticket`] protobuf, parse the SQL, determine the
/// required permission, authorize, then reconstruct the request byte-for-byte.
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

    // gRPC data frames have a 1-byte compression flag, a 4-byte length, and an N bytes message as
    // defined in https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md.
    if bytes.len() < 5 {
        return Err(Status::invalid_argument(
            "Request body too short to be a valid gRPC message.",
        ));
    }

    let ticket = Ticket::decode(&bytes[5..]).map_err(error_to_status_invalid_argument)?;
    let sql = str::from_utf8(&ticket.ticket).map_err(error_to_status_invalid_argument)?;

    let statement =
        parser::tokenize_and_parse_sql_statement(sql).map_err(error_to_status_invalid_argument)?;

    authenticator
        .authorize(metadata, permission_for_statement(&statement))
        .await?;

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

    use modelardb_auth::authenticator::mock::MockAuthenticator;
    use modelardb_test::table::{NORMAL_TABLE_SQL, TIME_SERIES_TABLE_SQL};

    const CLUSTER_KEY: &str = "cluster_key";

    #[tokio::test]
    async fn test_authorize_multi_node_with_valid_cluster_key_bypasses_authenticator() {
        let authenticator = Arc::new(MockAuthenticator::new());

        let request = empty_request_with_cluster_key(DO_PUT_PATH, CLUSTER_KEY);
        let cluster_key = Some(AsciiMetadataValue::from_static(CLUSTER_KEY));

        let result = authorize(request, Some(&*authenticator), &cluster_key).await;

        assert!(result.is_ok());
        assert!(authenticator.permissions().is_empty());
    }

    #[tokio::test]
    async fn test_authorize_multi_node_with_invalid_cluster_key() {
        let authenticator = Arc::new(MockAuthenticator::new());

        let request = empty_request_with_cluster_key(LIST_FLIGHTS_PATH, "invalid_key");
        let cluster_key = Some(AsciiMetadataValue::from_static(CLUSTER_KEY));

        let result = authorize(request, Some(&*authenticator), &cluster_key).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Internal error', message: \"Invalid cluster key.\""
        );
    }

    #[tokio::test]
    async fn test_authorize_single_node_with_cluster_key() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request_with_cluster_key(LIST_FLIGHTS_PATH, CLUSTER_KEY);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Internal error', message: \"Cluster key sent to single-node server.\""
        );
    }

    fn empty_request_with_cluster_key(path: &str, key: &str) -> Request<Body> {
        Request::builder()
            .uri(path)
            .header("x-cluster-key", key)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn test_authorize_without_authenticator_allows_request_without_parsing() {
        // A body that would fail ticket decoding if authorize_do_get() ran.
        let request = raw_frame_request(&[0xFF, 0xFF, 0xFF, 0xFF]);

        let result = authorize(request, None, &None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_authorize_list_flights_calls_authenticator_with_read() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request(LIST_FLIGHTS_PATH);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Read]);
    }

    #[tokio::test]
    async fn test_authorize_get_flight_info_calls_authenticator_with_read() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request(GET_FLIGHT_INFO_PATH);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Read]);
    }

    #[tokio::test]
    async fn test_authorize_get_schema_calls_authenticator_with_read() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request(GET_SCHEMA_PATH);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Read]);
    }

    #[tokio::test]
    async fn test_authorize_do_put_calls_authenticator_with_write() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request(DO_PUT_PATH);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Write]);
    }

    #[tokio::test]
    async fn test_authorize_do_action_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request(DO_ACTION_PATH);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_list_actions_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request(LIST_ACTIONS_PATH);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_create_normal_table_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request(NORMAL_TABLE_SQL);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_create_time_series_table_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request(TIME_SERIES_TABLE_SQL);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_drop_table_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("DROP TABLE test_table");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_truncate_table_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("TRUNCATE test_table");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_vacuum_calls_authenticator_with_admin() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("VACUUM test_table");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Admin]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_include_select_calls_authenticator_with_read() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("INCLUDE 'grpc://192.168.1.2:9999' SELECT * FROM test_table");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Read]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_insert_calls_authenticator_with_write() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("INSERT INTO test_table VALUES (1, 2, 3)");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Write]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_select_calls_authenticator_with_read() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("SELECT * FROM test_table");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Read]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_explain_calls_authenticator_with_read() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("EXPLAIN SELECT * FROM test_table");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert!(result.is_ok());
        assert_eq!(authenticator.permissions(), vec![Permission::Read]);
    }

    #[tokio::test]
    async fn test_authorize_do_get_body_is_reconstructed_intact() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let sql = "SELECT * FROM test_table";

        // Capture the original body bytes before calling authorize().
        let original_request = do_get_request(sql);
        let (_, original_body) = original_request.into_parts();
        let original_bytes = original_body.collect().await.unwrap().to_bytes();

        let request = do_get_request(sql);
        let result = authorize(request, Some(&*authenticator), &None).await;

        let (_, reconstructed_body) = result.unwrap().into_parts();
        let reconstructed_bytes = reconstructed_body.collect().await.unwrap().to_bytes();

        assert_eq!(original_bytes, reconstructed_bytes);
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_body_too_short() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = Request::builder()
            .uri(DO_GET_PATH)
            .body(Body::new(Full::new(bytes::Bytes::from(vec![0u8; 4]))))
            .unwrap();

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Client specified an invalid argument', \
            message: \"Request body too short to be a valid gRPC message.\""
        );
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_invalid_protobuf() {
        let authenticator = Arc::new(MockAuthenticator::new());

        // Valid 5-byte gRPC frame header but invalid protobuf bytes in the message.
        let request = raw_frame_request(&[0xFF, 0xFF, 0xFF, 0xFF]);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Client specified an invalid argument', \
            message: \"failed to decode Protobuf message: invalid varint\""
        );
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_non_utf8_ticket() {
        let authenticator = Arc::new(MockAuthenticator::new());

        // Encode a Ticket with invalid UTF-8 bytes.
        let request = ticket_frame_request(vec![0xFF, 0xFE]);

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Client specified an invalid argument', \
            message: \"invalid utf-8 sequence of 1 bytes from index 0\""
        );
    }

    #[tokio::test]
    async fn test_authorize_do_get_with_unparseable_sql() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = do_get_request("invalid sql");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Client specified an invalid argument', \
            message: \"Parser Error: sql parser error: Expected: an SQL statement, found: invalid at Line: 1, Column: 1\""
        );
    }

    fn do_get_request(sql: &str) -> Request<Body> {
        ticket_frame_request(sql.as_bytes().to_vec())
    }

    fn ticket_frame_request(ticket_bytes: Vec<u8>) -> Request<Body> {
        let ticket = Ticket {
            ticket: ticket_bytes.into(),
        };

        raw_frame_request(&ticket.encode_to_vec())
    }

    fn raw_frame_request(message_bytes: &[u8]) -> Request<Body> {
        // Construct a gRPC frame with the 1-byte compression flag, 4-byte message length, and message.
        let mut frame = Vec::with_capacity(5 + message_bytes.len());
        frame.push(0u8);
        frame.extend_from_slice(&(message_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(message_bytes);

        Request::builder()
            .uri(DO_GET_PATH)
            .body(Body::new(Full::new(bytes::Bytes::from(frame))))
            .unwrap()
    }

    #[tokio::test]
    async fn test_authorize_unknown_path() {
        let authenticator = Arc::new(MockAuthenticator::new());
        let request = empty_request("/unknown/path");

        let result = authorize(request, Some(&*authenticator), &None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "code: 'Client specified an invalid argument', message: \"Unknown path.\""
        );
    }

    fn empty_request(path: &str) -> Request<Body> {
        Request::builder().uri(path).body(Body::empty()).unwrap()
    }
}
