/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of the ModelarDB manager main function.

use std::sync::Arc;

use tokio::runtime::Runtime;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use modelardb_common::arguments::collect_command_line_arguments;

use crate::remote::start_apache_arrow_flight_server;

mod remote;

/// Parse the command line arguments to extract the metadata database and the remote object store
/// and start an Apache Arrow Flight server. Returns [`String`] if the command line arguments
/// cannot be parsed, if the metadata cannot be read from the database, or if the Apache Arrow
/// Flight server cannot be started.
fn main() -> Result<(), String> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    // Create a Tokio runtime for executing asynchronous tasks.
    let runtime = Arc::new(
        Runtime::new().map_err(|error| format!("Unable to create a Tokio Runtime: {error}"))?,
    );

    // TODO: Move function to it for validating remote object store.
    // TODO: Use common functionality in the server main and here.
    // TODO: Add function here to connect to database.

    let arguments = collect_command_line_arguments(3);
    start_apache_arrow_flight_server().map_err(|error| error.to_string())?;

    Ok(())
}
