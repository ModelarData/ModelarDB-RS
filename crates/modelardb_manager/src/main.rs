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

use object_store::ObjectStore;
use std::env;
use std::sync::Arc;

use modelardb_common::arguments::{
    argument_to_remote_object_store, collect_command_line_arguments,
};
use modelardb_common::remote_data_folder::validate_remote_data_folder_from_argument;
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgConnection};
use tokio::runtime::Runtime;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::remote::start_apache_arrow_flight_server;

mod remote;

pub const PORT: u16 = 8888;

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

    let arguments = collect_command_line_arguments(3);
    let arguments: Vec<&str> = arguments.iter().map(|arg| arg.as_str()).collect();

    let (_connection, _remote_data_folder) = runtime.block_on(async {
        let (connection, remote_data_folder) = parse_command_line_arguments(&arguments).await?;
        validate_remote_data_folder_from_argument(arguments.get(1).unwrap(), &remote_data_folder)
            .await?;

        Ok::<(PgConnection, Arc<dyn ObjectStore>), String>((connection, remote_data_folder))
    })?;

    start_apache_arrow_flight_server(&runtime, PORT).map_err(|error| error.to_string())?;

    Ok(())
}

/// Parse the command lines arguments into a [`PgConnection`] to the metadata database and a
/// remote data folder. If the necessary command line arguments are not provided, too many
/// arguments are provided, or if the arguments are malformed, [`String`] is returned.
async fn parse_command_line_arguments(
    arguments: &[&str],
) -> Result<(PgConnection, Arc<dyn ObjectStore>), String> {
    // Match the provided command line arguments to the supported inputs.
    match arguments {
        &[metadata_database, remote_data_folder] => {
            let username = env::var("METADATA_DB_USER").map_err(|error| error.to_string())?;
            let password = env::var("METADATA_DB_PASSWORD").map_err(|error| error.to_string())?;
            let host = env::var("METADATA_DB_HOST").map_err(|error| error.to_string())?;

            Ok((
                PgConnectOptions::new()
                    .host(host.as_str())
                    .username(username.as_str())
                    .password(password.as_str())
                    .database(metadata_database)
                    .connect()
                    .await
                    .map_err(|error| format!("Unable to connect to Postgres database: {error}"))?,
                argument_to_remote_object_store(remote_data_folder)?,
            ))
        }
        _ => {
            // The errors are consciously ignored as the program is terminating.
            let binary_path = std::env::current_exe().unwrap();
            let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
            Err(format!(
                "Usage: {binary_name} [metadata_database] [remote_data_folder]."
            ))
        }
    }
}
