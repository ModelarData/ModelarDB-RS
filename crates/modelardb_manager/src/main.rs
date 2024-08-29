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

//! Implementation of ModelarDB manager's main function.

mod cluster;
mod metadata;
mod remote;

use std::env;
use std::sync::{Arc, LazyLock};

use modelardb_common::arguments::{
    argument_to_connection_info, collect_command_line_arguments, validate_remote_data_folder,
};
use modelardb_common::storage::DeltaLake;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::cluster::Cluster;
use crate::metadata::MetadataManager;
use crate::remote::start_apache_arrow_flight_server;

/// The port of the Apache Arrow Flight Server. If the environment variable is not set, 9998 is used.
pub static PORT: LazyLock<u16> =
    LazyLock::new(|| env::var("MODELARDBM_PORT").map_or(9998, |value| value.parse().unwrap()));

/// Stores the connection information with the remote data folder to ensure that the information
/// is consistent with the remote data folder.
pub struct RemoteDataFolder {
    /// Connection information saved as bytes to make it possible to transfer the information using
    /// Apache Arrow Flight.
    connection_info: Vec<u8>,
    /// Remote object store for storing data and metadata in Apache Parquet files.
    delta_lake: Arc<DeltaLake>,
    /// Manager for the access to the metadata Delta Lake.
    metadata_manager: Arc<MetadataManager>,
}

impl RemoteDataFolder {
    pub fn new(
        connection_info: Vec<u8>,
        delta_lake: Arc<DeltaLake>,
        metadata_manager: Arc<MetadataManager>,
    ) -> Self {
        Self {
            connection_info,
            delta_lake,
            metadata_manager,
        }
    }

    /// Parse the command line arguments into a [`RemoteDataFolder`]. If the necessary command line
    /// arguments are not provided, too many arguments are provided, or if the arguments are malformed,
    /// [`String`] is returned.
    async fn try_from_command_line_arguments(arguments: &[&str]) -> Result<Self, String> {
        match arguments {
            &[remote_data_folder] => {
                let connection_info = argument_to_connection_info(remote_data_folder)?;

                let delta_lake = DeltaLake::try_remote_from_connection_info(&connection_info)
                    .await
                    .map_err(|error| error.to_string())?;

                let metadata_manager = MetadataManager::try_from_connection_info(&connection_info)
                    .await
                    .map_err(|error| error.to_string())?;

                Ok(Self::new(
                    connection_info,
                    Arc::new(delta_lake),
                    Arc::new(metadata_manager),
                ))
            }
            _ => {
                // The errors are consciously ignored as the program is terminating.
                let binary_path = std::env::current_exe().unwrap();
                let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
                Err(format!("Usage: {binary_name} remote_data_folder."))
            }
        }
    }
}

/// Provides access to the managers components.
pub struct Context {
    /// Folder for storing metadata and data in Apache Parquet files in a remote object store.
    pub remote_data_folder: RemoteDataFolder,
    /// Cluster of nodes currently controlled by the manager.
    pub cluster: RwLock<Cluster>,
    /// Key used to identify requests coming from the manager.
    pub key: MetadataValue<Ascii>,
}

/// Parse the command line arguments to extract the remote object store and start an Apache Arrow Flight server.
/// Returns [`String`] if the command line arguments cannot be parsed, if the metadata cannot be read from the
/// Delta Lake, or if the Apache Arrow Flight server cannot be started.
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

    let context = runtime.block_on(async {
        let remote_data_folder =
            RemoteDataFolder::try_from_command_line_arguments(&arguments).await?;
        validate_remote_data_folder(&remote_data_folder.delta_lake).await?;

        let nodes = remote_data_folder
            .metadata_manager
            .nodes()
            .await
            .map_err(|error| error.to_string())?;

        let mut cluster = Cluster::new();
        for node in nodes {
            cluster
                .register_node(node)
                .map_err(|error| error.to_string())?;
        }

        // Retrieve and parse the key to a tonic metadata value since it is used in tonic requests.
        let key = remote_data_folder
            .metadata_manager
            .manager_key()
            .await
            .map_err(|error| error.to_string())?
            .to_string()
            .parse()
            .map_err(|error: InvalidMetadataValue| error.to_string())?;

        // Create the Context.
        Ok::<Arc<Context>, String>(Arc::new(Context {
            remote_data_folder,
            cluster: RwLock::new(cluster),
            key,
        }))
    })?;

    start_apache_arrow_flight_server(context, &runtime, *PORT)
        .map_err(|error| error.to_string())?;

    Ok(())
}
