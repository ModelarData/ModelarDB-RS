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
mod error;
mod remote;

use std::env;
use std::sync::{Arc, LazyLock};

use modelardb_storage::data_folder::DataFolder;
use tokio::sync::RwLock;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

use crate::cluster::Cluster;
use crate::error::{ModelarDbManagerError, Result};
use crate::remote::start_apache_arrow_flight_server;

/// The port of the Apache Arrow Flight Server. If the environment variable is not set, 9998 is used.
pub static PORT: LazyLock<u16> =
    LazyLock::new(|| env::var("MODELARDBM_PORT").map_or(9998, |value| value.parse().unwrap()));

/// Provides access to the managers components.
pub struct Context {
    /// [`DataFolder`] for storing metadata and data in Apache Parquet files.
    pub remote_data_folder: DataFolder,
    /// Cluster of nodes currently controlled by the manager.
    pub cluster: RwLock<Cluster>,
    /// Key used to identify requests coming from the manager.
    pub key: MetadataValue<Ascii>,
}

/// Parse the command line arguments to extract the remote object store and start an Apache Arrow
/// Flight server. Returns [`ModelarDbManagerError`] if the command line arguments cannot be parsed,
/// if the metadata cannot be read from the Delta Lake, or if the Apache Arrow Flight server cannot
/// be started.
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let remote_data_folder = DataFolder::open_remote_url("s3://test").await?;

    let cluster = Cluster::new();

    // Retrieve and parse the key to a tonic metadata value since it is used in tonic requests.
    let key = Uuid::new_v4()
        .to_string()
        .parse()
        .map_err(|error: InvalidMetadataValue| {
            ModelarDbManagerError::InvalidArgument(error.to_string())
        })?;

    // Create the Context.
    let context = Arc::new(Context {
        remote_data_folder,
        cluster: RwLock::new(cluster),
        key,
    });

    start_apache_arrow_flight_server(context, *PORT).await?;

    Ok(())
}
