/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of ModelarDB's main function.

mod cluster;
mod configuration;
mod context;
mod data_folders;
mod error;
mod manager;
mod remote;
mod storage;

use std::sync::{Arc, LazyLock};
use std::{env, process};

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::cluster::Cluster;
use crate::context::Context;
use crate::data_folders::DataFolders;
use crate::error::Result;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// The port of the Apache Arrow Flight Server. If the environment variable is not set, 9999 is used.
pub static PORT: LazyLock<u16> =
    LazyLock::new(|| env::var("MODELARDBD_PORT").map_or(9999, |value| value.parse().unwrap()));

/// The different possible modes that a ModelarDB server can be deployed in, assigned when the
/// server is started.
#[derive(Clone)]
pub enum ClusterMode {
    SingleNode,
    MultiNode(Cluster),
}

/// Setup tracing that prints to stdout, parse the command line arguments to extract
/// [`DataFolders`], construct a [`Context`] with the systems components, initialize the normal
/// tables and time series tables in the Delta Lake, initialize a CTRL+C handler that flushes the
/// data in memory to disk, and start the Apache Arrow Flight interface. Returns
/// [`ModelarDbServerError`](error::ModelarDbServerError) if the command line arguments cannot be
/// parsed, if the metadata cannot be read from the database, or if the Apache Arrow Flight
/// interface cannot be started.
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let arguments = collect_command_line_arguments(3);
    let arguments: Vec<&str> = arguments.iter().map(|arg| arg.as_str()).collect();
    let (cluster_mode, data_folders) = if let Ok(cluster_mode_and_data_folders) =
        DataFolders::try_from_command_line_arguments(&arguments).await
    {
        cluster_mode_and_data_folders
    } else {
        print_usage_and_exit_with_error(
            "[server_mode] local_data_folder_url [remote_data_folder_url]",
        );
    };

    let context = Arc::new(Context::try_new(data_folders, cluster_mode.clone()).await?);

    // Register normal tables and time series tables.
    context.register_normal_tables().await?;
    context.register_time_series_tables().await?;

    if let ClusterMode::MultiNode(cluster) = &cluster_mode {
        cluster.retrieve_and_create_tables(&context).await?;
    }

    // Setup CTRL+C handler.
    setup_ctrl_c_handler(&context);

    // Initialize storage engine with spilled buffers.
    context
        .storage_engine
        .read()
        .await
        .initialize(&context)
        .await?;

    // Start the Apache Arrow Flight interface.
    remote::start_apache_arrow_flight_server(context, *PORT).await?;

    Ok(())
}

/// Collect the command line arguments that this program was started with.
pub fn collect_command_line_arguments(maximum_arguments: usize) -> Vec<String> {
    let mut args = std::env::args();
    args.next(); // Skip the executable.

    // Collect at most the maximum number of command line arguments plus one. The plus one argument
    // is collected to trigger the default pattern when parsing the command line arguments with
    // pattern matching, making it possible to handle errors caused by too many arguments.
    args.by_ref().take(maximum_arguments + 1).collect()
}

/// Prints a usage message with `parameters` appended to the name of the binary executing this
/// function to stderr and exits with status code one to indicate that an error has occurred.
pub fn print_usage_and_exit_with_error(parameters: &str) -> ! {
    // The errors are consciously ignored as the program is terminating.
    let binary_path = std::env::current_exe().unwrap();
    let binary_name = binary_path.file_name().unwrap().to_str().unwrap();

    // Punctuation at the end does not seem to be common in the usage message of Unix tools.
    eprintln!("Usage: {binary_name} {parameters}");
    process::exit(1);
}

/// Register a handler to execute when CTRL+C is pressed. The handler takes an exclusive lock for
/// the storage engine, flushes the data the storage engine currently buffers, removes the node
/// from the cluster if necessary, and terminates the system without releasing the lock.
fn setup_ctrl_c_handler(context: &Arc<Context>) {
    let ctrl_c_context = context.clone();
    tokio::spawn(async move {
        // Errors are consciously ignored as the program should terminate if the handler cannot be
        // registered as buffers otherwise cannot be flushed.
        tokio::signal::ctrl_c().await.unwrap();

        // Stop the threads in the storage engine and close it.
        ctrl_c_context.storage_engine.write().await.close().unwrap();

        // If running in a cluster, remove the node from the remote data folder.
        let configuration_manager = ctrl_c_context.configuration_manager.read().await;
        if let ClusterMode::MultiNode(cluster) = &configuration_manager.cluster_mode {
            cluster.remove_node().await.unwrap();
        }

        std::process::exit(0)
    });
}
