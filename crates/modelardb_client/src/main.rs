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

//! Implementation of ModelarDB's command line client.

mod error;
mod helper;

use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::path::{Path as StdPath, PathBuf};
use std::process;
use std::time::Instant;

use arrow::util::pretty;
use clap::Parser;
use futures::StreamExt;
use modelardb_embedded::error::ModelarDbEmbeddedError;
use modelardb_embedded::operations::Operations;
use modelardb_embedded::operations::client::Client;
use modelardb_types::flight::protocol;
use modelardb_types::flight::protocol::update_configuration::Setting;
use rustyline::Editor;
use rustyline::history::FileHistory;

use crate::error::{ModelarDbClientError, Result};
use crate::helper::ClientHelper;

/// Command line arguments for the ModelarDB client.
#[derive(Parser)]
#[command(
    about = "ModelarDB command-line client",
    long_about = "ModelarDB command-line client. Connects to a running instance of modelardbd and \
    allows executing queries and commands on it. If a file containing queries is provided as an \
    argument, the queries in the file are executed. Otherwise, an interactive read-eval-print loop \
    is opened where queries and commands can be executed interactively."
)]
struct ClientArgs {
    /// Host of the modelardbd instance to connect to.
    #[arg(long, default_value = "127.0.0.1", env = "MODELARDB_HOST")]
    host: String,

    /// Port of the modelardbd instance to connect to.
    #[arg(long, default_value_t = 9999, env = "MODELARDB_PORT")]
    port: u16,

    /// Bearer token for authenticating requests sent to the modelardbd instance. If not provided,
    /// requests are sent without an authorization header.
    #[arg(long, env = "MODELARDB_TOKEN")]
    token: Option<String>,

    /// Path to a file containing SQL queries to execute. If not provided, an interactive
    /// read-eval-print loop is opened.
    query_file: Option<PathBuf>,
}

/// Connect to the server and execute queries from a file or open a read-eval-print loop. Returns
/// [`ModelarDbClientError`] if the command-line arguments cannot be parsed, the client cannot
/// connect to the server, or the file containing the queries cannot be read.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let args = ClientArgs::parse();

    // Connect to the server.
    let url = format!("grpc://{}:{}", args.host, args.port);
    let client = Client::connect(&url, args.token.as_deref()).await?;

    // Execute the queries.
    if let Some(query_file) = args.query_file {
        execute_queries_from_a_file(client, &query_file).await
    } else {
        execute_queries_from_a_repl(client).await
    }
}

/// Execute the commands and queries in `query_file`.
async fn execute_queries_from_a_file(mut client: Client, query_file: &StdPath) -> Result<()> {
    let file = File::open(query_file)?;
    let lines = BufReader::new(file).lines();

    for line in lines {
        // Remove any comments.
        let input = line?;
        let query = if let Some(comment_start) = input.find("--") {
            input[0..comment_start].to_owned()
        } else {
            input
        };

        // Execute the query.
        if !query.is_empty() {
            println!("{query}");
            execute_and_print_command_or_query(&mut client, &query).await
        }
    }

    Ok(())
}

/// Execute commands and queries in a read-eval-print loop.
async fn execute_queries_from_a_repl(mut client: Client) -> Result<()> {
    // Create the read-eval-print loop.
    let mut editor = Editor::<ClientHelper, FileHistory>::new()?;
    let table_names = client.tables().await?;
    editor.set_helper(Some(ClientHelper::new(table_names)));

    // Read previously executed commands and queries from the history file.
    let history_file_name = ".modelardb_history";
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.load_history(&home);
    }

    // Specify where to find helpful information about the commands supported by the repl.
    println!("Type \\h for help.\n");

    // Execute commands and queries and print the result.
    while let Ok(line) = editor.readline("ModelarDB> ") {
        editor.add_history_entry(line.as_str())?;
        execute_and_print_command_or_query(&mut client, &line).await
    }

    // Append the executed commands and queries to the history file.
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.append_history(&home);
    }

    Ok(())
}

/// Execute a command or a query. Returns [`ModelarDbClientError`] if the command or query could not
/// be executed or their result could not be retrieved.
async fn execute_and_print_command_or_query(client: &mut Client, command_or_query: &str) {
    let start_time = Instant::now();
    let command_or_query = command_or_query.trim();

    let result = if command_or_query.starts_with('\\') {
        execute_command(client, command_or_query).await
    } else {
        execute_query_and_print_result(client, command_or_query).await
    };

    if let Err(message) = result {
        eprintln!("{message}");
    }
    println!("\nTime: {:?}\n", start_time.elapsed());
}

/// Execute a command. Returns [`ModelarDbClientError`] if:
/// * An incorrect command was provided.
/// * An incorrect argument for the command was provided.
/// * The command could not be executed.
/// * The result could not be retrieved.
async fn execute_command(client: &mut Client, command_and_arguments: &str) -> Result<()> {
    let mut command_and_arguments = command_and_arguments.split_whitespace();
    match command_and_arguments
        .next()
        .ok_or(ModelarDbClientError::InvalidArgument(
            "No command was provided.".to_owned(),
        ))? {
        // Print the schema of a table on the server.
        "\\d" => {
            let table_name =
                command_and_arguments
                    .next()
                    .ok_or(ModelarDbClientError::InvalidArgument(
                        "No table name was provided.".to_owned(),
                    ))?;

            let schema = client.schema(table_name).await?;
            for field in schema.fields() {
                print!("{}: {}", field.name(), field.data_type());
                for (metadata_name, metadata_value) in field.metadata() {
                    print!(", {metadata_name} {metadata_value}");
                }
                println!();
            }
            Ok(())
        }
        // Print the name of the tables on the server.
        "\\dt" => {
            for table_name in client.tables().await? {
                println!("{table_name}");
            }
            Ok(())
        }
        // Print the configuration of the node.
        "\\dc" => {
            let configuration = client.configuration().await?;
            print_configuration(&configuration);
            Ok(())
        }
        // Print the nodes that are currently part of the cluster.
        "\\dn" => {
            for node in client.list_nodes().await? {
                println!("{} ({})", node.url, node.mode);
            }
            Ok(())
        }
        // Print the resource usage metrics of the node.
        "\\dm" => {
            let node_metrics = client.node_metrics().await?;
            print_node_metrics(&node_metrics);
            Ok(())
        }
        // Update a setting in the configuration of the node.
        "\\s" => {
            let name =
                command_and_arguments
                    .next()
                    .ok_or(ModelarDbClientError::InvalidArgument(
                        "No setting was provided.".to_owned(),
                    ))?;

            let setting = Setting::from_str_name(&name.to_uppercase()).ok_or(
                ModelarDbClientError::InvalidArgument(format!("Unknown setting: {name}.")),
            )?;

            // Omitting the value unsets the setting if it is optional.
            let new_value = match command_and_arguments.next() {
                Some(value) => Some(value.parse::<u64>().map_err(|_error| {
                    ModelarDbClientError::InvalidArgument(format!(
                        "{value} is not a valid value for {name}."
                    ))
                })?),
                None => None,
            };

            client.update_configuration(setting, new_value).await?;
            Ok(())
        }
        // Flushes all data the server currently has in memory to disk.
        "\\f" => client.flush_memory().await.map_err(|error| error.into()),
        // Flushes all data the server currently has in memory and disk to the object store.
        "\\F" => client.flush_node().await.map_err(|error| error.into()),
        // Print helpful information, explanations with \\ must be indented more to be aligned.
        "\\h" => {
            println!(
                "CREATE [TIME SERIES] TABLE     Execute a CREATE TABLE or CREATE TIME SERIES TABLE statement.\n\
                 INSERT INTO                    Execute an INSERT INTO statement. Must include generated columns.\n\
                 SELECT                         Execute a SELECT statement.\n\
                 \\d TABLE_NAME                 Print the schema of a table with TABLE_NAME.\n\
                 \\dt                           Print the name of all the tables.\n\
                 \\f                            Flushes data in memory to disk.\n\
                 \\F                            Flushes data in memory and disk to the object store.\n\
                 \\h                            Print documentation for all supported commands.\n\
                 \\q                            Quit modelardb."
            );
            Ok(())
        }
        "\\q" => {
            process::exit(0);
        }
        command => Err(ModelarDbClientError::InvalidArgument(format!(
            "Unknown command: {command}."
        ))),
    }
}

/// Execute a query and print each batch in the result set. If standard output is a terminal, ask
/// the user for confirmation before printing each batch after the first. Returns
/// [`ModelarDbClientError`] if the query could not be executed or the batches in the result set
/// could not be printed.
async fn execute_query_and_print_result(client: &mut Client, query: &str) -> Result<()> {
    let mut record_batch_stream = client.read(query).await?;

    let print_confirmation = io::stdout().is_terminal();
    let mut multiple_batches = false;

    while let Some(record_batch) = record_batch_stream.next().await {
        // Only ask for confirmation to print the next batch if there are multiple batches.
        if print_confirmation && multiple_batches && !confirm_printing_next_batch()? {
            return Ok(());
        }

        let record_batch = record_batch.map_err(ModelarDbEmbeddedError::from)?;
        pretty::print_batches(&[record_batch])?;
        multiple_batches = true;
    }

    Ok(())
}

/// Ask the user for confirmation before printing the next batch in a result set. Returns false if
/// the user chose to stop printing batches. Returns [`ModelarDbClientError`] if the input could not
/// be read.
fn confirm_printing_next_batch() -> Result<bool> {
    let mut user_input = String::new();

    loop {
        user_input.clear();
        print!("Press Enter for next batch and q+Enter to quit> ");
        io::stdout().flush()?;

        // A read of zero bytes means standard input reached end-of-file, so no more batches can be
        // confirmed.
        if io::stdin().read_line(&mut user_input)? == 0 {
            return Ok(false);
        }

        // The line includes the line ending, which is \r\n on Windows and \n everywhere else.
        match user_input.trim() {
            "" => return Ok(true),
            "q" => return Ok(false),
            _ => (),
        }
    }
}

/// Print each field in `configuration` on its own line.
fn print_configuration(configuration: &protocol::Configuration) {
    println!(
        "ingested_reserved_memory_in_bytes: {}",
        configuration.ingested_reserved_memory_in_bytes
    );
    println!(
        "uncompressed_reserved_memory_in_bytes: {}",
        configuration.uncompressed_reserved_memory_in_bytes
    );
    println!(
        "compressed_reserved_memory_in_bytes: {}",
        configuration.compressed_reserved_memory_in_bytes
    );

    let transfer_batch_size_in_bytes = configuration
        .transfer_batch_size_in_bytes
        .map_or("not set".to_owned(), |value| value.to_string());
    println!("transfer_batch_size_in_bytes: {transfer_batch_size_in_bytes}");

    println!(
        "segment_size_threshold_in_bytes: {}",
        configuration.segment_size_threshold_in_bytes
    );
    println!(
        "optimize_target_file_size_in_bytes: {}",
        configuration.optimize_target_file_size_in_bytes
    );
    println!(
        "vacuum_retention_period_in_seconds: {}",
        configuration.vacuum_retention_period_in_seconds
    );
    println!("ingestion_threads: {}", configuration.ingestion_threads);
    println!("compression_threads: {}", configuration.compression_threads);
    println!("writer_threads: {}", configuration.writer_threads);
    println!("wal_enabled: {}", configuration.wal_enabled);
}

/// Print each field in `node_metrics` on its own line.
fn print_node_metrics(node_metrics: &protocol::NodeMetrics) {
    println!(
        "cpu_usage_percentage: {}",
        node_metrics.cpu_usage_percentage
    );
    println!("cpu_count: {}", node_metrics.cpu_count);
    println!(
        "used_memory_in_bytes: {}",
        node_metrics.used_memory_in_bytes
    );
    println!(
        "total_memory_in_bytes: {}",
        node_metrics.total_memory_in_bytes
    );
    println!(
        "used_disk_space_in_bytes: {}",
        node_metrics.used_disk_space_in_bytes
    );
    println!(
        "total_disk_space_in_bytes: {}",
        node_metrics.total_disk_space_in_bytes
    );
    println!(
        "ingested_used_memory_in_bytes: {}",
        node_metrics.ingested_used_memory_in_bytes
    );
    println!(
        "ingested_reserved_memory_in_bytes: {}",
        node_metrics.ingested_reserved_memory_in_bytes
    );
    println!(
        "uncompressed_used_memory_in_bytes: {}",
        node_metrics.uncompressed_used_memory_in_bytes
    );
    println!(
        "uncompressed_reserved_memory_in_bytes: {}",
        node_metrics.uncompressed_reserved_memory_in_bytes
    );
    println!(
        "compressed_used_memory_in_bytes: {}",
        node_metrics.compressed_used_memory_in_bytes
    );
    println!(
        "compressed_reserved_memory_in_bytes: {}",
        node_metrics.compressed_reserved_memory_in_bytes
    );
}
