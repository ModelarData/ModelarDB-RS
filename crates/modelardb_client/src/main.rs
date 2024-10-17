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

mod errors;
mod helper;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::env::{self, Args};
use std::io::{self, BufRead, IsTerminal, Write};
use std::process;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::datatypes::{Schema, SchemaRef, ToByteSlice};
use arrow::ipc::convert;
use arrow::util::pretty;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{utils, Action, Criteria, FlightData, FlightDescriptor, Ticket};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use rustyline::history::FileHistory;
use rustyline::Editor;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::errors::{ModelarDbClientError, Result};
use crate::helper::ClientHelper;

/// Default host to connect to.
const DEFAULT_HOST: &str = "127.0.0.1";

/// Default port to connect to.
const DEFAULT_PORT: u16 = 9999;

/// Error to emit when the server does not provide a response when one is expected.
const TRANSPORT_ERROR: &str = "transport error: no messages received.";

/// Parse the command line arguments to extract the host running the server to connect to, the
/// server port to connect to, and the file containing the queries to execute on the server. If the
/// server host is not provided it defaults to [`DEFAULT_HOST`], if the server port is not provided
/// it defaults to [`DEFAULT_PORT`], and if the file containing queries is not provided a
/// read-eval-print loop is opened. Returns [`ModelarDbClientError`] if the command line arguments
/// cannot be parsed, the client cannot connect to the server, or the file containing the queries
/// cannot be read.
#[tokio::main]
async fn main() -> Result<()> {
    let current_dir = env::current_dir()?;
    let local_file_system = LocalFileSystem::new_with_prefix(current_dir)?;

    // Parse the command line arguments.
    let args = env::args();
    if args.len() > 3 {
        // The errors are consciously ignored as the client is terminating.
        let binary_path = env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap();
        eprintln!(
            "Usage: {} [server host or host:port] [query_file].",
            binary_name.to_str().unwrap()
        );
        process::exit(1);
    }
    let (maybe_host, maybe_port, maybe_query_file) =
        parse_command_line_arguments(args, &local_file_system).await?;

    // Connect to the server.
    let host = maybe_host.unwrap_or_else(|| DEFAULT_HOST.to_owned());
    let port = maybe_port.unwrap_or_else(|| DEFAULT_PORT.to_owned());
    let flight_service_client = connect(&host, port).await?;

    // Execute the queries.
    if let Some(query_file) = maybe_query_file {
        execute_queries_from_a_file(flight_service_client, &query_file, &local_file_system).await
    } else {
        execute_queries_from_a_repl(flight_service_client).await
    }
}

/// Parse the command line arguments in `args` and return a triple with the host of the server to
/// connect to, the port to connect to, and the file containing the queries to execute on the
/// server. If one of these command line arguments is not provided it is replaced with [`None`].
async fn parse_command_line_arguments(
    mut args: Args,
    local_file_system: &LocalFileSystem,
) -> Result<(Option<String>, Option<u16>, Option<ObjectMeta>)> {
    // Drop the path of the executable.
    args.next();

    // Parse command line arguments.
    let mut maybe_host = None;
    let mut maybe_port = None;
    let mut maybe_query_file = None;

    for arg in args {
        let arg_path = Path::from(arg.as_str());
        if let Ok(query_file) = local_file_system.head(&arg_path).await {
            // Assumes all files contains queries.
            maybe_query_file = Some(query_file);
        } else if arg.contains(':') {
            // Assumes anything with : is host:port.
            let host_and_port = arg.splitn(2, ':').collect::<Vec<&str>>();
            maybe_host = Some(host_and_port[0].to_owned());
            maybe_port = Some(host_and_port[1].parse().map_err(|_| {
                ModelarDbClientError::InvalidArgument(
                    "Port must be between 1 and 65535.".to_owned(),
                )
            })?);
        } else {
            // Assumes anything else is a host.
            maybe_host = Some(arg);
        }
    }

    Ok((maybe_host, maybe_port, maybe_query_file))
}

/// Connect to the server at `host`:`port`. Returns [`ModelarDbClientError`] if a connection to the
/// server cannot be established.
async fn connect(host: &str, port: u16) -> Result<FlightServiceClient<Channel>> {
    let address = format!("grpc://{host}:{port}");
    FlightServiceClient::connect(address)
        .await
        .map_err(|error| error.into())
}

/// Execute the actions, commands, and queries in `query_file`.
async fn execute_queries_from_a_file(
    mut flight_service_client: FlightServiceClient<Channel>,
    query_file: &ObjectMeta,
    local_file_system: &LocalFileSystem,
) -> Result<()> {
    let file = local_file_system.get(&query_file.location).await?;
    let bytes = file.bytes().await?;
    let lines = io::BufReader::new(bytes.to_byte_slice()).lines();

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
            execute_and_print_action_command_or_query(&mut flight_service_client, &query).await
        }
    }

    Ok(())
}

/// Execute actions, commands and queries in a read-eval-print loop.
async fn execute_queries_from_a_repl(
    mut flight_service_client: FlightServiceClient<Channel>,
) -> Result<()> {
    // Create the read-eval-print loop.
    let mut editor = Editor::<ClientHelper, FileHistory>::new()?;
    let table_names = retrieve_table_names(&mut flight_service_client).await?;
    editor.set_helper(Some(ClientHelper::new(table_names)));

    // Read previously executed actions, commands, and queries from the history file.
    let history_file_name = ".modelardb_history";
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.load_history(&home);
    }

    // Specify where to find helpful information about the commands supported by the repl.
    println!("Type \\h for help.\n");

    // Execute actions, commands, and queries and print the result.
    while let Ok(line) = editor.readline("ModelarDB> ") {
        editor.add_history_entry(line.as_str())?;
        execute_and_print_action_command_or_query(&mut flight_service_client, &line).await
    }

    // Append the executed actions, commands, and queries to the history file.
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.append_history(&home);
    }

    Ok(())
}

/// Execute an action, a command, or a query. Returns [`ModelarDbClientError`] if the action,
/// command, or query could not be executed or their result could not be retrieved.
async fn execute_and_print_action_command_or_query(
    flight_service_client: &mut FlightServiceClient<Channel>,
    action_command_or_query: &str,
) {
    let start_time = Instant::now();
    let action_command_or_query = action_command_or_query.trim();
    let action_command_or_query_upper = action_command_or_query.to_uppercase();

    let result = if action_command_or_query.starts_with('\\') {
        execute_command(flight_service_client, action_command_or_query).await
    } else if action_command_or_query_upper.starts_with("INSERT")
        || action_command_or_query_upper.starts_with("EXPLAIN")
        || action_command_or_query_upper.starts_with("SELECT")
    {
        execute_query_and_print_result(flight_service_client, action_command_or_query).await
    } else {
        execute_action(
            flight_service_client,
            "CreateTable",
            action_command_or_query,
        )
        .await
    };

    if let Err(message) = result {
        eprintln!("{message}");
    }
    println!("\nTime: {:?}\n", start_time.elapsed());
}

/// Execute an action. The function returns [`ModelarDbClientError`] if the action could not be
/// executed.
async fn execute_action(
    flight_service_client: &mut FlightServiceClient<Channel>,
    action_type: &str,
    action_body: &str,
) -> Result<()> {
    let action = Action {
        r#type: action_type.to_owned(),
        body: action_body.to_owned().into(),
    };

    let request = Request::new(action);

    flight_service_client
        .do_action(request)
        .await?
        .into_inner()
        .message()
        .await?;

    Ok(())
}

/// Execute a command. Returns [`ModelarDbClientError`] if:
/// * An incorrect command was provided.
/// * An incorrect argument for the command was provided.
/// * The command could not be executed.
/// * The result could not be retrieved.
async fn execute_command(
    flight_service_client: &mut FlightServiceClient<Channel>,
    command_and_argument: &str,
) -> Result<()> {
    let mut command_and_argument = command_and_argument.split(' ');
    match command_and_argument
        .next()
        .ok_or(ModelarDbClientError::InvalidArgument(
            "No command was provided.".to_owned(),
        ))? {
        // Print the schema of a table on the server.
        "\\d" => {
            let table_name =
                command_and_argument
                    .next()
                    .ok_or(ModelarDbClientError::InvalidArgument(
                        "No table name was provided.".to_owned(),
                    ))?;
            let flight_descriptor = FlightDescriptor::new_path(vec![table_name.to_owned()]);
            let request = Request::new(flight_descriptor);
            let schema_result = flight_service_client
                .get_schema(request)
                .await?
                .into_inner();
            let schema = convert::try_schema_from_ipc_buffer(&schema_result.schema)?;
            for field in schema.fields() {
                print!("{}: {}", field.name(), field.data_type());
                for (metadata_name, metadata_value) in field.metadata() {
                    print!(", {} {}", metadata_name, metadata_value);
                }
                println!();
            }
            Ok(())
        }
        // Print the name of the tables on the server.
        "\\dt" => {
            if let Ok(tables) = retrieve_table_names(flight_service_client).await {
                for table in tables {
                    println!("{table}");
                }
            }
            Ok(())
        }
        // Flushes all data the server currently has in memory to disk.
        "\\f" => execute_action(flight_service_client, "FlushMemory", "").await,
        // Flushes all data the server currently has in memory and disk to the object store.
        "\\F" => execute_action(flight_service_client, "FlushNode", "").await,
        // Print helpful information, explanations with \\ must be indented more to be aligned.
        "\\h" => {
            println!(
                "CREATE [MODEL] TABLE     Execute a CREATE TABLE or CREATE MODEL TABLE statement.\n\
                 INSERT INTO              Execute an INSERT INTO statement. Must include generated columns.\n\
                 SELECT                   Execute a SELECT statement.\n\
                 \\d TABLE_NAME            Print the schema of a table with TABLE_NAME.\n\
                 \\dt                      Print the name of all the tables.\n\
                 \\f                       Flushes data in memory to disk.\n\
                 \\F                       Flushes data in memory and disk to the object store.\n\
                 \\h                       Print documentation for all supported commands.\n\
                 \\q                       Quit modelardb."
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

/// Retrieve the names of the tables available on the server. Returns [`ModelarDbClientError`] if
/// the request could not be performed or the tables names could not be retrieved.
async fn retrieve_table_names(
    flight_service_client: &mut FlightServiceClient<Channel>,
) -> Result<Vec<String>> {
    let criteria = Criteria {
        expression: Bytes::new(),
    };
    let request = Request::new(criteria);

    let mut stream = flight_service_client
        .list_flights(request)
        .await?
        .into_inner();

    let flight_infos = stream
        .message()
        .await?
        .ok_or(ModelarDbClientError::InvalidArgument(
            TRANSPORT_ERROR.to_owned(),
        ))?;

    let mut table_names = vec![];
    if let Some(flight_descriptor) = flight_infos.flight_descriptor {
        for table_name in flight_descriptor.path {
            table_names.push(table_name);
        }
    }

    Ok(table_names)
}

/// Execute a query and print each batch in the result set. Returns [`ModelarDbClientError`] if the
/// query could not be executed or the batches in the result set could not be printed.
async fn execute_query_and_print_result(
    flight_service_client: &mut FlightServiceClient<Channel>,
    query: &str,
) -> Result<()> {
    // Execute the query.
    let ticket = Ticket {
        ticket: query.to_owned().into(),
    };
    let mut stream = flight_service_client.do_get(ticket).await?.into_inner();

    // Get the schema of the data in the query result.
    let flight_data = stream
        .message()
        .await?
        .ok_or(ModelarDbClientError::InvalidArgument(
            TRANSPORT_ERROR.to_owned(),
        ))?;
    let schema = Arc::new(Schema::try_from(&flight_data)?);
    let dictionaries_by_id = HashMap::new();

    if io::stdout().is_terminal() {
        print_batches_with_confirmation(stream, schema, &dictionaries_by_id).await
    } else {
        print_batches_without_confirmation(stream, schema, &dictionaries_by_id).await
    }
}

/// Print each batch in the result set with confirmation from the user before printing each batch.
/// Returns [`ModelarDbClientError`] if the batches in the result set could not be printed.
async fn print_batches_with_confirmation(
    mut stream: Streaming<FlightData>,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<()> {
    let mut user_input = String::new();
    let mut multiple_batches = false;

    while let Some(flight_data) = stream.message().await? {
        let record_batch =
            utils::flight_data_to_arrow_batch(&flight_data, schema.clone(), dictionaries_by_id)?;

        // Only ask for confirmation to print the next batch if there are multiple batches.
        if multiple_batches {
            loop {
                user_input.clear();
                print!("Press Enter for next batch and q+Enter to quit> ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut user_input)?;

                match user_input.as_str() {
                    "\n" => break,
                    "q\n" => return Ok(()),
                    _ => (),
                }
            }
        }

        pretty::print_batches(&[record_batch])?;
        multiple_batches = true;
    }

    Ok(())
}

/// Print each batch in the result set without user input. Returns [`ModelarDbClientError`] if the
/// batches in the result set could not be printed.
async fn print_batches_without_confirmation(
    mut stream: Streaming<FlightData>,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<()> {
    while let Some(flight_data) = stream.message().await? {
        let record_batch =
            utils::flight_data_to_arrow_batch(&flight_data, schema.clone(), dictionaries_by_id)?;

        pretty::print_batches(&[record_batch])?;
    }

    Ok(())
}
