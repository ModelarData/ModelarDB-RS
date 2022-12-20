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

mod helper;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::env::{self, Args};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::process;
use std::result::Result;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use arrow_flight::{Action, Criteria, FlightDescriptor};
use rustyline::Editor;
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::Request;

use crate::helper::ClientHelper;

/// Default host to connect to.
const DEFAULT_HOST: &str = "127.0.0.1";

/// Default port to connect to. The server and client currently does not support changing the port.
const DEFAULT_PORT: u16 = 9999;

/// Error to emit when the server does not provide a response when one is expected.
const TRANSPORT_ERROR: &str = "transport error: no messages received.";

/// Parse the command line arguments to extract the host running the server to connect to and the
/// file containing the queries to execute on the server. If the server host is not provided it
/// defaults to [`DEFAULT_HOST`], and if the file containing queries is not provided a
/// read-eval-print loop is opened. Returns [`String`] if the command line arguments cannot be
/// parsed, the client cannot connect to the server, or the file containing the queries cannot be
/// read.
fn main() -> Result<(), String> {
    // Parse the command line arguments.
    let args = env::args();
    if args.len() > 3 {
        // The errors are consciously ignored as the client is terminating.
        let binary_path = env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap();
        Err(format!(
            "Usage: {} [server_address] [query_file].",
            binary_name.to_str().unwrap()
        ))?;
    }
    let (maybe_host, maybe_query_file_path) = parse_command_line_arguments(args);

    // Create the Tokio runtime.
    let runtime =
        Runtime::new().map_err(|error| format!("Unable to create a Tokio Runtime: {}", error))?;

    // Connect to the server.
    let host = maybe_host.unwrap_or_else(|| DEFAULT_HOST.to_owned());
    let flight_service_client = connect(&runtime, &host, DEFAULT_PORT)
        .map_err(|error| format!("Cannot connect to {}: {}", host, error))?;

    // Execute the queries.
    if let Some(query_file) = maybe_query_file_path {
        file(runtime, flight_service_client, &query_file)
    } else {
        repl(runtime, flight_service_client)
    }
    .map_err(|error| format!("Cannot execute queries: {}", error))
}

/// Parse the command line arguments in `args` and return a pair with the address of the server to
/// connect to and the file containing the queries to execute on the server. If one of the command
/// line arguments is not provided it is replaced with [`None`] in the returned pair.
fn parse_command_line_arguments(mut args: Args) -> (Option<String>, Option<String>) {
    // Drop the path of the executable.
    args.next();

    // Parse command line arguments.
    let mut host = None;
    let mut query_file = None;

    for arg in args {
        let metadata = fs::metadata(&arg);
        if metadata.is_ok() && metadata.unwrap().is_file() {
            // Assumes all files contains queries.
            query_file = Some(arg);
        } else {
            // Assumes anything else is a host.
            host = Some(arg);
        }
    }

    (host, query_file)
}

/// Connect to the server at `host`:`port`. Returns [`Error`] if a connection to the server cannot
/// be established.
fn connect(
    runtime: &Runtime,
    host: &str,
    port: u16,
) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
    let address = format!("grpc://{}:{}", host, port);
    runtime.block_on(async { Ok(FlightServiceClient::connect(address).await?) })
}

/// Execute the actions, commands, and queries in the file at `query_file_path`.
fn file(
    runtime: Runtime,
    mut flight_service_client: FlightServiceClient<Channel>,
    query_file_path: &str,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(query_file_path)?;
    let lines = io::BufReader::new(file).lines();

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
            println!("{}", query);
            if let Err(message) = execute_and_print_action_command_or_query(
                &runtime,
                &mut flight_service_client,
                &query,
            ) {
                eprintln!("{}", message);
            }
            // Formatting newline.
            println!();
        }
    }

    Ok(())
}

/// Execute actions, commands and queries in a read-eval-print loop.
fn repl(
    runtime: Runtime,
    mut flight_service_client: FlightServiceClient<Channel>,
) -> Result<(), Box<dyn Error>> {
    // Create the read-eval-print loop.
    let mut editor = Editor::<ClientHelper>::new()?;
    let table_names = retrieve_table_names(&runtime, &mut flight_service_client)?;
    editor.set_helper(Some(ClientHelper::new(table_names)));

    // Read previously executed actions, commands, and queries from the history file.
    let history_file_name = ".modelardb_history";
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.load_history(&home);
    }

    // Execute actions, commands, and queries and print the result.
    while let Ok(line) = editor.readline("ModelarDB> ") {
        editor.add_history_entry(line.as_str());
        if let Err(message) =
            execute_and_print_action_command_or_query(&runtime, &mut flight_service_client, &line)
        {
            eprintln!("{}", message);
        }
    }

    // Append the executed actions, commands, and queries to the history file.
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.append_history(&home);
    }

    Ok(())
}

/// Execute an action, a command, or a query. Returns [`Error`] if the action, command, or query
/// could not be executed or their result could not be retrieved.
fn execute_and_print_action_command_or_query(
    runtime: &Runtime,
    flight_service_client: &mut FlightServiceClient<Channel>,
    action_command_or_query: &str,
) -> Result<(), Box<dyn Error>> {
    if action_command_or_query.starts_with('\\') {
        execute_command(runtime, flight_service_client, action_command_or_query)?;
    } else if action_command_or_query.starts_with("SELECT")
        || action_command_or_query.starts_with("EXPLAIN")
    {
        let record_batches =
            execute_query(runtime, flight_service_client, action_command_or_query)?;
        pretty::print_batches(&record_batches)?;
    } else {
        execute_action(runtime, flight_service_client, action_command_or_query)?;
    }
    Ok(())
}

/// Execute an action. Currently, only the action `CommandStatementUpdate` is supported, which
/// executes a SQL query that does not return a result on the server. The function returns [`Error`]
/// if the action could not be executed.
fn execute_action(
    runtime: &Runtime,
    flight_service_client: &mut FlightServiceClient<Channel>,
    action_body: &str,
) -> Result<(), Box<dyn Error>> {
    let action = Action {
        r#type: "CommandStatementUpdate".to_owned(),
        body: action_body.to_owned().into_bytes(),
    };

    let request = Request::new(action);

    runtime.block_on(async {
        flight_service_client
            .do_action(request)
            .await?
            .into_inner()
            .message()
            .await?;
        Ok(())
    })
}

/// Execute a command. Returns [`Error`] if:
/// * An incorrect command was provided.
/// * An incorrect argument for the command was provided.
/// * The command could not be executed.
/// * The result could not be retrieved.
fn execute_command(
    runtime: &Runtime,
    flight_service_client: &mut FlightServiceClient<Channel>,
    command_and_argument: &str,
) -> Result<(), Box<dyn Error>> {
    let mut command_and_argument = command_and_argument.split(' ');
    match command_and_argument
        .next()
        .ok_or("no command was provided")?
    {
        "\\d" => {
            //Print the schema of a table on the server.
            let table_name = command_and_argument
                .next()
                .ok_or("no table name was provided")?;
            let flight_descriptor = FlightDescriptor::new_path(vec![table_name.to_owned()]);
            let request = Request::new(flight_descriptor);
            runtime.block_on(async {
                let schema_result = flight_service_client
                    .get_schema(request)
                    .await?
                    .into_inner();
                let schema = try_schema_from_ipc_buffer(&schema_result.schema)?;
                for field in schema.fields() {
                    println!("{}: {}", field.name(), field.data_type());
                }
                Ok(())
            })
        }
        "\\dt" => {
            //Print the name of the tables on the server.
            if let Ok(tables) = retrieve_table_names(runtime, flight_service_client) {
                for table in tables {
                    println!("{}", table);
                }
            }
            Ok(())
        }
        "\\q" => {
            process::exit(0);
        }
        _ => Err(Box::new(ArrowError::ParseError(
            "unknown command".to_owned(),
        ))),
    }
}

/// Retrieve the names of the tables available on the server. Returns [`Error`] if the request could
/// not be performed or the tables names could not be retrieved.
fn retrieve_table_names(
    runtime: &Runtime,
    flight_service_client: &mut FlightServiceClient<Channel>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let criteria = Criteria { expression: vec![] };
    let request = Request::new(criteria);

    runtime.block_on(async {
        let mut stream = flight_service_client
            .list_flights(request)
            .await?
            .into_inner();

        let flight_infos = stream.message().await?.ok_or(TRANSPORT_ERROR)?;

        let mut table_names = vec![];
        if let Some(flight_descriptor) = flight_infos.flight_descriptor {
            for table_name in flight_descriptor.path {
                table_names.push(table_name);
            }
        }

        Ok(table_names)
    })
}

/// Execute a query. Returns [`Error`] if the query could not be executed or the result could not be
/// retrieved.
fn execute_query(
    runtime: &Runtime,
    flight_service_client: &mut FlightServiceClient<Channel>,
    query: &str,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    runtime.block_on(async {
        // Execute the query.
        let ticket_data = query.to_owned().into_bytes();
        let ticket = Ticket {
            ticket: ticket_data,
        };
        let mut stream = flight_service_client.do_get(ticket).await?.into_inner();

        // Get the schema of the data in the query result.
        let flight_data = stream.message().await?.ok_or(TRANSPORT_ERROR)?;
        let schema = Arc::new(Schema::try_from(&flight_data)?);

        // Get the data in the result set.
        let mut record_batches = vec![];
        let dictionaries_by_id = HashMap::new();
        while let Some(flight_data) = stream.message().await? {
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_id)?;
            record_batches.push(record_batch);
        }

        Ok(record_batches)
    })
}
