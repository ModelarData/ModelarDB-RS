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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::path::{Path as StdPath, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::ipc::convert;
use arrow::util::pretty;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Criteria, FlightData, FlightDescriptor, Ticket, utils};
use bytes::Bytes;
use clap::Parser;
use modelardb_auth::BearerInterceptor;
use rustyline::Editor;
use rustyline::history::FileHistory;
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

use crate::error::{ModelarDbClientError, Result};
use crate::helper::ClientHelper;

/// Error to emit when the server does not provide a response when one is expected.
const TRANSPORT_ERROR: &str = "transport error: no messages received.";

/// [`FlightServiceClient`] with a [`BearerInterceptor`] that attaches an authorization header.
type AuthenticatedFlightClient =
    FlightServiceClient<InterceptedService<Channel, BearerInterceptor>>;

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

    // Execute the queries.
    let flight_service_client = connect(&args.host, args.port, args.token).await?;
    if let Some(query_file) = args.query_file {
        execute_queries_from_a_file(flight_service_client, &query_file).await
    } else {
        execute_queries_from_a_repl(flight_service_client).await
    }
}

/// Connect to the server at `host`:`port` with an optional bearer `maybe_token`. Returns
/// [`ModelarDbClientError`] if a connection to the server cannot be established or the token is
/// not a valid ASCII metadata value.
async fn connect(
    host: &str,
    port: u16,
    maybe_token: Option<String>,
) -> Result<AuthenticatedFlightClient> {
    let interceptor = BearerInterceptor::try_new(maybe_token.as_deref())?;

    let address = format!("grpc://{host}:{port}");
    let connection = Endpoint::new(address)?.connect().await?;

    Ok(FlightServiceClient::with_interceptor(
        connection,
        interceptor,
    ))
}

/// Execute the commands and queries in `query_file`.
async fn execute_queries_from_a_file(
    mut flight_service_client: AuthenticatedFlightClient,
    query_file: &StdPath,
) -> Result<()> {
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
            execute_and_print_command_or_query(&mut flight_service_client, &query).await
        }
    }

    Ok(())
}

/// Execute commands and queries in a read-eval-print loop.
async fn execute_queries_from_a_repl(
    mut flight_service_client: AuthenticatedFlightClient,
) -> Result<()> {
    // Create the read-eval-print loop.
    let mut editor = Editor::<ClientHelper, FileHistory>::new()?;
    let table_names = retrieve_table_names(&mut flight_service_client).await?;
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
        execute_and_print_command_or_query(&mut flight_service_client, &line).await
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
async fn execute_and_print_command_or_query(
    flight_service_client: &mut AuthenticatedFlightClient,
    command_or_query: &str,
) {
    let start_time = Instant::now();
    let command_or_query = command_or_query.trim();

    let result = if command_or_query.starts_with('\\') {
        execute_command(flight_service_client, command_or_query).await
    } else {
        execute_query_and_print_result(flight_service_client, command_or_query).await
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
async fn execute_command(
    flight_service_client: &mut AuthenticatedFlightClient,
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
                    print!(", {metadata_name} {metadata_value}");
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

/// Retrieve the names of the tables available on the server. Returns [`ModelarDbClientError`] if
/// the request could not be performed or the tables names could not be retrieved.
async fn retrieve_table_names(
    flight_service_client: &mut AuthenticatedFlightClient,
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

/// Execute an action. Returns [`ModelarDbClientError`] if the action could not be executed.
async fn execute_action(
    flight_service_client: &mut AuthenticatedFlightClient,
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

/// Execute a query and print each batch in the result set. Returns [`ModelarDbClientError`] if the
/// query could not be executed or the batches in the result set could not be printed.
async fn execute_query_and_print_result(
    flight_service_client: &mut AuthenticatedFlightClient,
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
    schema: Arc<Schema>,
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
    schema: Arc<Schema>,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<()> {
    while let Some(flight_data) = stream.message().await? {
        let record_batch =
            utils::flight_data_to_arrow_batch(&flight_data, schema.clone(), dictionaries_by_id)?;

        pretty::print_batches(&[record_batch])?;
    }

    Ok(())
}
