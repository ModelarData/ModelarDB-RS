/* Copyright 2021 The MiniModelarDB Contributors
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
use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::fs::{metadata, File};
use std::io::{self, BufRead};
use std::process;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::convert::schema_from_bytes;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{Criteria, FlightDescriptor};

use tokio::runtime::Runtime;

use tonic::transport::Channel;
use tonic::Request;

/** Public Types **/
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/** Public Methods **/
fn main() {
    let args = env::args();
    if args.len() > 3 {
        //The errors are consciously ignored as the program is terminating
        let binary_path = env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap();
        println!(
            "usage: {} [server_address] [query_file]",
            binary_name.to_str().unwrap()
        );
        return;
    }
    let (address, query_file) = parse_arguments(args);

    if let Ok(rt) = tokio::runtime::Runtime::new() {
        let address = address.unwrap_or_else(|| "127.0.0.1".to_string());
        match connect(&rt, &address, 9999) {
            Ok(fsc) => {
                //Execute queries
                let result = if let Some(query_file) = query_file {
                    file(rt, fsc, &query_file)
                } else {
                    repl(rt, fsc)
                };

                //Print error
                match result {
                    Ok(_) => (),
                    Err(message) => eprintln!("{}", message),
                };
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }
}

/** Private Functions **/
fn parse_arguments(mut args: env::Args) -> (Option<String>, Option<String>) {
    args.next(); //Drop the path of the executable
    let mut address = None;
    let mut query_file = None;

    for arg in args {
        let metadata = metadata(&arg);
        if metadata.is_ok() && metadata.unwrap().is_file() {
            query_file = Some(arg); //A file is probably the query file
        } else {
            address = Some(arg); //Otherwise, it is probably the address
        }
    }
    (address, query_file)
}

fn connect(rt: &Runtime, host: &str, port: u16) -> Result<FlightServiceClient<Channel>> {
    let address = format!("grpc://{}:{}", host, port);
    rt.block_on(async {
        let fsc = FlightServiceClient::connect(address).await?;
        Ok(fsc)
    })
}

fn file(rt: Runtime, mut fsc: FlightServiceClient<Channel>, queries_path: &str) -> Result<()> {
    let file = File::open(queries_path)?;
    let lines = io::BufReader::new(file).lines();

    for line in lines {
        let input = line?;
        println!("{}", &input);
        if let Err(message) = execute_and_print_query_or_command(&rt, &mut fsc, &input) {
            eprintln!("{}", message);
        }
        println!(); //Formatting newline
    }
    Ok(())
}

fn repl(rt: Runtime, mut fsc: FlightServiceClient<Channel>) -> Result<()> {
    let mut editor = rustyline::Editor::<()>::new();
    let history_file_name = ".mmdbc_history";
    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.load_history(&home);
    }

    while let Ok(line) = editor.readline("ModelarDB> ") {
        editor.add_history_entry(line.as_str());
        if let Err(message) = execute_and_print_query_or_command(&rt, &mut fsc, &line) {
            eprintln!("{}", message);
        }
    }

    if let Some(mut home) = dirs::home_dir() {
        home.push(history_file_name);
        let _ = editor.append_history(&home);
    }
    Ok(())
}

fn execute_and_print_query_or_command(
    rt: &Runtime,
    fsc: &mut FlightServiceClient<Channel>,
    query_or_command: &str,
) -> Result<()> {
    if query_or_command.starts_with('\\') {
        execute_command(rt, fsc, query_or_command)?;
    } else {
        let df = execute_query(rt, fsc, query_or_command)?;
        pretty::print_batches(&df)?;
    }
    Ok(())
}

fn execute_command(
    rt: &Runtime,
    fsc: &mut FlightServiceClient<Channel>,
    command_and_argument: &str,
) -> Result<()> {
    let mut command_and_argument = command_and_argument.split(' ');
    match command_and_argument
        .next()
        .ok_or("no command was provided")?
    {
        "\\d" => {
            //List schema for table
            let table_name = command_and_argument
                .next()
                .ok_or("no table name was provided")?;
            let fd = FlightDescriptor::new_path(vec![table_name.to_string()]);
            let request = Request::new(fd);
            rt.block_on(async {
                let schema_result = fsc.get_schema(request).await?.into_inner();
                let schema = schema_from_bytes(&schema_result.schema)?;
                for field in schema.fields() {
                    println!("{}: {}", field.name(), field.data_type());
                }
                Ok(())
            })
        }
        "\\dt" => {
            //List all tables
            let criteria = Criteria { expression: vec![] };
            let request = Request::new(criteria);
            rt.block_on(async {
                let mut stream = fsc.list_flights(request).await?.into_inner();
                let flights = stream
                    .message()
                    .await?
                    .ok_or("transport error: no messages received")?;
                if let Some(fd) = flights.flight_descriptor {
                    for table in fd.path {
                        println!("{}", table);
                    }
                }
                Ok(())
            })
        }
        "\\q" => {
            process::exit(0);
        }
        _ => Err(Box::new(ArrowError::ParseError(
            "unknown command".to_owned(),
        ))),
    }
}

fn execute_query(
    rt: &Runtime,
    fsc: &mut FlightServiceClient<Channel>,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    rt.block_on(async {
        //Execute query
        let ticket_data = query.to_owned().into_bytes();
        let ticket = arrow_flight::Ticket {
            ticket: ticket_data,
        };
        let mut stream = fsc.do_get(ticket).await?.into_inner();

        //Get schema of result set
        let flight_data = stream
            .message()
            .await?
            .ok_or("transport error: no messages received")?;
        let schema = Arc::new(Schema::try_from(&flight_data)?);

        //Get data in result set
        let mut results = vec![];
        while let Some(flight_data) = stream.message().await? {
            let dictionaries_by_field = vec![None; schema.fields().len()];
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_field)?;
            results.push(record_batch);
        }
        Ok(results)
    })
}
