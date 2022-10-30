use std::{env, io};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error;

use std::fs;
use std::io::{BufRead, Write};
use std::os::linux::process::PidFd;
use std::path;
use std::path::Path;
use std::process;
use std::process::Child;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor, FlightInfo, PutResult, SchemaAsIpc};
use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};
use assert_cmd::Command;
use assert_cmd::output::OutputError;
use datafusion::arrow::{array, ipc};
use datafusion::arrow::array::{
    Float32Array, PrimitiveArray, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::TimeUnit::Millisecond;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimestampMillisecondType};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::executor::block_on;
use futures::stream;
use nix::unistd::Pid;
use nix::sys::signal::{self, Signal};
use libc;
use log::error;
use prost::Message;
use rand::Rng;
use tokio::io::BufReader;
use tokio::runtime::Runtime;
use tonic::{IntoStreamingRequest, Request};
use tonic::transport::Channel;
use crate::array::Array;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[test]
fn test_ingest_message_into_storage_engine() {
    create_directory();

    //let mut f = fs::File::create("tests/ArrowFlight.log").expect("Failed to create log file");
    //io::copy(&mut flight_server.stdout.unwrap(), &mut f).expect("Failed to write to stdout");

    let message = generate_random_message("location".to_string());

    let mut flight_server = start_arrow_flight_server();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {

                let tag = "location".to_string();

                let _result_create_table = create_table_in_folder(&mut fsc);

                let options = ipc::writer::IpcWriteOptions::default();

                let flight_descriptor = FlightDescriptor::new_path(vec![String::from("data")]);

                let mut flight_data_vec = vec![FlightData{
                    flight_descriptor: Some(flight_descriptor),
                    data_header: vec![],
                    app_metadata: vec![],
                    data_body: vec![]
                }];

                let (_ , flight_data) = flight_data_from_arrow_batch(&message, &options);

                flight_data_vec.push(flight_data);

                let _result = send_messages_to_arrow_flight_server(&mut fsc, flight_data_vec);

            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }
    // TODO: Assert that a new segment has been created in the storage engine.

    let mut flight_server_2 = start_arrow_flight_server();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {

                let query_result = execute_query(&mut fsc,"SELECT * FROM data");

                let result = query_result.expect("Could not execute query.");


                assert_eq!(result[0], message);
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    flight_server_2.kill().expect("Could not kill process.");

    remove_directory().expect("Failed to remove directory");
}

#[test]
fn test_can_ingest_multiple_time_series_into_storage_engine() {
    // TODO: Send multiple messages from multiple different time series to do_put.
    // TODO: Assert that multiple new segments have been created in the storage engine.
}

#[test]
fn test_can_ingest_full_segment() {
    // TODO: Send BUILDER_CAPACITY messages from the same time series to do_put.
    // TODO: Assert that the full segment has been made available for compression.
}

#[test]
fn test_cannot_ingest_invalid_message() {
    // TODO: Send a single message to do put with the wrong schema.
    // TODO: Assert that the storage engine is empty.
}

#[test]
fn test_can_compress_ingested_segment() {
    // TODO: Send BUILDER_CAPACITY messages from the same time series to do_put.
    // TODO: Assert that the segment is compressed.
}

#[test]
fn test_can_compress_multiple_ingested_segments() {
    // TODO: Send BUILDER_CAPACITY messages from multiple time series to do_put.
    // TODO: Assert that all segments have been compressed.
}

#[test]
fn test_can_query_ingested_uncompressed_data() {
    // TODO: Send a single message to do_put.
    // TODO: Use the query engine to query from the time series the message belongs to.
}

#[test]
fn test_can_query_ingested_compressed_data() {
    // TODO: Send BUILDER_CAPACITY messages from the same time series to do_put.
    // TODO: Use the query engine to query from the time series the messages belong to.
}

#[test]
fn test_can_query_ingested_uncompressed_and_compressed_data() {
    // TODO: Send BUILDER_CAPACITY + 1 messages from the same time series to do_put.
    // TODO: Use the query engine to query from the time series the messages belong to.
    // TODO: Ensure that the uncompressed message is part of the query result.
}

fn get_binary_directory() -> path::PathBuf {
    let current_executable = env::current_exe().expect("Failed to get the path of the binary");

    let parent_directory = current_executable
        .parent()
        .expect("Failed to get the parent directory");

    let binary_directory = parent_directory
        .parent()
        .expect("Failed to get the directory of the binary");

    binary_directory.to_owned()
}

fn start_binary(binary: &str) -> process::Command {
    // Create path to binary
    let mut path = get_binary_directory();
    path.push(binary);
    path.set_extension(env::consts::EXE_EXTENSION);

    assert!(path.exists());

    // Create command process
    process::Command::new(path.into_os_string())
}

fn create_directory() {

    if !Path::new("tests/data").try_exists().unwrap(){
        fs::create_dir("tests/data").expect("Could not create directory");
    }
}

fn remove_directory() -> std::io::Result<()> {
    fs::remove_dir_all("tests/data")?;
    Ok(())
}

fn start_arrow_flight_server() -> process::Child  {

    let output = start_binary("modelardbd")
        .arg("tests/data")
        .stdout(process::Stdio::piped())
        .spawn()
        .expect("Failed to start Arrow Flight Server");

    return output
}

fn create_flight_service_client(rt: &Runtime, host: &str, port: u16) -> Result<FlightServiceClient<Channel>> {
    let address = format!("grpc://{}:{}", host, port);

    rt.block_on(async{
        let fsc = FlightServiceClient::connect(address).await?;
        Ok(fsc)
    })
}

fn create_table_in_folder(mut client: &mut FlightServiceClient<Channel>) -> std::result::Result<(), ()> {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to start runtime(create_table_in_folder)");

    let cmd = "CREATE MODEL TABLE data(timestamp TIMESTAMP, value FIELD, location TAG)".to_string();
    let action = Action{
        r#type: "CommandStatementUpdate".to_string(),
        body: cmd.into_bytes(),
    };
    runtime.block_on(async{
        let mut _result = client.do_action(tonic::Request::new(action)).await;

        Ok(())
    })
}

/// Send messages to the ModelarDB server with
/// Arrow Flight through the `do_put()` endpoint.
fn send_messages_to_arrow_flight_server(
    mut client: &mut FlightServiceClient<Channel>,
    flight_data_vec: Vec<FlightData>
) -> std::result::Result<(), ()> {


        let runtime = Runtime::new().expect("Failed to start runtime(send_messages_to_arrow_flight_server)");

        runtime.block_on(async {

            let flight_data_stream = stream::iter(flight_data_vec);

            let mut _streaming = client.do_put(flight_data_stream)
                .await;

            Ok(())
        })
}

/// Generate a [`RecordBatch`] with the current timestamp, a random value, and a tag.
fn generate_random_message(tag: String) -> RecordBatch {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    let value = rand::thread_rng().gen_range(0..100) as f32;

    let message_schema = Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
        Field::new("value", DataType::Float32, false),
        Field::new("location", DataType::Utf8, false),
    ]);

    RecordBatch::try_new(
        Arc::new(message_schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
            Arc::new(Float32Array::from(vec![value])),
            Arc::new(StringArray::from(vec![tag])),
        ],
    )
    .unwrap()
}

fn execute_query(
    fsc: &mut FlightServiceClient<Channel>,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    let runtime = Runtime::new().expect("Failed to start runtime(execute_query)");
    runtime.block_on(async {
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
            let dictionaries_by_id = HashMap::new();
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_id)?;
            results.push(record_batch);
        }
        Ok(results)
    })
}

pub fn status_to_arrow_error(status: tonic::Status) -> ArrowError {
    ArrowError::IoError(format!("{:?}", status))
}
