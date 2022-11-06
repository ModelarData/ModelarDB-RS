use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::error::Error;
use std::{env, io};

use std::fs;
use std::io::{BufRead, Write};
use std::path;
use std::path::Path;
use std::process;
use std::process::Child;
use std::string::String;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::array::Array;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};
use arrow_flight::{
    Action, Criteria, FlightData, FlightDescriptor, FlightInfo, PutResult, SchemaAsIpc,
};
use assert_cmd::output::OutputError;
use assert_cmd::Command;
use datafusion::arrow::array::{
    ArrayAccessor, Float32Array, Int64Array, PrimitiveArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::TimeUnit::Millisecond;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimestampMillisecondType};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{array, ipc};
use futures::executor::block_on;
use futures::stream;
use libc::time;
use log::{error, Record};
use prost::Message;
use rand::Rng;
use rusqlite::ffi::sqlite3_uint64;
use sqlparser::ast::DataType::{Time};
use sqlparser::test_utils::table;
use tokio::io::BufReader;
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::{IntoStreamingRequest, Request};

pub type ArrowValue = datafusion::arrow::datatypes::Float32Type;
pub type ValueArray = PrimitiveArray<ArrowValue>;
pub type TimeSeriesId = std::primitive::u64;
pub type ArrowTimeSeriesId = datafusion::arrow::datatypes::UInt64Type;
pub type ArrowTimestamp = TimestampMillisecondType;
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;


#[test]
fn test_can_create_table() {
    //create_directory();

    //let mut flight_server = start_arrow_flight_server();

    let table_name = "data".to_string();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {
                let _result_create_table = create_table(&rt, &mut fsc, table_name.clone());

                if let Ok(tables) = retrieve_table_names(&rt, &mut fsc) {
                    assert_eq!(tables[0], table_name)
                }
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    //flight_server.kill().expect("Failed to kill server");

    //remove_directory();
}

#[test]
fn test_can_create_model_table() {
    //create_directory();

    //let mut flight_server = start_arrow_flight_server();

    let table_name = "data".to_string();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {
                let _result_create_table = create_model_table(&rt, &mut fsc, table_name.clone());

                if let Ok(tables) = retrieve_table_names(&rt, &mut fsc) {
                    assert_eq!(tables[0], table_name)
                }
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    //flight_server.kill().expect("Failed to kill server");

    //remove_directory();
}

#[test]
fn test_can_ingest_message_with_tags() {
    //create_directory();

    let tag = "location".to_string();

    let (message, schema) = generate_random_message(Some(tag.clone()));

    let options = ipc::writer::IpcWriteOptions::default();

    let table_name = "data".to_string();

    let flight_descriptor = FlightDescriptor::new_path(vec![String::from("data")]);

    let mut flight_data_vec = vec![FlightData {
        flight_descriptor: Some(flight_descriptor),
        data_header: vec![],
        app_metadata: vec![],
        data_body: vec![],
    }];

    let (_, flight_data) = flight_data_from_arrow_batch(&message, &options);

    flight_data_vec.push(flight_data);

    //let mut flight_server = start_arrow_flight_server();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {
                let _result_create_table = create_model_table(&rt, &mut fsc, table_name);

                let _ = send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);

                let _ = flush_data_to_disk(&rt, &mut fsc);

                let query = execute_query(&rt, &mut fsc, "SELECT * FROM data")
                    .expect("Could not execute query.");

                let reconstructed_recordbatch = reconstruct_recordbatch(message.clone(), query);

                assert_eq!(message, reconstructed_recordbatch);
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    //flight_server.kill().expect("Failed to kill server");

    //remove_directory();
}


#[test]
fn test_can_ingest_message_without_tags() {
    //create_directory();

    let (message, schema) = generate_random_message(None);

    let options = ipc::writer::IpcWriteOptions::default();

    let table_name = "data".to_string();

    let flight_descriptor = FlightDescriptor::new_path(vec![String::from("data")]);

    let mut flight_data_vec = vec![FlightData {
        flight_descriptor: Some(flight_descriptor),
        data_header: vec![],
        app_metadata: vec![],
        data_body: vec![],
    }];

    let (_, flight_data) = flight_data_from_arrow_batch(&message, &options);

    flight_data_vec.push(flight_data);

    //let mut flight_server = start_arrow_flight_server();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {
                let _result_create_table =
                    create_model_table_without_tags(&rt, &mut fsc, table_name);

                let _ = send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);

                let _ = flush_data_to_disk(&rt, &mut fsc);

                let query = execute_query(&rt, &mut fsc, "SELECT * FROM data")
                    .expect("Could not execute query.");

                let TimestampValue = query[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("Cannot downcast value.");

                let Value = query[0]
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("Cannot downcast value.");
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    //remove_directory();
}

#[test]
fn test_can_ingest_multiple_time_series_with_different_tags() {
    //create_directory();

    let options = ipc::writer::IpcWriteOptions::default();

    let table_name = "data".to_string();

    let flight_descriptor = FlightDescriptor::new_path(vec![String::from("data")]);

    let mut flight_data_vec = vec![FlightData {
        flight_descriptor: Some(flight_descriptor),
        data_header: vec![],
        app_metadata: vec![],
        data_body: vec![],
    }];

    let tags = vec![
        "Aalborg".to_string(),
        "Viborg".to_string(),
        "Randers".to_string(),
        "Aarhus".to_string(),
        "Hirtshals".to_string(),
    ];

    let mut record_batch_vec = vec![];

    for tag in tags {
        for _ in 1..5 {
            let (message, schema) = generate_random_message(Some(tag.clone()));

            record_batch_vec.push(message.clone());

            flight_data_vec.push(flight_data_from_arrow_batch(&message, &options).1);
        }
    }

    //let mut flight_server = start_arrow_flight_server();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {
                let _result_create_table = create_model_table(&rt, &mut fsc, table_name);

                let _ = send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);

                let _ = flush_data_to_disk(&rt, &mut fsc);

                let query = execute_query(&rt, &mut fsc, "SELECT * FROM data").expect("Could not execute query.");

                for record_batch in query{
                }


                //assert_eq!(result, record_batch_vec);
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    //remove_directory();
}

#[test]
fn test_cannot_ingest_invalid_message() {
    create_directory();

    let (message, _schema) = generate_random_message(None);

    let options = ipc::writer::IpcWriteOptions::default();

    let table_name = "data".to_string();

    let flight_descriptor = FlightDescriptor::new_path(vec![String::from("data")]);

    let mut flight_data_vec = vec![FlightData {
        flight_descriptor: Some(flight_descriptor),
        data_header: vec![],
        app_metadata: vec![],
        data_body: vec![],
    }];

    let (_, flight_data) = flight_data_from_arrow_batch(&message, &options);

    flight_data_vec.push(flight_data);

    let mut flight_server = start_arrow_flight_server();

    if let Ok(rt) = Runtime::new() {
        let address = ("127.0.0.1".to_string());
        match create_flight_service_client(&rt, &address, 9999) {
            Ok(mut fsc) => {
                let _result_create_table = create_model_table(&rt, &mut fsc, table_name);

                let _ = send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);

                let _ = flush_data_to_disk(&rt, &mut fsc);

                let query = execute_query(&rt, &mut fsc, "SELECT * FROM data")
                    .expect("Could not execute query.");

                assert!(query.is_empty());
            }
            Err(message) => eprintln!("error: cannot connect to {} due to a {}", address, message),
        }
    } else {
        eprintln!("error: unable to initialize run-time");
    }

    remove_directory();
}

/// Create a directory to be used by the Arrow Flight server.
fn create_directory() {
    if !Path::new("tests/data")
        .exists()
    {
        fs::create_dir("tests/data").expect("Could not create directory.");
    } else {
        return
    }
}

/// Remove the created directory used by the Arrow Flight Server.
fn remove_directory() {
    if Path::new("tests/data")
        .exists()
    {
        fs::remove_dir_all("tests/data").expect("Could not remove directory.");
    } else {
        return
    }
}

/// Get the directory of the binary built for integration testing.
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

/// Start the binary built for integration testing.
fn start_binary(binary: &str) -> process::Command {
    // Create path to binary
    let mut path = get_binary_directory();
    path.push(binary);
    path.set_extension(env::consts::EXE_EXTENSION);

    assert!(path.exists());

    // Create command process
    process::Command::new(path.into_os_string())
}

/// Start a new Arrow Flight Server to simulate a server for the integration tests.
fn start_arrow_flight_server() -> process::Child {
    let output = start_binary("modelardbd")
        .arg("tests/data")
        .stdout(process::Stdio::piped())
        .spawn()
        .expect("Failed to start Arrow Flight Server");

    return output;
}

/// Create a new Arrow Flight client to access the endpoints in the ModelarDB server.
fn create_flight_service_client(
    rt: &Runtime,
    host: &str,
    port: u16,
) -> Result<FlightServiceClient<Channel>> {
    let address = format!("grpc://{}:{}", host, port);

    rt.block_on(async {
        let fsc = FlightServiceClient::connect(address).await?;
        Ok(fsc)
    })
}

/// Create a model table in the ModelarDB server with Arrow Flight using the `do_action()` SQL Parser.
fn create_model_table(
    rt: &Runtime,
    mut client: &mut FlightServiceClient<Channel>,
    table_name: String,
) -> std::result::Result<(), ()> {
    let cmd = format!(
        "CREATE MODEL TABLE {}(timestamp TIMESTAMP, value FIELD, tag TAG)",
        table_name
    );
    let action = Action {
        r#type: "CommandStatementUpdate".to_string(),
        body: cmd.into_bytes(),
    };

    rt.block_on(async {
        let mut _result = client.do_action(tonic::Request::new(action)).await;

        Ok(())
    })
}

/// Create a normal table without tags in the ModelarDB server with Arrow Flight using the `do_action()` SQL Parser.
fn create_model_table_without_tags(
    rt: &Runtime,
    mut client: &mut FlightServiceClient<Channel>,
    table_name: String,
) -> std::result::Result<(), ()> {
    let cmd = format!(
        "CREATE MODEL TABLE {}(timestamp TIMESTAMP, value FIELD)",
        table_name
    );
    let action = Action {
        r#type: "CommandStatementUpdate".to_string(),
        body: cmd.into_bytes(),
    };

    rt.block_on(async {
        let mut _result = client.do_action(tonic::Request::new(action)).await;

        Ok(())
    })
}

/// Create a normal table in the ModelarDB server with Arrow Flight using the `do_action()` SQL Parser.
fn create_table(
    rt: &Runtime,
    mut client: &mut FlightServiceClient<Channel>,
    table_name: String,
) -> std::result::Result<(), ()> {
    let cmd = format!(
        "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
        table_name
    );
    let action = Action {
        r#type: "CommandStatementUpdate".to_string(),
        body: cmd.into_bytes(),
    };

    rt.block_on(async {
        let mut _result = client.do_action(tonic::Request::new(action)).await;

        Ok(())
    })
}

/// Generate a [`RecordBatch`] with the current timestamp, a random value and an optional tag.
fn generate_random_message(tag: Option<String>) -> (RecordBatch, Schema) {

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Can't generate the time.")
        .as_micros() as i64;

    let value = rand::thread_rng().gen_range(0..100) as f32;

    if tag.clone().unwrap_or("".to_string()) == "" {
        let message_schema = Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
            Field::new("value", DataType::Float32, false),
        ]);

        (
            RecordBatch::try_new(
                Arc::new(message_schema.clone()),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                    Arc::new(Float32Array::from(vec![value])),
                ],
            )
            .expect("Could not generate RecordBatch."),
            message_schema
        )
    } else {
        let message_schema = Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
            Field::new("value", DataType::Float32, false),
            Field::new("tag", DataType::Utf8, false),
        ]);

        (
            RecordBatch::try_new(
                Arc::new(message_schema.clone()),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                    Arc::new(Float32Array::from(vec![value])),
                    Arc::new(StringArray::from(vec![tag.unwrap()])),
                ],
            )
            .expect("Could not generate RecordBatch."),
            message_schema
        )
    }
}

/// Send messages to the ModelarDB server with Arrow Flight through the `do_put()` endpoint.
fn send_messages_to_arrow_flight_server(
    rt: &Runtime,
    mut client: &mut FlightServiceClient<Channel>,
    flight_data_vec: Vec<FlightData>,
) -> std::result::Result<(), ()> {
    rt.block_on(async {
        let flight_data_stream = stream::iter(flight_data_vec);

        let mut _streaming = client.do_put(flight_data_stream).await;

        Ok(())
    })
}

/// Flush the data in the StorageEngine to disk through the `do_action()` endpoint.
fn flush_data_to_disk(rt: &Runtime, fsc: &mut FlightServiceClient<Channel>) -> Result<()> {
    let action = Action {
        r#type: "Flush".to_owned(),
        body: vec![],
    };
    let request = Request::new(action);

    rt.block_on(async {
        fsc.do_action(request).await?.into_inner().message().await?;
        Ok(())
    })
}

/// Execute a query on the ModelarDB server through the `do_get()`endpoint.
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
            let dictionaries_by_id = HashMap::new();
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_id)?;
            results.push(record_batch);
        }
        Ok(results)
    })
}

/// Retrieve the table names currently in the ModelarDB server and return as a Vector.
fn retrieve_table_names(
    rt: &Runtime,
    fsc: &mut FlightServiceClient<Channel>,
) -> Result<Vec<String>> {
    let criteria = Criteria { expression: vec![] };
    let request = Request::new(criteria);

    rt.block_on(async {
        let mut stream = fsc.list_flights(request).await?.into_inner();
        let flights = stream
            .message()
            .await?
            .ok_or("transport error: no messages received")?;

        let mut table_names = vec![];
        if let Some(fd) = flights.flight_descriptor {
            for table in fd.path {
                table_names.push(table);
            }
        }
        Ok(table_names)
    })
}


/// Reconstructs the record_batch based on the queried RecordBatch and the tag and schema from the generated RecordBatch.
fn reconstruct_recordbatch(message: RecordBatch, query: Vec<RecordBatch>) -> RecordBatch {

    let tag = message
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or(&StringArray::from(vec![""]))
        .value(0)
        .to_string();

    let schema= message.schema();

    if tag == ""{
        let timestamp_value = query[0]
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Cannot downcast value.")
            .value(0);

        let value = query[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Cannot downcast value.")
            .value(0);

        let reconstructed_recordbatch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp_value])),
                Arc::new(Float32Array::from(vec![value])),
            ],
        ).expect("Could not create a Record Batch from query.");
        reconstructed_recordbatch
    }
    else {
        let timestamp_value = query[0]
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Cannot downcast value.")
            .value(0);

        let value = query[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Cannot downcast value.")
            .value(0);

        let reconstructed_recordbatch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp_value])),
                Arc::new(Float32Array::from(vec![value])),
                Arc::new(StringArray::from(vec![tag])),
            ],
        ).expect("Could not create a Record Batch from query.");
        reconstructed_recordbatch
    }
}

pub fn status_to_arrow_error(status: tonic::Status) -> ArrowError {
    ArrowError::IoError(format!("{:?}", status))
}