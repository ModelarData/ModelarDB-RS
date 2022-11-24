use std::borrow::Borrow;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::path;
use std::path::Path;
use std::process;
use std::process::Child;
use std::string::{String, ToString};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use array::Array;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};
use arrow_flight::{Action, Criteria, FlightData, FlightDescriptor};
use datafusion::arrow::array::{
    Float32Array, PrimitiveArray, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::TimeUnit::Millisecond;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimestampMillisecondType};
use datafusion::arrow::ipc::convert::try_schema_from_ipc_buffer;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{array, ipc};
use futures::stream;
use rand::Rng;
use serial_test::serial;
use sysinfo::{Pid, PidExt, ProcessExt, ProcessStatus, System, SystemExt};
use tempfile::tempdir;
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::Request;

/// The following types are duplicates of the types defined
/// in src/types.rs. They are redefined here because they cannot
/// be imported to this file.
pub type ArrowValue = datafusion::arrow::datatypes::Float32Type;
pub type ValueArray = PrimitiveArray<ArrowValue>;
pub type TimeSeriesId = std::primitive::u64;
pub type ArrowTimeSeriesId = datafusion::arrow::datatypes::UInt64Type;
pub type ArrowTimestamp = TimestampMillisecondType;

const TABLE_NAME: &str = "data";
const ADDRESS: &str = "127.0.0.1";

#[test]
#[serial]
fn test_can_create_table() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    create_table(&rt, &mut fsc, TABLE_NAME, "NormalTable");

    let retrieved_tables =
        retrieve_all_table_names(&rt, &mut fsc).expect("Could not retrieve tables.");

    assert_eq!(retrieved_tables[0], TABLE_NAME);

    terminate_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_create_model_table() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTable");

    let retrieved_tables =
        retrieve_all_table_names(&rt, &mut fsc).expect("Could not retrieve tables.");

    assert_eq!(retrieved_tables[0], TABLE_NAME);

    terminate_arrow_flight_server(flight_server);

}

#[test]
#[serial]
fn test_creating_and_listing_multiple_tables() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    let created_tables = vec!["data1", "data2", "data3", "data4", "data5"];
    for table_name in created_tables.clone() {
        create_table(&rt, &mut fsc, table_name, "ModelTable");
    }

    let retrieved_tables =
        retrieve_all_table_names(&rt, &mut fsc).expect("Could not retrieve tables.");

    for table in created_tables.clone() {
        assert!(retrieved_tables.contains(&table.to_string()))
    }

    terminate_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_get_schema() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTable");

    let schema = retrieve_schema(&rt, &mut fsc, TABLE_NAME);

    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("tid", DataType::UInt64, false),
            Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
            Field::new("value", DataType::Float32, false)
        ])
    );

    terminate_arrow_flight_server(flight_server);

}

#[test]
#[serial]
fn test_can_ingest_message_with_tags() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    let message = generate_random_message(Some("location"));
    let flight_data_vec = create_flight_data_from_messages(&vec![message.clone()]);

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTable");

    send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);
    flush_data_to_disk(&rt, &mut fsc);

    let query =
        execute_query(&rt, &mut fsc, "SELECT * FROM data").expect("Could not execute query.");

    let reconstructed_record_batch = reconstruct_record_batch(&message, &query[0]);

    assert_eq!(message, reconstructed_record_batch);

    terminate_arrow_flight_server(flight_server);

}

#[test]
#[serial]
fn test_can_ingest_message_without_tags() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    let message = generate_random_message(None);
    let flight_data_vec = create_flight_data_from_messages(&vec![message.clone()]);

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTableNoTag");

    send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);
    flush_data_to_disk(&rt, &mut fsc);

    let query =
        execute_query(&rt, &mut fsc, "SELECT * FROM data").expect("Could not execute query.");

    let reconstructed_record_batch = reconstruct_record_batch(&message, &query[0]);

    assert_eq!(message, reconstructed_record_batch);

    terminate_arrow_flight_server(flight_server);

}

#[test]
#[serial]
fn test_can_ingest_multiple_time_series_with_different_tags() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    let mut messages = vec![];
    for i in 1..5 {
        let batch = generate_random_message(Some(&format!("location{}", i)));

        messages.push(batch);
    }
    let flight_data_vec = create_flight_data_from_messages(&messages);

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTable");

    send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);
    flush_data_to_disk(&rt, &mut fsc);

    let query =
        execute_query(&rt, &mut fsc, "SELECT * FROM data").expect("Could not execute query.");

    // The following loop matches the queried record batches with the
    // original record batches and asserts that they exist in
    // in the query.
    for queried in &query {
        for original in &messages {
            let original_timestamp_value = original
                .clone()
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("Cannot downcast value.")
                .value(0);

            let queried_timestamp_value = queried
                .clone()
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("Cannot downcast value.")
                .value(0);

            if original_timestamp_value == queried_timestamp_value {
                let reconstructed_record_batch = &reconstruct_record_batch(&original, &queried);
                assert_eq!(original, reconstructed_record_batch);
            }
        }
    }

    terminate_arrow_flight_server(flight_server);

}

#[test]
#[serial]
fn test_cannot_ingest_invalid_message() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    let message = generate_random_message(None);
    let flight_data_vec = create_flight_data_from_messages(&vec![message]);

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTable");

    send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);
    flush_data_to_disk(&rt, &mut fsc);

    let query =
        execute_query(&rt, &mut fsc, "SELECT * FROM data").expect("Could not execute query.");

    assert!(query.is_empty());

    terminate_arrow_flight_server(flight_server);

}

#[test]
#[serial]
fn test_optimized_query_equals_non_optimized_query() {
    let dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_arrow_flight_server(dir.path());

    let rt = Runtime::new().expect("Unable to initialize run-time.");
    let mut fsc = create_flight_service_client(&rt, &ADDRESS, 9999)
        .expect("Cannot connect to flight service client.");

    let mut messages = vec![];
    for i in 1..5 {
        let batch = generate_random_message(Some(&format!("location{}", i)));

        messages.push(batch);
    }
    let flight_data_vec = create_flight_data_from_messages(&messages);

    create_table(&rt, &mut fsc, TABLE_NAME, "ModelTable");

    send_messages_to_arrow_flight_server(&rt, &mut fsc, flight_data_vec);
    flush_data_to_disk(&rt, &mut fsc);

    let optimized_query = execute_query(&rt, &mut fsc, "SELECT MIN(value) FROM data")
        .expect("Could not execute query.");

    // The following results in a non-optimized query
    // because the optimizer only works on aggregate queries
    // without a filter.
    let non_optimized_query = execute_query(&rt, &mut fsc, "SELECT MIN(value) FROM data WHERE 1=1")
        .expect("Could not execute query.");

    assert_eq!(optimized_query, non_optimized_query);

    terminate_arrow_flight_server(flight_server);

}

/// Retrieve and return the directory of the binary built for integration testing.
fn get_binary_directory() -> path::PathBuf {
    let current_executable = env::current_exe().expect("Failed to get the path of the binary.");

    let parent_directory = current_executable
        .parent()
        .expect("Failed to get the parent directory.");

    let binary_directory = parent_directory
        .parent()
        .expect("Failed to get the directory of the binary.");

    binary_directory.to_owned()
}

/// Start and return the binary built for integration testing.
fn start_binary(binary: &str) -> process::Command {
    // Create path to binary
    let mut path = get_binary_directory();
    path.push(binary);
    path.set_extension(env::consts::EXE_EXTENSION);

    assert!(path.exists());

    // Create command process
    process::Command::new(path.into_os_string())
}

/// Start and return a new Arrow Flight Server to simulate a server for the integration tests.
fn start_arrow_flight_server(dir: &Path) -> Child {
    let mut process = start_binary("modelardbd")
        .arg(dir)
        .stdout(process::Stdio::piped())
        .spawn()
        .expect("Failed to start Arrow Flight Server");

    // Ensure that the process has fully started.
    let mut output = BufReader::new(process.stdout.as_mut().unwrap());
    let mut line = String::new();
    while !line.contains("Starting Apache Arrow Flight") {
        output
            .read_line(&mut line)
            .expect("Could not read line of process.");
    }

    return process;
}

/// Create and return a new Arrow Flight client to access the endpoints in the ModelarDB server.
fn create_flight_service_client(
    rt: &Runtime,
    host: &str,
    port: u16,
) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
    let address = format!("grpc://{}:{}", host, port);

    rt.block_on(async {
        let fsc = FlightServiceClient::connect(address).await?;
        Ok(fsc)
    })
}

/// Create a normal table or model table with/without tags in the
/// ModelarDB server with Arrow Flight using the `do_action()` SQL Parser.
///
/// mode options: NormalTable/ModelTable/ModelTableNoTag
fn create_table(
    rt: &Runtime,
    client: &mut FlightServiceClient<Channel>,
    table_name: &str,
    mode: &str,
) {
    let cmd = match mode {
        "NormalTable" => format!(
            "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
            table_name
        ),
        "ModelTable" => format!(
            "CREATE MODEL TABLE {}(timestamp TIMESTAMP, value FIELD, tag TAG)",
            table_name
        ),
        "ModelTableNoTag" => format!(
            "CREATE MODEL TABLE {}(timestamp TIMESTAMP, value FIELD)",
            table_name
        ),
        _ => panic!("No mode selected in create_table."),
    };

    let action = Action {
        r#type: "CommandStatementUpdate".to_string(),
        body: cmd.into_bytes(),
    };

    rt.block_on(async {
        client
            .do_action(Request::new(action))
            .await
            .expect("Could not create table.");
    })
}

/// Generate and return a [`RecordBatch`] with the current timestamp, a random value and an optional tag.
fn generate_random_message(tag: Option<&str>) -> RecordBatch {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Cannot generate the time.")
        .as_micros() as i64;
    let value = rand::thread_rng().gen_range(0..100) as f32;

    let mut fields = vec![
        Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
        Field::new("value", DataType::Float32, false),
    ];

    if let Some(tag) = tag {
        fields.push(Field::new("tag", DataType::Utf8, false));
        let message_schema = Schema::new(fields);
        RecordBatch::try_new(
            Arc::new(message_schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                Arc::new(Float32Array::from(vec![value])),
                Arc::new(StringArray::from(vec![tag])),
            ],
        )
        .expect("Could not generate RecordBatch.")
    } else {
        let message_schema = Schema::new(fields);
        RecordBatch::try_new(
            Arc::new(message_schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                Arc::new(Float32Array::from(vec![value])),
            ],
        )
        .expect("Could not generate RecordBatch.")
    }
}

/// Creates and returns FlightData based on the messages inserted.
fn create_flight_data_from_messages(messages: &Vec<RecordBatch>) -> Vec<FlightData> {
    let flight_descriptor = FlightDescriptor::new_path(vec![TABLE_NAME.to_string()]);

    let mut flight_data_vec = vec![FlightData {
        flight_descriptor: Some(flight_descriptor),
        data_header: vec![],
        app_metadata: vec![],
        data_body: vec![],
    }];
    for message in messages {
        flight_data_vec.push(
            (flight_data_from_arrow_batch(&message, &ipc::writer::IpcWriteOptions::default())).1,
        );
    }

    return flight_data_vec;
}

/// Send messages to the ModelarDB server with Arrow Flight through the `do_put()` endpoint.
fn send_messages_to_arrow_flight_server(
    rt: &Runtime,
    client: &mut FlightServiceClient<Channel>,
    flight_data_vec: Vec<FlightData>,
) {
    rt.block_on(async {
        let flight_data_stream = stream::iter(flight_data_vec);

        let mut _streaming = client.do_put(flight_data_stream).await;
    })
}

/// Flush the data in the StorageEngine to disk through the `do_action()` endpoint.
fn flush_data_to_disk(rt: &Runtime, fsc: &mut FlightServiceClient<Channel>) {
    let action = Action {
        r#type: "FlushMemory".to_owned(),
        body: vec![],
    };
    let request = Request::new(action);

    rt.block_on(async {
        fsc.do_action(request).await.expect("Could not flush data.");
    })
}

/// Execute a query on the ModelarDB server through the `do_get()` endpoint and return it.
fn execute_query(
    rt: &Runtime,
    fsc: &mut FlightServiceClient<Channel>,
    query: &str,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    rt.block_on(async {
        // Execute query.
        let ticket_data = query.to_owned().into_bytes();
        let ticket = arrow_flight::Ticket {
            ticket: ticket_data,
        };
        let mut stream = fsc.do_get(ticket).await?.into_inner();

        // Get schema of result set.
        let flight_data = stream.message().await?.ok_or("No messages received.")?;
        let schema = Arc::new(Schema::try_from(&flight_data)?);

        // Get data in result set.
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

/// Retrieve the table names currently in the ModelarDB server and return them.
fn retrieve_all_table_names(
    rt: &Runtime,
    fsc: &mut FlightServiceClient<Channel>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let criteria = Criteria { expression: vec![] };
    let request = Request::new(criteria);

    rt.block_on(async {
        let mut stream = fsc.list_flights(request).await?.into_inner();
        let flights = stream.message().await?.ok_or("No messages received.")?;

        let mut table_names = vec![];
        if let Some(fd) = flights.flight_descriptor {
            for table in fd.path {
                table_names.push(table);
            }
        }
        Ok(table_names)
    })
}

/// Retrieve the schema of a table in the ModelarDB server and return it.
fn retrieve_schema(
    rt: &Runtime,
    client: &mut FlightServiceClient<Channel>,
    table_name: &str,
) -> Schema {
    rt.block_on(async {
        let schema_result = client
            .get_schema(Request::new(FlightDescriptor::new_path(vec![
                table_name.to_string()
            ])))
            .await
            .expect("Could not retrieve schema.")
            .into_inner();

        let schema = try_schema_from_ipc_buffer(&schema_result.schema)
            .expect("Could not convert SchemaResult to schema.");
        schema
    })
}

/// Reconstructs and returns a RecordBatch based on the queried RecordBatch
/// and the tag and schema from the original RecordBatch.
///
/// This is necessary because the current implementation of querying
/// and retrieving schemas does not return the original tag column,
/// and therefore does not return the original schema.
fn reconstruct_record_batch(original: &RecordBatch, query: &RecordBatch) -> RecordBatch {
    let timestamp_value = query
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("Cannot downcast value.")
        .value(0);

    let value = query
        .column(2)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Cannot downcast value.")
        .value(0);

    let tag = if original.num_columns() == 3 {
        Some(
            original
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or(&StringArray::from(vec![""]))
                .value(0)
                .to_string(),
        )
    } else {
        None
    };

    let schema = original.schema();

    if let Some(tag) = tag {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp_value])),
                Arc::new(Float32Array::from(vec![value])),
                Arc::new(StringArray::from(vec![tag])),
            ],
        )
        .expect("Could not create a Record Batch from query.")
    } else {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp_value])),
                Arc::new(Float32Array::from(vec![value])),
            ],
        )
        .expect("Could not create a Record Batch from query.")
    }
}

/// Terminates the Arrow Flight Server, and only returns when the process has been terminated.
fn terminate_arrow_flight_server(flight_server: Child) {
    let mut s = System::new_all();

    while let Some(process) = s.process(Pid::from_u32(flight_server.id())){
        if process.status() == ProcessStatus::Run{
            process.kill();
            s.refresh_processes();
        }
        else {
            break;
        }
    }
}
