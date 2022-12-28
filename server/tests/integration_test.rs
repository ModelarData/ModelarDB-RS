use std::collections::HashMap;
use std::env::{self, consts};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::string::String;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use serial_test::serial;
use sysinfo::{Pid, PidExt, System, SystemExt};
use tempfile::tempdir;
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::Request;

/// These types are duplicates of the types defined in src/types.rs. They are redefined here because
/// they cannot be imported to this file.
pub type ArrowValue = datafusion::arrow::datatypes::Float32Type;
pub type ValueArray = PrimitiveArray<ArrowValue>;
pub type TimeSeriesId = std::primitive::u64;
pub type ArrowTimeSeriesId = datafusion::arrow::datatypes::UInt64Type;
pub type ArrowTimestamp = TimestampMillisecondType;

/// The different types of tables used in integration the tests.
enum TableType {
    NormalTable,
    ModelTable,
    ModelTableNoTag,
}

const TABLE_NAME: &str = "table_name";
const HOST: &str = "127.0.0.1";

#[test]
#[serial]
fn test_can_create_table() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::NormalTable,
    );

    let retrieved_table_names = retrieve_all_table_names(&runtime, &mut flight_service_client)
        .expect("Could not retrieve table names.");

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_create_model_table() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTable,
    );

    let retrieved_table_names = retrieve_all_table_names(&runtime, &mut flight_service_client)
        .expect("Could not retrieve tables.");

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_create_and_listen_multiple_model_tables() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    let table_names = vec!["data1", "data2", "data3", "data4", "data5"];
    for table_name in &table_names {
        create_table(
            &runtime,
            &mut flight_service_client,
            table_name,
            TableType::ModelTable,
        );
    }

    let retrieved_table_names = retrieve_all_table_names(&runtime, &mut flight_service_client)
        .expect("Could not retrieve tables.");

    assert_eq!(retrieved_table_names.len(), table_names.len());
    for table_name in table_names {
        assert!(retrieved_table_names.contains(&table_name.to_owned()))
    }

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_get_schema() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTable,
    );

    let schema = retrieve_schema(&runtime, &mut flight_service_client, TABLE_NAME);

    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("univariate_id", DataType::UInt64, false),
            Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
            Field::new("value", DataType::Float32, false)
        ])
    );

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_ingest_data_point_with_tags() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    let data_point = generate_random_data_point(Some("location"));
    let flight_data = create_flight_data_from_data_points(&[data_point.clone()]);

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTable,
    );

    send_data_points_to_apache_arrow_flight_server(
        &runtime,
        &mut flight_service_client,
        flight_data,
    );

    flush_data_to_disk(&runtime, &mut flight_service_client);

    let query = execute_query(
        &runtime,
        &mut flight_service_client,
        format!("SELECT * FROM {}", TABLE_NAME),
    )
    .expect("Could not execute query.");

    let reconstructed_record_batch = reconstruct_record_batch(&data_point, &query[0]);

    assert_eq!(data_point, reconstructed_record_batch);

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_ingest_data_point_without_tags() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    let data_point = generate_random_data_point(None);
    let flight_data = create_flight_data_from_data_points(&[data_point.clone()]);

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTableNoTag,
    );

    send_data_points_to_apache_arrow_flight_server(
        &runtime,
        &mut flight_service_client,
        flight_data,
    );

    flush_data_to_disk(&runtime, &mut flight_service_client);

    let query = execute_query(
        &runtime,
        &mut flight_service_client,
        format!("SELECT * FROM {}", TABLE_NAME),
    )
    .expect("Could not execute query.");

    let reconstructed_record_batch = reconstruct_record_batch(&data_point, &query[0]);

    assert_eq!(data_point, reconstructed_record_batch);

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_can_ingest_multiple_time_series_with_different_tags() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    let data_points: Vec<RecordBatch> = (1..5)
        .map(|i| generate_random_data_point(Some(&format!("location{}", i))))
        .collect();
    let flight_data = create_flight_data_from_data_points(&data_points);

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTable,
    );

    send_data_points_to_apache_arrow_flight_server(
        &runtime,
        &mut flight_service_client,
        flight_data,
    );

    flush_data_to_disk(&runtime, &mut flight_service_client);

    let query = execute_query(
        &runtime,
        &mut flight_service_client,
        format!("SELECT * FROM {}", TABLE_NAME),
    )
    .expect("Could not execute query.");

    // The loops matches the data points in the queried RecordBatches with the data points in the
    // original RecordBatches and asserts that the data points with the same timestamps are equal.
    for queried in &query {
        for original in &data_points {
            let original_timestamp = original
                .clone()
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("Cannot downcast value.")
                .value(0);

            let queried_timestamp = queried
                .clone()
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("Cannot downcast value.")
                .value(0);

            if original_timestamp == queried_timestamp {
                let reconstructed_record_batch = &reconstruct_record_batch(&original, &queried);
                assert_eq!(original, reconstructed_record_batch);
            }
        }
    }

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_cannot_ingest_invalid_data_point() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    let data_point = generate_random_data_point(None);
    let flight_data = create_flight_data_from_data_points(&[data_point]);

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTable,
    );

    send_data_points_to_apache_arrow_flight_server(
        &runtime,
        &mut flight_service_client,
        flight_data,
    );

    flush_data_to_disk(&runtime, &mut flight_service_client);

    let query = execute_query(
        &runtime,
        &mut flight_service_client,
        format!("SELECT * FROM {}", TABLE_NAME),
    )
    .expect("Could not execute query.");

    assert!(query.is_empty());

    terminate_apache_arrow_flight_server(flight_server);
}

#[test]
#[serial]
fn test_optimized_query_results_equals_non_optimized_query_results() {
    let temp_dir = tempdir().expect("Could not create a directory.");
    let flight_server = start_apache_arrow_flight_server(temp_dir.path());

    let runtime = Runtime::new().expect("Unable to initialize runtime.");
    let mut flight_service_client =
        create_apache_arrow_flight_service_client(&runtime, &HOST, 9999)
            .expect("Cannot connect to flight service client.");

    let mut data_points = vec![];
    for i in 1..5 {
        let batch = generate_random_data_point(Some(&format!("location{}", i)));

        data_points.push(batch);
    }
    let flight_data = create_flight_data_from_data_points(&data_points);

    create_table(
        &runtime,
        &mut flight_service_client,
        TABLE_NAME,
        TableType::ModelTable,
    );

    send_data_points_to_apache_arrow_flight_server(
        &runtime,
        &mut flight_service_client,
        flight_data,
    );

    flush_data_to_disk(&runtime, &mut flight_service_client);

    let optimized_query = execute_query(
        &runtime,
        &mut flight_service_client,
        format!("SELECT MIN(value) FROM {}", TABLE_NAME),
    )
    .expect("Could not execute query.");

    // The trivial filter ensures the query is rewritten by the optimizer.
    let non_optimized_query = execute_query(
        &runtime,
        &mut flight_service_client,
        format!("SELECT MIN(value) FROM {} WHERE 1=1", TABLE_NAME),
    )
    .expect("Could not execute query.");

    assert_eq!(optimized_query, non_optimized_query);

    terminate_apache_arrow_flight_server(flight_server);
}

/// Return the path to the directory containing the binary with the integration tests.
fn binary_directory() -> PathBuf {
    let current_executable = env::current_exe().expect("Failed to get the path of the binary.");

    let parent_directory = current_executable
        .parent()
        .expect("Failed to get the parent directory.");

    let binary_directory = parent_directory
        .parent()
        .expect("Failed to get the directory of the binary.");

    binary_directory.to_owned()
}

/// Execute the binary with the integration tests and return a handle to the process.
fn start_binary(binary: &str) -> Command {
    // Create path to binary.
    let mut path = binary_directory();
    path.push(binary);
    path.set_extension(consts::EXE_EXTENSION);

    assert!(path.exists());

    // Create command process.
    Command::new(path.into_os_string())
}

/// Execute the binary with the ModelarDB server and return a handle to the process.
fn start_apache_arrow_flight_server(path: &Path) -> Child {
    // Spawn the Apache Arrow Flight Server. stdout is piped to /dev/null so the logged data_points
    // are not printed when the unit tests and the integration tests are run using "cargo test".
    let process = start_binary("modelardbd")
        .arg(path)
        .stdout(Stdio::null())
        .spawn()
        .expect("Failed to start Apache Arrow Flight Server");

    // The thread needs to sleep to ensure that the server has properly started before sending
    // streams to it.
    sleep(Duration::from_secs(2));

    return process;
}

/// Return a new Apache Arrow Flight client to access the endpoints in the ModelarDB server.
fn create_apache_arrow_flight_service_client(
    runtime: &Runtime,
    host: &str,
    port: u16,
) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
    let address = format!("grpc://{}:{}", host, port);

    runtime.block_on(async {
        let flight_service_client = FlightServiceClient::connect(address).await?;
        Ok(flight_service_client)
    })
}

/// Create a normal table or model table with or without tags in the ModelarDB server through Apache
/// Arrow Flight `do_action()` method and the `CommandStatementUpdate` Apache Arrow Flight Action.
fn create_table(
    runtime: &Runtime,
    client: &mut FlightServiceClient<Channel>,
    table_name: &str,
    table_type: TableType,
) {
    let cmd = match table_type {
        TableType::NormalTable => format!(
            "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
            table_name
        ),
        TableType::ModelTable => format!(
            "CREATE MODEL TABLE {}(timestamp TIMESTAMP, value FIELD, tag TAG)",
            table_name
        ),
        TableType::ModelTableNoTag => format!(
            "CREATE MODEL TABLE {}(timestamp TIMESTAMP, value FIELD)",
            table_name
        ),
    };

    let action = Action {
        r#type: "CommandStatementUpdate".to_owned(),
        body: cmd.into_bytes(),
    };

    runtime.block_on(async {
        client
            .do_action(Request::new(action))
            .await
            .expect("Could not create table.");
    })
}

/// Return a [`RecordBatch`] containing a data point with the current time, a random value and an
/// optional tag.
fn generate_random_data_point(tag: Option<&str>) -> RecordBatch {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Cannot generate the time.")
        .as_micros() as i64;

    let value = (timestamp % 100) as f32;

    let mut fields = vec![
        Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
        Field::new("value", DataType::Float32, false),
    ];

    if let Some(tag) = tag {
        fields.push(Field::new("tag", DataType::Utf8, false));
        let data_point_schema = Schema::new(fields);
        RecordBatch::try_new(
            Arc::new(data_point_schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                Arc::new(Float32Array::from(vec![value])),
                Arc::new(StringArray::from(vec![tag])),
            ],
        )
        .expect("Could not generate RecordBatch.")
    } else {
        let data_point_schema = Schema::new(fields);
        RecordBatch::try_new(
            Arc::new(data_point_schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                Arc::new(Float32Array::from(vec![value])),
            ],
        )
        .expect("Could not generate RecordBatch.")
    }
}

/// Create and returns [`FlightData`] based on the data_points inserted.
fn create_flight_data_from_data_points(data_points: &[RecordBatch]) -> Vec<FlightData> {
    let flight_descriptor = FlightDescriptor::new_path(vec![TABLE_NAME.to_owned()]);

    let mut flight_data = vec![FlightData {
        flight_descriptor: Some(flight_descriptor),
        data_header: vec![],
        app_metadata: vec![],
        data_body: vec![],
    }];
    for data_point in data_points {
        flight_data.push(
            (flight_data_from_arrow_batch(&data_point, &ipc::writer::IpcWriteOptions::default())).1,
        );
    }

    return flight_data;
}

/// Send data_points to the ModelarDB server with Apache Arrow Flight through the `do_put()`
/// endpoint.
fn send_data_points_to_apache_arrow_flight_server(
    runtime: &Runtime,
    client: &mut FlightServiceClient<Channel>,
    flight_data: Vec<FlightData>,
) {
    runtime.block_on(async {
        let flight_data_stream = stream::iter(flight_data);

        let mut _streaming = client.do_put(flight_data_stream).await;
    })
}

/// Flush the data in the StorageEngine to disk using the `do_action()` Apache Arrow Flight method.
fn flush_data_to_disk(runtime: &Runtime, flight_service_client: &mut FlightServiceClient<Channel>) {
    let action = Action {
        r#type: "FlushMemory".to_owned(),
        body: vec![],
    };
    let request = Request::new(action);

    runtime.block_on(async {
        flight_service_client
            .do_action(request)
            .await
            .expect("Could not flush data.");
    })
}

/// Execute a query against the ModelarDB server through the `do_get()` endpoint and return it.
fn execute_query(
    runtime: &Runtime,
    flight_service_client: &mut FlightServiceClient<Channel>,
    query: String,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    runtime.block_on(async {
        // Execute query.
        let ticket_data = query.into_bytes();
        let ticket = arrow_flight::Ticket {
            ticket: ticket_data,
        };
        let mut stream = flight_service_client.do_get(ticket).await?.into_inner();

        // Get schema of result set.
        let flight_data = stream.message().await?.ok_or("No data_points received.")?;
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
        let flights = stream.message().await?.ok_or("No data_points received.")?;

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
    runtime: &Runtime,
    client: &mut FlightServiceClient<Channel>,
    table_name: &str,
) -> Schema {
    runtime.block_on(async {
        let schema_result = client
            .get_schema(Request::new(FlightDescriptor::new_path(vec![
                table_name.to_owned()
            ])))
            .await
            .expect("Could not retrieve schema.")
            .into_inner();

        let schema = try_schema_from_ipc_buffer(&schema_result.schema)
            .expect("Could not convert SchemaResult to schema.");
        schema
    })
}

/// Return a [`RecordBatch`] based on the queried [`RecordBatch`] and the tag and schema from the
/// original [`RecordBatch`]. Needed as the server currently returns the raw univariate time series
/// without tags instead of a multivariate time series with tags that match the model tables schema.
fn reconstruct_record_batch(original: &RecordBatch, query: &RecordBatch) -> RecordBatch {
    let timestamp = query
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
                .to_owned(),
        )
    } else {
        None
    };

    let schema = original.schema();

    if let Some(tag) = tag {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                Arc::new(Float32Array::from(vec![value])),
                Arc::new(StringArray::from(vec![tag])),
            ],
        )
        .expect("Could not create a Record Batch from query.")
    } else {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                Arc::new(Float32Array::from(vec![value])),
            ],
        )
        .expect("Could not create a Record Batch from query.")
    }
}

/// Terminate the Apache Arrow Flight Server and return when the process has been terminated.
fn terminate_apache_arrow_flight_server(mut flight_server: Child) {
    let mut system = System::new_all();

    while let Some(_process) = system.process(Pid::from_u32(flight_server.id())) {
        flight_server
            .kill()
            .expect(&*format!("Could not kill process {}.", flight_server.id()));
        flight_server.wait().expect(&*format!(
            "Could not wait for process {}.",
            flight_server.id()
        ));
        system.refresh_all();
    }
}
