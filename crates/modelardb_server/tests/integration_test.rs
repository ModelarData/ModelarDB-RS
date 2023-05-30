/* Copyright 2022 The ModelarDB Contributors
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

//! Integration tests for modelardb_server's Apache Arrow Flight endpoints.

use std::collections::HashMap;
use std::env::{self, consts};
use std::error::Error;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::str;
use std::string::String;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{utils, Action, Criteria, FlightData, FlightDescriptor, PutResult, Ticket};
use bytes::Bytes;
use datafusion::arrow::array::{Float32Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit::Millisecond};
use datafusion::arrow::ipc::convert;
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, StreamExt};
use sysinfo::{Pid, PidExt, System, SystemExt};

use tempfile::TempDir;
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

const TABLE_NAME: &str = "table_name";
const HOST: &str = "127.0.0.1";

/// The next port to be used for the server in an integration test. Each test uses a unique port and
/// local data folder so they can run in parallel and so that any failing tests does not cascade.
static PORT: AtomicU16 = AtomicU16::new(9999);

/// Number of times to try executing something that may sometimes temporarily fail.
const ATTEMPTS: u8 = 10;

/// Amount of time to sleep between each attempt when trying to execute something that may sometimes
/// temporarily fail.
const ATTEMPT_SLEEP_IN_SECONDS: Duration = Duration::from_secs(1);

/// The different types of tables used in the integration tests.
enum TableType {
    NormalTable,
    ModelTable,
    ModelTableNoTag,
    ModelTableAsField,
}

/// Async runtime, handler to the server process, and client for use by the tests. It implements
/// `drop()` so the resources it manages is released no matter if a test succeeds, fails, or panics.
struct TestContext {
    temp_dir: TempDir,
    port: u16,
    runtime: Runtime,
    server: Child,
    client: FlightServiceClient<Channel>,
}

impl TestContext {
    /// Create a [`Runtime`], a server that stores data in a randomly generated local data folder
    /// and listens on `PORT`, and a client and ensure they are all ready to be used in the tests.
    fn new() -> Self {
        let runtime = Runtime::new().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let port = PORT.fetch_add(1, Ordering::Relaxed);
        let mut server = Self::create_server(temp_dir.path(), port);
        let client = Self::create_client(&runtime, &mut server, port);

        Self {
            temp_dir,
            port,
            runtime,
            server,
            client,
        }
    }

    /// Restart the server and reconnect the client.
    fn restart_server(&mut self) {
        Self::kill_child(&mut self.server);
        self.server = Self::create_server(self.temp_dir.path(), self.port);
        self.client = Self::create_client(&self.runtime, &mut self.server, self.port);
    }

    /// Create a server that stores data in `local_data_folder` and listens on `port` and ensure it
    /// is ready to receive requests.
    fn create_server(local_data_folder: &Path, port: u16) -> Child {
        // The server's stdout and stderr is piped so the log messages (stdout) and expected errors
        // (stderr) are not printed when all of the tests are run using the "cargo test" command.
        let mut server = Self::create_command("modelardbd")
            .unwrap()
            .env("MODELARDBD_PORT", port.to_string())
            .arg(local_data_folder)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        // Ensure that the server has started before executing the test. stdout will not include EOF
        // until the process ends, so the bytes are read one at a time until the output stating that
        // the server is ready is printed. If this ever becomes a bottleneck tokio::process::Command
        // looks like an alternative if it can be used without adding features to the server itself.
        let stdout = server.stdout.as_mut().unwrap();
        let mut stdout_bytes = stdout.bytes().flatten();
        let mut stdout_output: Vec<u8> = Vec::with_capacity(512);
        let stdout_expected = "Starting Apache Arrow Flight on".as_bytes();
        while stdout_output.len() < stdout_expected.len()
            || &stdout_output[stdout_output.len() - stdout_expected.len()..] != stdout_expected
        {
            stdout_output.push(stdout_bytes.next().unwrap());
        }

        server
    }

    /// Return a [`Command`] that can run the executable `binary` located in the same folder as the
    /// binary containing the execution test. If the `binary` does not exists, [`None`] is returned.
    fn create_command(binary: &str) -> Option<Command> {
        // Compute the folder containing the binary.
        let current_executable = env::current_exe().unwrap();
        let parent_directory = current_executable.parent().unwrap();
        let binary_directory = parent_directory.parent().unwrap();

        // Compute the path to the binary with suffix.
        let mut binary_path = binary_directory.to_owned();
        binary_path.push(binary);
        binary_path.set_extension(consts::EXE_EXTENSION);

        // Create the command if the binary exists.
        if binary_path.exists() {
            Some(Command::new(binary_path))
        } else {
            None
        }
    }

    /// Create the client and ensure it can connect to the server.
    fn create_client(
        runtime: &Runtime,
        server: &mut Child,
        port: u16,
    ) -> FlightServiceClient<Channel> {
        // Despite waiting for the expected output, the client may not able to connect the first
        // time. This rarely happens on a local machine but happens more often in GitHub Actions.
        let mut attempts = ATTEMPTS;
        let client = loop {
            if let Ok(client) = Self::create_apache_arrow_flight_service_client(runtime, HOST, port)
            {
                break client;
            } else if attempts == 0 {
                Self::kill_child(server);
                panic!("The Apache Arrow Flight client could not connect to modelardbd.");
            } else {
                thread::sleep(ATTEMPT_SLEEP_IN_SECONDS);
                attempts -= 1;
            }
        };

        client
    }

    /// Kill a `child` process.
    fn kill_child(child: &mut Child) {
        let mut system = System::new_all();

        while let Some(_process) = system.process(Pid::from_u32(child.id())) {
            system.refresh_all();

            // Microsoft Windows often fail to kill the process and then succeed afterwards.
            let mut attempts = ATTEMPTS;
            while child.kill().is_err() && attempts > 0 {
                thread::sleep(ATTEMPT_SLEEP_IN_SECONDS);
                attempts -= 1;
            }
            child.wait().unwrap();

            system.refresh_all();
        }
    }

    /// Return a Apache Arrow Flight client to access the remote methods provided by the server.
    fn create_apache_arrow_flight_service_client(
        runtime: &Runtime,
        host: &str,
        port: u16,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
        let address = format!("grpc://{host}:{port}");

        runtime.block_on(async {
            let client = FlightServiceClient::connect(address).await?;
            Ok(client)
        })
    }

    /// Create a normal table or model table with or without tags in the server through the
    /// `do_action()` method and the `CommandStatementUpdate` action.
    fn create_table(&mut self, table_name: &str, table_type: TableType) {
        let cmd = match table_type {
            TableType::NormalTable => {
                format!("CREATE TABLE {table_name}(timestamp TIMESTAMP, value REAL, metadata REAL)")
            }
            TableType::ModelTable => {
                format!(
                "CREATE MODEL TABLE {table_name}(timestamp TIMESTAMP, value FIELD(0.0), tag TAG)"
            )
            }
            TableType::ModelTableNoTag => {
                format!("CREATE MODEL TABLE {table_name}(timestamp TIMESTAMP, value FIELD)")
            }
            TableType::ModelTableAsField => {
                format!(
                    "CREATE MODEL TABLE {table_name}(timestamp TIMESTAMP,
                 generated FIELD AS (value + CAST(37.0 AS REAL)), value FIELD(0.0))"
                )
            }
        };

        let action = Action {
            r#type: "CommandStatementUpdate".to_owned(),
            body: cmd.into(),
        };

        self.runtime.block_on(async {
            self.client.do_action(Request::new(action)).await.unwrap();
        })
    }

    /// Return a [`RecordBatch`] containing a data point with the current time, a random value and an
    /// optional tag.
    fn generate_random_data_point(tag: Option<&str>) -> RecordBatch {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
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
            .unwrap()
        } else {
            let data_point_schema = Schema::new(fields);
            RecordBatch::try_new(
                Arc::new(data_point_schema),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                    Arc::new(Float32Array::from(vec![value])),
                ],
            )
            .unwrap()
        }
    }

    /// Create and return [`FlightData`] based on the `data_points` to be inserted into `table_name`.
    fn create_flight_data_from_data_points(
        table_name: String,
        data_points: &[RecordBatch],
    ) -> Vec<FlightData> {
        let flight_descriptor = FlightDescriptor::new_path(vec![table_name]);

        let mut flight_data = vec![FlightData {
            flight_descriptor: Some(flight_descriptor),
            data_header: Bytes::new(),
            app_metadata: Bytes::new(),
            data_body: Bytes::new(),
        }];

        let data_generator = IpcDataGenerator::default();
        let writer_options = IpcWriteOptions::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        for data_point in data_points {
            let (_encoded_dictionaries, encoded_batch) = data_generator
                .encoded_batch(data_point, &mut dictionary_tracker, &writer_options)
                .unwrap();
            flight_data.push(encoded_batch.into());
        }

        flight_data
    }

    /// Send data points to the server through the `do_put()` method.
    fn send_data_points_to_server(
        &mut self,
        flight_data: Vec<FlightData>,
    ) -> Result<Response<Streaming<PutResult>>, Status> {
        self.runtime.block_on(async {
            let flight_data_stream = stream::iter(flight_data);
            self.client.do_put(flight_data_stream).await
        })
    }

    /// Flush the data in the StorageEngine to disk through the `do_action()` method.
    fn flush_data_to_disk(&mut self) {
        let action = Action {
            r#type: "FlushMemory".to_owned(),
            body: Bytes::new(),
        };

        self.runtime.block_on(async {
            self.client.do_action(Request::new(action)).await.unwrap();
        })
    }

    /// Execute a query against the server through the `do_get()` method and return it.
    fn execute_query(&mut self, query: String) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        self.runtime.block_on(async {
            // Execute query.
            let ticket = Ticket {
                ticket: query.into(),
            };
            let mut stream = self.client.do_get(ticket).await?.into_inner();

            // Get schema of result set.
            let flight_data = stream.message().await?.ok_or("No data_points received.")?;
            let schema = Arc::new(Schema::try_from(&flight_data)?);

            // Get data in result set.
            let mut results = vec![];
            while let Some(flight_data) = stream.message().await? {
                let dictionaries_by_id = HashMap::new();
                let record_batch = utils::flight_data_to_arrow_batch(
                    &flight_data,
                    schema.clone(),
                    &dictionaries_by_id,
                )?;
                results.push(record_batch);
            }
            Ok(results)
        })
    }

    /// Retrieve the table names currently in the server and return them.
    fn retrieve_all_table_names(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        let criteria = Criteria {
            expression: Bytes::new(),
        };
        let request = Request::new(criteria);

        self.runtime.block_on(async {
            let mut stream = self.client.list_flights(request).await?.into_inner();
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

    /// Retrieve the schema of a table in the server and return it.
    fn retrieve_schema(&mut self, table_name: &str) -> Schema {
        self.runtime.block_on(async {
            let schema_result = self
                .client
                .get_schema(Request::new(FlightDescriptor::new_path(vec![
                    table_name.to_owned()
                ])))
                .await
                .unwrap()
                .into_inner();

            convert::try_schema_from_ipc_buffer(&schema_result.schema).unwrap()
        })
    }
}

impl Drop for TestContext {
    /// Kill the server process when [`TestContext`] is dropped.
    fn drop(&mut self) {
        Self::kill_child(&mut self.server);
    }
}

#[test]
fn test_can_create_table() {
    let mut test_context = TestContext::new();

    test_context.create_table(TABLE_NAME, TableType::NormalTable);

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[test]
fn test_can_register_table() {
    let mut test_context = TestContext::new();

    test_context.create_table(TABLE_NAME, TableType::NormalTable);
    test_context.restart_server();

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[test]
fn test_can_create_model_table() {
    let mut test_context = TestContext::new();

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[test]
fn test_can_register_model_table() {
    let mut test_context = TestContext::new();

    test_context.create_table(TABLE_NAME, TableType::ModelTable);
    test_context.restart_server();

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[test]
fn test_can_create_register_and_list_multiple_tables_and_model_tables() {
    let mut test_context = TestContext::new();
    let table_types = &[
        TableType::NormalTable,
        TableType::ModelTable,
        TableType::ModelTableNoTag,
        TableType::ModelTableAsField,
    ];

    // Create number_of_each_to_create of each table type.
    let number_of_each_to_create = 5;
    let mut table_names = Vec::with_capacity(number_of_each_to_create * table_types.len());
    for table_type in table_types {
        // The table type name is included to simplify determining which type of table caused this
        // test to fail and to also make compilation fail if no table types are added to TableType.
        let table_type_name = match table_type {
            TableType::NormalTable => "normal_table",
            TableType::ModelTable => "model_table",
            TableType::ModelTableNoTag => "model_table_no_tag",
            TableType::ModelTableAsField => "model_table_as_field",
        };

        for table_number in 0..number_of_each_to_create {
            let table_name = table_type_name.to_owned() + &table_number.to_string();
            test_context.create_table(&table_name, TableType::NormalTable);
            table_names.push(table_name);
        }
    }

    // Sort the table names to simplify the comparisons.
    table_names.sort();

    // Ensure the table were created without a restart.
    let mut retrieved_table_names = test_context.retrieve_all_table_names().unwrap();
    retrieved_table_names.sort();
    assert_eq!(retrieved_table_names, table_names);

    // Ensure the table were registered after a restart.
    test_context.restart_server();
    let mut retrieved_table_names = test_context.retrieve_all_table_names().unwrap();
    retrieved_table_names.sort();
    assert_eq!(retrieved_table_names, table_names);
}

#[test]
fn test_can_get_schema() {
    let mut test_context = TestContext::new();

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    let schema = test_context.retrieve_schema(TABLE_NAME);

    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
            Field::new("value", DataType::Float32, false),
            Field::new("tag", DataType::Utf8, false)
        ])
    );
}

#[test]
fn test_can_list_actions() {
    let mut test_context = TestContext::new();

    let mut actions = test_context.runtime.block_on(async {
        test_context
            .client
            .list_actions(Request::new(arrow_flight::Empty {}))
            .await
            .unwrap()
            .into_inner()
            .map(|action| action.unwrap().r#type)
            .collect::<Vec<String>>()
            .await
    });

    // Sort() is called on the vector to ensure that the assertion will pass even if the order of
    // the actions returned by the endpoint changes.
    actions.sort();

    assert_eq!(
        actions,
        vec![
            "CollectMetrics",
            "CommandStatementUpdate",
            "FlushEdge",
            "FlushMemory",
            "UpdateRemoteObjectStore",
        ]
    );
}

#[test]
fn test_can_collect_metrics() {
    let mut test_context = TestContext::new();

    let metrics = test_context.runtime.block_on(async {
        test_context
            .client
            .do_action(Request::new(Action {
                r#type: "CollectMetrics".to_owned(),
                body: Bytes::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .map(|metric| metric.unwrap().body)
            .collect::<Vec<Bytes>>()
            .await
    });

    assert!(!metrics.is_empty());
}

#[test]
fn test_can_ingest_data_point_with_tags() {
    let mut test_context = TestContext::new();

    let data_point = TestContext::generate_random_data_point(Some("location"));
    let flight_data = TestContext::create_flight_data_from_data_points(
        TABLE_NAME.to_owned(),
        &[data_point.clone()],
    );

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    test_context
        .send_data_points_to_server(flight_data)
        .unwrap();

    test_context.flush_data_to_disk();

    let query = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .unwrap();

    assert_eq!(data_point, query[0]);
}

#[test]
fn test_can_ingest_data_point_without_tags() {
    let mut test_context = TestContext::new();

    let data_point = TestContext::generate_random_data_point(None);
    let flight_data = TestContext::create_flight_data_from_data_points(
        TABLE_NAME.to_owned(),
        &[data_point.clone()],
    );

    test_context.create_table(TABLE_NAME, TableType::ModelTableNoTag);

    test_context
        .send_data_points_to_server(flight_data)
        .unwrap();

    test_context.flush_data_to_disk();

    let query = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .unwrap();

    assert_eq!(data_point, query[0]);
}

#[test]
fn test_can_ingest_data_point_with_generated_field() {
    let mut test_context = TestContext::new();

    let data_point = TestContext::generate_random_data_point(None);
    let flight_data = TestContext::create_flight_data_from_data_points(
        TABLE_NAME.to_owned(),
        &[data_point.clone()],
    );

    test_context.create_table(TABLE_NAME, TableType::ModelTableAsField);

    test_context
        .send_data_points_to_server(flight_data)
        .unwrap();

    test_context.flush_data_to_disk();

    let query = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .unwrap();

    // Column two in the query is the generated column which does not exist in data point.
    assert_eq!(data_point.num_columns(), 2);
    assert_eq!(query[0].num_columns(), 3);
    assert_eq!(data_point.column(0), query[0].column(0));
    assert_eq!(data_point.column(1), query[0].column(2));
}

#[test]
fn test_can_ingest_multiple_time_series_with_different_tags() {
    let mut test_context = TestContext::new();

    let data_points: Vec<RecordBatch> = (1..5)
        .map(|i| TestContext::generate_random_data_point(Some(&format!("location{i}"))))
        .collect();
    let flight_data =
        TestContext::create_flight_data_from_data_points(TABLE_NAME.to_owned(), &data_points);

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    test_context
        .send_data_points_to_server(flight_data)
        .unwrap();

    test_context.flush_data_to_disk();

    let query = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME} ORDER BY timestamp"))
        .unwrap();

    let expected = compute::concat_batches(&data_points[0].schema(), &data_points).unwrap();
    assert_eq!(expected, query[0]);
}

#[test]
fn test_cannot_ingest_invalid_data_point() {
    let mut test_context = TestContext::new();

    let data_point = TestContext::generate_random_data_point(None);
    let flight_data =
        TestContext::create_flight_data_from_data_points(TABLE_NAME.to_owned(), &[data_point]);

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    assert!(test_context
        .send_data_points_to_server(flight_data)
        .is_err());

    test_context.flush_data_to_disk();

    let query = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .unwrap();
    assert!(query.is_empty());
}

#[test]
fn test_optimized_query_results_equals_non_optimized_query_results() {
    let mut test_context = TestContext::new();

    let mut data_points = vec![];
    for i in 1..5 {
        let batch = TestContext::generate_random_data_point(Some(&format!("location{i}")));

        data_points.push(batch);
    }
    let flight_data =
        TestContext::create_flight_data_from_data_points(TABLE_NAME.to_owned(), &data_points);

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    test_context
        .send_data_points_to_server(flight_data)
        .unwrap();

    test_context.flush_data_to_disk();

    let optimized_query = test_context
        .execute_query(format!("SELECT MIN(value) FROM {TABLE_NAME}"))
        .unwrap();

    // The trivial filter ensures the query is rewritten by the optimizer.
    let non_optimized_query = test_context
        .execute_query(format!("SELECT MIN(value) FROM {TABLE_NAME} WHERE 1=1"))
        .unwrap();

    assert_eq!(optimized_query, non_optimized_query);
}
