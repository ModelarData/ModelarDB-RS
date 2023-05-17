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
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::string::String;
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
use serial_test::serial;
use sysinfo::{Pid, PidExt, System, SystemExt};

use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

const TABLE_NAME: &str = "table_name";
const HOST: &str = "127.0.0.1";
const PORT: u16 = 9999;

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
    runtime: Runtime,
    server: Child,
    client: FlightServiceClient<Channel>,
}

impl TestContext {
    /// Create a [`Runtime`] and execute the binary with the server.
    fn new(path: &Path) -> Self {
        let runtime = Runtime::new().unwrap();

        // The server's stdout and stderr is piped to /dev/null so the log messages and expected
        // errors are not printed when all of the tests are run using the "cargo test" command.
        let server = TestContext::create_command("modelardbd")
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        // The thread needs to sleep to ensure that the server has properly started before sending
        // streams to it.
        thread::sleep(Duration::from_secs(5));

        let client =
            TestContext::create_apache_arrow_flight_service_client(&runtime, HOST, PORT).unwrap();

        TestContext {
            runtime,
            server,
            client,
        }
    }

    /// Return a [`Command`] that can run the executable `binary`.
    fn create_command(binary: &str) -> Command {
        // Create path to binary.
        let mut path = TestContext::binary_directory();
        path.push(binary);
        path.set_extension(consts::EXE_EXTENSION);

        assert!(path.exists());

        // Create command process.
        Command::new(path.into_os_string())
    }

    /// Return the path to the directory containing the binary with the integration tests.
    fn binary_directory() -> PathBuf {
        let current_executable = env::current_exe().unwrap();

        let parent_directory = current_executable.parent().unwrap();

        let binary_directory = parent_directory.parent().unwrap();

        binary_directory.to_owned()
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
                 generated FIELD AS CAST(COS(CAST(value AS DOUBLE) * PI() / 180.0) AS REAL),
                 value FIELD(0.0))"
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
        let mut system = System::new_all();

        while let Some(_process) = system.process(Pid::from_u32(self.server.id())) {
            system.refresh_all();

            // Microsoft Windows often fail to kill the process and then succeed afterwards.
            let mut attempts = 10;
            while self.server.kill().is_err() && attempts > 0 {
                thread::sleep(Duration::from_secs(5));
                attempts -= 1;
            }
            self.server.wait().unwrap();

            system.refresh_all();
        }
    }
}

#[test]
#[serial]
fn test_can_create_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

    test_context.create_table(TABLE_NAME, TableType::NormalTable);

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[test]
#[serial]
fn test_can_create_model_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

    test_context.create_table(TABLE_NAME, TableType::ModelTable);

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[test]
#[serial]
fn test_can_create_and_list_multiple_model_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

    let table_names = vec!["data1", "data2", "data3", "data4", "data5"];
    for table_name in &table_names {
        test_context.create_table(table_name, TableType::ModelTable);
    }

    let retrieved_table_names = test_context.retrieve_all_table_names().unwrap();

    assert_eq!(retrieved_table_names.len(), table_names.len());
    for table_name in table_names {
        assert!(retrieved_table_names.contains(&table_name.to_owned()))
    }
}

#[test]
#[serial]
fn test_can_get_schema() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_can_list_actions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_can_collect_metrics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_can_ingest_data_point_with_tags() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_can_ingest_data_point_without_tags() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_can_ingest_data_point_with_generated_field() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_can_ingest_multiple_time_series_with_different_tags() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_cannot_ingest_invalid_data_point() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
#[serial]
fn test_optimized_query_results_equals_non_optimized_query_results() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut test_context = TestContext::new(temp_dir.path());

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
