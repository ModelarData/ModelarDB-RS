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
use std::error::Error;
use std::io::Read;
use std::iter::repeat;
use std::ops::Range;
use std::process::{Child, Command, Stdio};
use std::str;
use std::string::String;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{utils, Action, Criteria, FlightData, FlightDescriptor, PutResult, Ticket};
use bytes::{Buf, Bytes};
use datafusion::arrow::array::{Array, Float64Array, StringArray, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit::Microsecond};
use datafusion::arrow::ipc::convert;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, StreamExt};
use modelardb_common::test::data_generation;
use modelardb_types::types::ErrorBound;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tempfile::TempDir;
use tokio::time;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

const TABLE_NAME: &str = "table_name";
const HOST: &str = "127.0.0.1";

/// The next port to be used for the server in an integration test. Each test uses a unique port and
/// local data folder, so they can run in parallel and so that any failing tests do not cascade.
/// Ports 9000 and 9001 are used by MinIO and Ports 10000, 10001, and 10002 are used by Azurite.
static PORT: AtomicU16 = AtomicU16::new(20_000);

/// Number of times to try to create the client and kill child processes.
const ATTEMPTS: u8 = 10;

/// Amount of time to sleep between each attempt to create the client and kill child processes.
const ATTEMPT_SLEEP_IN_SECONDS: Duration = Duration::from_secs(1);

/// Length of time series generated for integration tests.
const TIME_SERIES_TEST_LENGTH: usize = 5000;

/// Minimum length of each segment in a time series generated for integration tests. The maximum
/// length is `2 * SEGMENT_TEST_MINIMUM_LENGTH`.
const SEGMENT_TEST_MINIMUM_LENGTH: usize = 50;

/// The different types of tables used in the integration tests.
enum TableType {
    NormalTable,
    ModelTable,
    ModelTableNoTag,
    ModelTableAsField,
}

/// Handler to the server process and client for use by the tests. It implements `drop()` so the
/// resources it manages is released no matter if a test succeeds, fails, or panics.
struct TestContext {
    temp_dir: TempDir,
    port: u16,
    server: Child,
    client: FlightServiceClient<Channel>,
}

impl TestContext {
    /// Create a server that stores data in a randomly generated local data folder and listens on
    /// `PORT` and a client and ensure they are ready to be used in the tests.
    async fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = PORT.fetch_add(1, Ordering::Relaxed);
        let mut server = Self::create_server(&temp_dir, port);
        let client = Self::create_client(&mut server, port).await;

        Self {
            temp_dir,
            port,
            server,
            client,
        }
    }

    /// Restart the server and reconnect the client.
    async fn restart_server(&mut self) {
        Self::kill_child(&mut self.server);
        self.server = Self::create_server(&self.temp_dir, self.port);
        self.client = Self::create_client(&mut self.server, self.port).await;
    }

    /// Create a server that stores data in `local_data_folder` and listens on `port` and ensure it
    /// is ready to receive requests.
    fn create_server(local_data_folder: &TempDir, port: u16) -> Child {
        // The server's stdout and stderr are piped so the log messages (stdout) and expected errors
        // (stderr) are not printed when all the tests are run using the "cargo test" command.
        // modelardbd is run using dev-release so the tests can use larger more realistic data sets.
        let local_data_folder = local_data_folder.path().to_str().unwrap();
        // TODO: Maybe replace with tokio::process::Command.
        let mut server = Command::new("cargo")
            .env("MODELARDBD_PORT", port.to_string())
            .args([
                "run",
                "--profile",
                "dev-release",
                "--bin",
                "modelardbd",
                local_data_folder,
            ])
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

    /// Create the client and ensure it can connect to the server.
    async fn create_client(server: &mut Child, port: u16) -> FlightServiceClient<Channel> {
        // Despite waiting for the expected output, the client may not able to connect the first
        // time. This rarely happens on a local machine but happens more often in GitHub Actions.
        let mut attempts = ATTEMPTS;
        loop {
            if let Ok(client) = Self::create_apache_arrow_flight_service_client(HOST, port).await {
                return client;
            } else if attempts == 0 {
                Self::kill_child(server);
                panic!("The Apache Arrow Flight client could not connect to modelardbd.");
            } else {
                time::sleep(ATTEMPT_SLEEP_IN_SECONDS).await;
                attempts -= 1;
            }
        }
    }

    /// Kill a `child` process.
    fn kill_child(child: &mut Child) {
        let mut system = System::new_all();

        let pid = Pid::from_u32(child.id());
        let pids = &[pid];
        let processes_to_update = ProcessesToUpdate::Some(pids);

        while let Some(_process) = system.process(pid) {
            // Microsoft Windows often fails to kill the process and then succeed afterward.
            let mut attempts = ATTEMPTS;
            while child.kill().is_err() && attempts > 0 {
                thread::sleep(ATTEMPT_SLEEP_IN_SECONDS);
                attempts -= 1;
            }
            child.wait().unwrap();

            // Ensure the process is removed if it is killed before checking the condition.
            system.refresh_processes(processes_to_update, true);
        }
    }

    /// Return an Apache Arrow Flight client to access the remote methods provided by the server.
    async fn create_apache_arrow_flight_service_client(
        host: &str,
        port: u16,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
        let address = format!("grpc://{host}:{port}");

        let client = FlightServiceClient::connect(address).await?;
        Ok(client)
    }

    /// Create a normal table or model table with or without tags in the server through the
    /// `do_action()` method and the `CreateTable` action.
    async fn create_table(&mut self, table_name: &str, table_type: TableType) {
        let cmd = match table_type {
            TableType::NormalTable => {
                format!(
                    "CREATE TABLE {table_name}(
                         timestamp TIMESTAMP,
                         field_one REAL,
                         field_two REAL,
                         field_three REAL,
                         field_four REAL,
                         field_five REAL,
                         metadata TEXT
                     )"
                )
            }
            TableType::ModelTable => {
                format!(
                    "CREATE MODEL TABLE {table_name}(
                         timestamp TIMESTAMP,
                         field_one FIELD,
                         field_two FIELD,
                         field_three FIELD,
                         field_four FIELD,
                         field_five FIELD,
                         tag TAG
                     )"
                )
            }
            TableType::ModelTableNoTag => {
                format!(
                    "CREATE MODEL TABLE {table_name}(
                         timestamp TIMESTAMP,
                         field_one FIELD,
                         field_two FIELD,
                         field_three FIELD,
                         field_four FIELD,
                         field_five FIELD
                     )"
                )
            }
            TableType::ModelTableAsField => {
                format!(
                    "CREATE MODEL TABLE {table_name}(
                         timestamp TIMESTAMP,
                         generated FIELD AS (field_one + CAST(37.0 AS REAL)),
                         field_one FIELD,
                         field_two FIELD,
                         field_three FIELD
                     )"
                )
            }
        };

        let ticket = Ticket { ticket: cmd.into() };

        self.client.do_get(ticket).await.unwrap();
    }

    /// Drop a table in the server through the `do_get()` method.
    async fn drop_table(
        &mut self,
        table_name: &str,
    ) -> Result<Response<Streaming<FlightData>>, Status> {
        let ticket = Ticket::new(format!("DROP TABLE {table_name}"));
        self.client.do_get(ticket).await
    }

    /// Truncate a table in the server through the `do_get()` method.
    async fn truncate_table(
        &mut self,
        table_name: &str,
    ) -> Result<Response<Streaming<FlightData>>, Status> {
        let ticket = Ticket::new(format!("TRUNCATE TABLE {table_name}"));
        self.client.do_get(ticket).await
    }

    /// Return a [`RecordBatch`] containing a time series with regular or irregular time stamps
    /// depending on `generate_irregular_timestamps`, generated values with noise depending on
    /// `multiply_noise_range`, and an optional tag.
    fn generate_time_series_with_tag(
        generate_irregular_timestamps: bool,
        multiply_noise_range: Option<Range<f32>>,
        maybe_tag: Option<&str>,
    ) -> RecordBatch {
        let (uncompressed_timestamps, mut uncompressed_values) =
            data_generation::generate_multivariate_time_series(
                TIME_SERIES_TEST_LENGTH,
                5,
                SEGMENT_TEST_MINIMUM_LENGTH..2 * SEGMENT_TEST_MINIMUM_LENGTH + 1,
                generate_irregular_timestamps,
                multiply_noise_range,
                100.0..200.0,
            );

        let time_series_len = uncompressed_timestamps.len();

        let mut fields = vec![
            Field::new("timestamp", DataType::Timestamp(Microsecond, None), false),
            Field::new("field_one", DataType::Float32, false),
            Field::new("field_two", DataType::Float32, false),
            Field::new("field_three", DataType::Float32, false),
            Field::new("field_four", DataType::Float32, false),
            Field::new("field_five", DataType::Float32, false),
        ];

        // 0 is used as the index for each call to remove() as a value is removed each time.
        let mut columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(uncompressed_timestamps),
            Arc::new(uncompressed_values.remove(0)),
            Arc::new(uncompressed_values.remove(0)),
            Arc::new(uncompressed_values.remove(0)),
            Arc::new(uncompressed_values.remove(0)),
            Arc::new(uncompressed_values.remove(0)),
        ];

        if let Some(tag) = maybe_tag {
            fields.push(Field::new("tag", DataType::Utf8, false));
            columns.push(Arc::new(StringArray::from_iter_values(
                repeat(tag).take(time_series_len),
            )));
        }

        let schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// Create and return [`FlightData`] based on the `time_series` to be inserted into `table_name`.
    fn create_flight_data_from_time_series(
        table_name: String,
        time_series: &[RecordBatch],
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

        for data_point in time_series {
            let (_encoded_dictionaries, encoded_batch) = data_generator
                .encoded_batch(data_point, &mut dictionary_tracker, &writer_options)
                .unwrap();
            flight_data.push(encoded_batch.into());
        }

        flight_data
    }

    /// Send data points to the server through the `do_put()` method.
    async fn send_time_series_to_server(
        &mut self,
        flight_data: Vec<FlightData>,
    ) -> Result<Response<Streaming<PutResult>>, Status> {
        let flight_data_stream = stream::iter(flight_data);
        self.client.do_put(flight_data_stream).await
    }

    /// Flush the data in the StorageEngine to disk through the `do_action()` method.
    async fn flush_data_to_disk(&mut self) {
        let action = Action {
            r#type: "FlushMemory".to_owned(),
            body: Bytes::new(),
        };

        self.client.do_action(Request::new(action)).await.unwrap();
    }

    /// Execute a query against the server through the `do_get()` method and return it.
    async fn execute_query(&mut self, query: String) -> Result<RecordBatch, Box<dyn Error>> {
        // Execute query.
        let ticket = Ticket {
            ticket: query.into(),
        };
        let mut stream = self.client.do_get(ticket).await?.into_inner();

        // Get schema of result set.
        let flight_data = stream.message().await?.ok_or("No time series received.")?;
        let schema = Arc::new(Schema::try_from(&flight_data)?);

        // Get data in result set.
        let mut query_result = vec![];
        while let Some(flight_data) = stream.message().await? {
            let dictionaries_by_id = HashMap::new();
            let record_batch = utils::flight_data_to_arrow_batch(
                &flight_data,
                schema.clone(),
                &dictionaries_by_id,
            )?;
            query_result.push(record_batch);
        }

        if query_result.is_empty() {
            Ok(RecordBatch::new_empty(schema))
        } else {
            // unwrap() is used as ? makes the return type Result<RecordBatch, ArrowError>>.
            Ok(compute::concat_batches(&query_result[0].schema(), &query_result).unwrap())
        }
    }

    /// Retrieve the table names currently in the server and return them.
    async fn retrieve_all_table_names(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        let criteria = Criteria {
            expression: Bytes::new(),
        };
        let request = Request::new(criteria);

        let mut stream = self.client.list_flights(request).await?.into_inner();
        let flights = stream.message().await?.ok_or("No time series received.")?;

        let mut table_names = vec![];
        if let Some(fd) = flights.flight_descriptor {
            for table in fd.path {
                table_names.push(table);
            }
        }
        Ok(table_names)
    }

    /// Retrieve the schema of a table in the server and return it.
    async fn retrieve_schema(&mut self, table_name: &str) -> Schema {
        let schema_result = self
            .client
            .get_schema(Request::new(FlightDescriptor::new_path(vec![
                table_name.to_owned(),
            ])))
            .await
            .unwrap()
            .into_inner();

        convert::try_schema_from_ipc_buffer(&schema_result.schema).unwrap()
    }

    /// Update `setting` to `setting_value` in the server configuration using the
    /// `UpdateConfiguration` action.
    async fn update_configuration(
        &mut self,
        setting: &str,
        setting_value: &str,
    ) -> Result<Response<Streaming<arrow_flight::Result>>, Status> {
        let setting = setting.as_bytes();
        let setting_size = &[0, setting.len() as u8];

        let setting_value = setting_value.as_bytes();
        let setting_value_size = &[0, setting_value.len() as u8];

        let action = Action {
            r#type: "UpdateConfiguration".to_owned(),
            body: [setting_size, setting, setting_value_size, setting_value]
                .concat()
                .into(),
        };

        self.client.do_action(Request::new(action)).await
    }

    /// Retrieve the response of the [`Action`] with the type `action_type` and convert it into an
    /// Apache Arrow [`RecordBatch`].
    async fn retrieve_action_record_batch(&mut self, action_type: &str) -> RecordBatch {
        let response_bytes = self.retrieve_action_bytes(action_type).await;

        // Convert the bytes in the response into an Apache Arrow record batch.
        let mut reader = StreamReader::try_new(response_bytes.reader(), None).unwrap();
        reader.next().unwrap().unwrap()
    }

    /// Retrieve the response of the [`Action`] with the type `action_type`.
    async fn retrieve_action_bytes(&mut self, action_type: &str) -> Bytes {
        let action = Action {
            r#type: action_type.to_owned(),
            body: Bytes::new(),
        };

        // Retrieve the bytes from the action response.
        let response = self
            .client
            .do_action(Request::new(action))
            .await
            .unwrap()
            .into_inner()
            .message()
            .await
            .unwrap()
            .unwrap();

        response.body
    }
}

impl Drop for TestContext {
    /// Kill the server process when [`TestContext`] is dropped.
    fn drop(&mut self) {
        Self::kill_child(&mut self.server);
    }
}

#[tokio::test]
async fn test_can_create_normal_table() {
    let mut test_context = TestContext::new().await;

    test_context
        .create_table(TABLE_NAME, TableType::NormalTable)
        .await;

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[tokio::test]
async fn test_can_register_normal_table_after_restart() {
    let mut test_context = TestContext::new().await;

    test_context
        .create_table(TABLE_NAME, TableType::NormalTable)
        .await;

    test_context.restart_server().await;

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[tokio::test]
async fn test_can_create_model_table() {
    let mut test_context = TestContext::new().await;

    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[tokio::test]
async fn test_can_register_model_table_after_restart() {
    let mut test_context = TestContext::new().await;

    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;

    test_context.restart_server().await;

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();

    assert_eq!(retrieved_table_names.len(), 1);
    assert_eq!(retrieved_table_names[0], TABLE_NAME);
}

#[tokio::test]
async fn test_can_create_register_and_list_multiple_normal_tables_and_model_tables() {
    let mut test_context = TestContext::new().await;
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
            test_context
                .create_table(&table_name, TableType::NormalTable)
                .await;

            table_names.push(table_name);
        }
    }

    // Sort the table names to simplify the comparisons.
    table_names.sort();

    // Ensure the tables were created without a restart.
    let mut retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    retrieved_table_names.sort();
    assert_eq!(retrieved_table_names, table_names);

    // Ensure the tables were registered after a restart.
    test_context.restart_server().await;
    let mut retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    retrieved_table_names.sort();
    assert_eq!(retrieved_table_names, table_names);
}

#[tokio::test]
async fn test_can_drop_normal_table() {
    let mut test_context = TestContext::new().await;
    test_context
        .create_table(TABLE_NAME, TableType::NormalTable)
        .await;

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    assert_eq!(retrieved_table_names[0], TABLE_NAME);

    test_context.drop_table(TABLE_NAME).await.unwrap();

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    assert_eq!(retrieved_table_names.len(), 0);

    // It should be possible to create a normal table, drop it, and then create a new normal table
    // with the same name.
    test_context
        .create_table(TABLE_NAME, TableType::NormalTable)
        .await;
}

#[tokio::test]
async fn test_can_drop_model_table() {
    let mut test_context = TestContext::new().await;
    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    assert_eq!(retrieved_table_names[0], TABLE_NAME);

    test_context.drop_table(TABLE_NAME).await.unwrap();

    let retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    assert_eq!(retrieved_table_names.len(), 0);

    // It should be possible to create a model table, drop it, and then create a new model table with
    // the same name.
    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;
}

#[tokio::test]
async fn test_cannot_drop_missing_table() {
    let mut test_context = TestContext::new().await;

    let result = test_context.drop_table(TABLE_NAME).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_can_truncate_normal_table() {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, Some("location"));

    ingest_time_series_and_flush_data(
        &mut test_context,
        &[time_series.clone()],
        TableType::NormalTable,
    )
    .await;

    test_context.truncate_table(TABLE_NAME).await.unwrap();

    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    // The normal table should be empty after truncating it.
    assert_eq!(query_result.num_rows(), 0);
}

#[tokio::test]
async fn test_can_truncate_model_table() {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, Some("location"));

    ingest_time_series_and_flush_data(
        &mut test_context,
        &[time_series.clone()],
        TableType::ModelTable,
    )
    .await;

    test_context.truncate_table(TABLE_NAME).await.unwrap();

    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    // The model table should be empty after truncating it.
    assert_eq!(query_result.num_rows(), 0);
}

#[tokio::test]
async fn test_cannot_truncate_missing_table() {
    let mut test_context = TestContext::new().await;

    let result = test_context.truncate_table(TABLE_NAME).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_can_get_schema() {
    let mut test_context = TestContext::new().await;

    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;

    let schema = test_context.retrieve_schema(TABLE_NAME).await;

    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(Microsecond, None), false),
            Field::new("field_one", DataType::Float32, false),
            Field::new("field_two", DataType::Float32, false),
            Field::new("field_three", DataType::Float32, false),
            Field::new("field_four", DataType::Float32, false),
            Field::new("field_five", DataType::Float32, false),
            Field::new("tag", DataType::Utf8, false)
        ])
    );
}

#[tokio::test]
async fn test_can_list_actions() {
    let mut test_context = TestContext::new().await;

    let mut actions = test_context
        .client
        .list_actions(Request::new(arrow_flight::Empty {}))
        .await
        .unwrap()
        .into_inner()
        .map(|action| action.unwrap().r#type)
        .collect::<Vec<String>>()
        .await;

    // Sort() is called on the vector to ensure that the assertion will pass even if the order of
    // the actions returned by the endpoint changes.
    actions.sort();

    assert_eq!(
        actions,
        vec![
            "CreateTables",
            "FlushMemory",
            "FlushNode",
            "GetConfiguration",
            "KillNode",
            "NodeType",
            "UpdateConfiguration",
        ]
    );
}

#[tokio::test]
async fn test_do_put_can_ingest_time_series_with_tags() {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, Some("location"));

    ingest_time_series_and_flush_data(
        &mut test_context,
        &[time_series.clone()],
        TableType::ModelTable,
    )
    .await;

    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    assert_eq!(time_series, query_result);
}

#[tokio::test]
async fn test_insert_can_ingest_time_series_with_tags() {
    let mut test_context = TestContext::new().await;
    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;

    let insert_result = test_context
        .execute_query(format!(
            "INSERT INTO {TABLE_NAME} VALUES\
             ('2020-01-01 13:00:00', 1, 2, 3, 4, 5, 'Aalborg'),\
             ('2020-01-01 13:00:01', 1, 2, 3, 4, 5, 'Aalborg'),\
             ('2020-01-01 13:00:02', 1, 2, 3, 4, 5, 'Aalborg'),\
             ('2020-01-01 13:00:03', 1, 2, 3, 4, 5, 'Aalborg'),\
             ('2020-01-01 13:00:04', 1, 2, 3, 4, 5, 'Aalborg')"
        ))
        .await
        .unwrap();
    let insert_result_count = modelardb_types::array!(insert_result, 0, UInt64Array).value(0);

    test_context.flush_data_to_disk().await;
    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    assert_eq!(insert_result.num_rows(), 1);
    assert_eq!(insert_result_count, 5);
    assert_eq!(query_result.num_rows(), 5);
}

#[tokio::test]
async fn test_do_put_can_ingest_time_series_without_tags() {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, None);

    ingest_time_series_and_flush_data(
        &mut test_context,
        &[time_series.clone()],
        TableType::ModelTableNoTag,
    )
    .await;

    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    assert_eq!(time_series, query_result);
}

#[tokio::test]
async fn test_insert_can_ingest_time_series_without_tags() {
    let mut test_context = TestContext::new().await;
    test_context
        .create_table(TABLE_NAME, TableType::ModelTableNoTag)
        .await;

    let insert_result = test_context
        .execute_query(format!(
            "INSERT INTO {TABLE_NAME} VALUES\
             ('2020-01-01 13:00:00', 1, 2, 3, 4, 5),\
             ('2020-01-01 13:00:01', 1, 2, 3, 4, 5),\
             ('2020-01-01 13:00:02', 1, 2, 3, 4, 5),\
             ('2020-01-01 13:00:03', 1, 2, 3, 4, 5),\
             ('2020-01-01 13:00:04', 1, 2, 3, 4, 5)"
        ))
        .await
        .unwrap();
    let insert_result_count = modelardb_types::array!(insert_result, 0, UInt64Array).value(0);

    test_context.flush_data_to_disk().await;
    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    assert_eq!(insert_result.num_rows(), 1);
    assert_eq!(insert_result_count, 5);
    assert_eq!(query_result.num_rows(), 5);
}

#[tokio::test]
async fn test_do_put_can_ingest_time_series_with_generated_field() {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, None);

    ingest_time_series_and_flush_data(
        &mut test_context,
        &[time_series.clone()],
        TableType::ModelTableAsField,
    )
    .await;

    // The optimizer is allowed to add SortedJoinExec between SortedJoinExec and GeneratedAsExec.
    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME} ORDER BY timestamp"))
        .await
        .unwrap();

    // Column two in the query is the generated column which does not exist in data point.
    assert_eq!(time_series.num_columns(), 6);
    assert_eq!(query_result.num_columns(), 5);
    assert_eq!(time_series.column(0), query_result.column(0));
    assert_eq!(time_series.column(1), query_result.column(2));
}

#[tokio::test]
async fn test_insert_can_ingest_time_series_with_generated_field() {
    let mut test_context = TestContext::new().await;
    test_context
        .create_table(TABLE_NAME, TableType::ModelTableAsField)
        .await;

    let insert_result = test_context
        .execute_query(format!(
            "INSERT INTO {TABLE_NAME} VALUES\
             ('2020-01-01 13:00:00', 1, 2, 3, 4),\
             ('2020-01-01 13:00:01', 1, 2, 3, 4),\
             ('2020-01-01 13:00:02', 1, 2, 3, 4),\
             ('2020-01-01 13:00:03', 1, 2, 3, 4),\
             ('2020-01-01 13:00:04', 1, 2, 3, 4)"
        ))
        .await
        .unwrap();
    let insert_result_count = modelardb_types::array!(insert_result, 0, UInt64Array).value(0);

    test_context.flush_data_to_disk().await;
    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    assert_eq!(insert_result.num_rows(), 1);
    assert_eq!(insert_result_count, 5);
    assert_eq!(query_result.num_rows(), 5);
}

#[tokio::test]
async fn test_do_put_can_ingest_multiple_time_series_with_different_tags() {
    let mut test_context = TestContext::new().await;

    let time_series_with_tag_one: RecordBatch =
        TestContext::generate_time_series_with_tag(false, None, Some("tag_one"));
    let time_series_with_tag_two: RecordBatch =
        TestContext::generate_time_series_with_tag(false, None, Some("tag_two"));
    let time_series = &[time_series_with_tag_one, time_series_with_tag_two];

    ingest_time_series_and_flush_data(&mut test_context, time_series, TableType::ModelTable).await;

    let query_result = test_context
        .execute_query(format!(
            "SELECT * FROM {TABLE_NAME} ORDER BY tag, timestamp"
        ))
        .await
        .unwrap();

    let expected = compute::concat_batches(&time_series[0].schema(), time_series).unwrap();

    // More specific asserts are included before checking the results sets to simplify debugging.
    assert_eq!(expected.num_columns(), 7);
    assert_eq!(expected.num_columns(), query_result.num_columns());
    assert_eq!(expected.num_columns(), query_result.num_columns());
    assert_eq!(expected.num_rows(), query_result.num_rows());
    assert_eq!(expected, query_result);
}

#[tokio::test]
async fn test_cannot_ingest_invalid_time_series() {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, None);
    let flight_data =
        TestContext::create_flight_data_from_time_series(TABLE_NAME.to_owned(), &[time_series]);

    test_context
        .create_table(TABLE_NAME, TableType::ModelTable)
        .await;

    assert!(
        test_context
            .send_time_series_to_server(flight_data)
            .await
            .is_err()
    );

    test_context.flush_data_to_disk().await;

    let query_result = test_context
        .execute_query(format!("SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();
    assert_eq!(query_result.num_rows(), 0);
}

#[tokio::test]
async fn test_do_get_can_execute_include_address_select_query() {
    execute_and_assert_include_select(1).await;
}

#[tokio::test]
async fn test_do_get_can_execute_include_address_address_select_query() {
    execute_and_assert_include_select(2).await;
}

async fn execute_and_assert_include_select(address_count: usize) {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, Some("location"));

    let expected_record_batches: Vec<_> = (0..address_count + 1).map(|_| &time_series).collect();
    let expected_time_series =
        compute::concat_batches(&time_series.schema(), expected_record_batches).unwrap();

    ingest_time_series_and_flush_data(&mut test_context, &[time_series], TableType::ModelTable)
        .await;

    let port = test_context.port;
    let address = format!("'grpc://{HOST}:{port}'");
    let addresses_separate: Vec<_> = (0..address_count).map(|_| address.clone()).collect();
    let address = addresses_separate.join(", ");

    let query_result = test_context
        .execute_query(format!("INCLUDE {address} SELECT * FROM {TABLE_NAME}"))
        .await
        .unwrap();

    assert_eq!(expected_time_series, query_result);
}

#[tokio::test]
async fn test_count_from_segments_equals_count_from_data_points() {
    assert_ne_query_plans_and_eq_result(format!("SELECT COUNT(field_one) FROM {TABLE_NAME}"), 0.0).await;
}

#[tokio::test]
async fn test_min_from_segments_equals_min_from_data_points() {
    assert_ne_query_plans_and_eq_result(format!("SELECT MIN(field_one) FROM {TABLE_NAME}"), 0.0).await;
}

#[tokio::test]
async fn test_max_from_segments_equals_max_from_data_points() {
    assert_ne_query_plans_and_eq_result(format!("SELECT MAX(field_one) FROM {TABLE_NAME}"), 0.0).await;
}

#[tokio::test]
async fn test_sum_from_segments_equals_sum_from_data_points() {
    assert_ne_query_plans_and_eq_result(format!("SELECT SUM(field_one) FROM {TABLE_NAME}"), 0.001).await;
}

#[tokio::test]
async fn test_avg_from_segments_equals_avg_from_data_points() {
    assert_ne_query_plans_and_eq_result(format!("SELECT AVG(field_one) FROM {TABLE_NAME}"), 0.001).await;
}

/// Asserts that the query executed on segments in `segment_query` returns a result within
/// `error_bound` of an equivalent query executed on data points reconstructed from the segments by:
/// 1. Generating a multivariate time series with one tag and ingesting it into a model table.
/// 2. Creating an equivalent query from `segment_query` that the optimizer cannot rewrite, so it is
///    executed on segments.
/// 3. Executing `segment_query` and the new equivalent query against the model table containing the
///    generated time series.
/// 4. Comparing the query plans of `segment_query` and the new equivalent query to ensure the first
///    query was rewritten by the optimizer and the other query was not.
/// 5. Comparing the results of `segment_query` and the new equivalent query to ensure they are
///    within `error_bound`.
async fn assert_ne_query_plans_and_eq_result(segment_query: String, error_bound: f32) {
    let mut test_context = TestContext::new().await;
    let time_series = TestContext::generate_time_series_with_tag(false, None, Some("tag"));

    ingest_time_series_and_flush_data(&mut test_context, &[time_series], TableType::ModelTable)
        .await;

    // The predicate will guarantee that all data points will be included in the query but will
    // prevent the optimizer from rewriting the query due to its presence in segment_query.
    let data_point_query = format!("{segment_query} WHERE timestamp >= 0::TIMESTAMP");

    let data_point_query_plans = test_context
        .execute_query(format!("EXPLAIN {}", data_point_query))
        .await
        .unwrap();
    let segment_query_plans = test_context
        .execute_query(format!("EXPLAIN {}", segment_query))
        .await
        .unwrap();

    let data_point_query_plans_text =
        modelardb_types::array!(data_point_query_plans, 1, StringArray);
    let segment_query_plans_text = modelardb_types::array!(segment_query_plans, 1, StringArray);
    assert_ne!(data_point_query_plans, segment_query_plans);
    assert!(data_point_query_plans_text.value(1).contains("GridExec"));
    assert!(!segment_query_plans_text.value(1).contains("GridExec"));

    let data_point_query_result_set = test_context.execute_query(data_point_query).await.unwrap();
    let segment_query_result_set = test_context.execute_query(segment_query).await.unwrap();

    if error_bound == 0.0 {
        assert_eq!(data_point_query_result_set, segment_query_result_set);
    } else {
        assert_eq!(data_point_query_result_set.num_columns(), 1);
        assert_eq!(data_point_query_result_set.num_rows(), 1);
        assert_eq!(segment_query_result_set.num_columns(), 1);
        assert_eq!(segment_query_result_set.num_rows(), 1);

        let data_point_query_result =
            modelardb_types::array!(data_point_query_result_set, 0, Float64Array);
        let segment_query_result =
            modelardb_types::array!(segment_query_result_set, 0, Float64Array);

        let within_error_bound = modelardb_compression::is_value_within_error_bound(
            ErrorBound::try_new_relative(error_bound).unwrap(),
            data_point_query_result.value(0) as f32,
            segment_query_result.value(0) as f32,
        );

        assert!(
            within_error_bound,
            "{} is not within {}% of {}.",
            segment_query_result.value(0),
            error_bound,
            data_point_query_result.value(0)
        );
    }
}

/// Creates a table of type `table_type`, ingests `time_series`, and then flushes that data to disk.
async fn ingest_time_series_and_flush_data(
    test_context: &mut TestContext,
    time_series: &[RecordBatch],
    table_type: TableType,
) {
    let flight_data =
        TestContext::create_flight_data_from_time_series(TABLE_NAME.to_owned(), time_series);

    test_context.create_table(TABLE_NAME, table_type).await;

    test_context
        .send_time_series_to_server(flight_data)
        .await
        .unwrap();

    test_context.flush_data_to_disk().await;
}

#[tokio::test]
async fn test_can_get_configuration() {
    let mut test_context = TestContext::new().await;
    let configuration = test_context
        .retrieve_action_record_batch("GetConfiguration")
        .await;

    let settings = modelardb_types::array!(configuration, 0, StringArray);
    let values = modelardb_types::array!(configuration, 1, UInt64Array);

    assert_eq!(settings.value(0), "uncompressed_reserved_memory_in_bytes");
    assert_eq!(values.value(0), 512 * 1024 * 1024);

    assert_eq!(settings.value(1), "compressed_reserved_memory_in_bytes");
    assert_eq!(values.value(1), 512 * 1024 * 1024);

    assert_eq!(settings.value(2), "transfer_batch_size_in_bytes");
    assert_eq!(values.value(2), 64 * 1024 * 1024);

    assert_eq!(settings.value(3), "transfer_time_in_seconds");
    assert_eq!(values.value(3), 0);
    assert_eq!(values.null_count(), 1);

    assert_eq!(settings.value(4), "ingestion_threads");
    assert_eq!(values.value(4), 1);

    assert_eq!(settings.value(5), "compression_threads");
    assert_eq!(values.value(5), 1);

    assert_eq!(settings.value(6), "writer_threads");
    assert_eq!(values.value(6), 1);
}

#[tokio::test]
async fn test_can_update_uncompressed_reserved_memory_in_bytes() {
    let values_array =
        update_and_retrieve_configuration_values("uncompressed_reserved_memory_in_bytes").await;

    assert_eq!(values_array.value(0), 1);
}

#[tokio::test]
async fn test_can_update_compressed_reserved_memory_in_bytes() {
    let values_array =
        update_and_retrieve_configuration_values("compressed_reserved_memory_in_bytes").await;

    assert_eq!(values_array.value(1), 1);
}

async fn update_and_retrieve_configuration_values(setting: &str) -> UInt64Array {
    let mut test_context = TestContext::new().await;
    test_context
        .update_configuration(setting, "1")
        .await
        .unwrap();

    let configuration = test_context
        .retrieve_action_record_batch("GetConfiguration")
        .await;

    modelardb_types::array!(configuration, 1, UInt64Array).clone()
}

#[tokio::test]
async fn test_cannot_update_transfer_batch_size_in_bytes() {
    // It is only possible to test that this fails since we cannot start the server with a
    // remote data folder.
    update_configuration_and_assert_error(
        "transfer_batch_size_in_bytes",
        "1",
        "Invalid State Error: Storage engine is not configured to transfer data.",
    )
    .await;
}

#[tokio::test]
async fn test_cannot_update_transfer_time_in_seconds() {
    // It is only possible to test that this fails since we cannot start the server with a
    // remote data folder.
    update_configuration_and_assert_error(
        "transfer_time_in_seconds",
        "1",
        "Invalid State Error: Storage engine is not configured to transfer data.",
    )
    .await;
}

#[tokio::test]
async fn test_cannot_update_non_existing_setting() {
    update_configuration_and_assert_error(
        "invalid",
        "1",
        "invalid is not a setting in the server configuration.",
    )
    .await;
}

#[tokio::test]
async fn test_cannot_update_non_nullable_setting_with_empty_value() {
    for setting in [
        "uncompressed_reserved_memory_in_bytes",
        "compressed_reserved_memory_in_bytes",
    ] {
        update_configuration_and_assert_error(
            setting,
            "",
            format!("New value for {setting} cannot be empty.").as_str(),
        )
        .await;
    }
}

#[tokio::test]
async fn test_cannot_update_non_updatable_setting() {
    for setting in ["ingestion_threads", "compression_threads", "writer_threads"] {
        update_configuration_and_assert_error(
            setting,
            "1",
            format!("{setting} is not an updatable setting in the server configuration.").as_str(),
        )
        .await;
    }
}

#[tokio::test]
async fn test_cannot_update_setting_with_invalid_value() {
    update_configuration_and_assert_error(
        "compressed_reserved_memory_in_bytes",
        "-1",
        "New value for compressed_reserved_memory_in_bytes is not valid: invalid digit found in string",
    ).await;
}

async fn update_configuration_and_assert_error(setting: &str, setting_value: &str, error: &str) {
    let mut test_context = TestContext::new().await;
    let response = test_context
        .update_configuration(setting, setting_value)
        .await;

    assert!(response.is_err());
    assert_eq!(response.err().unwrap().message(), error);
}

#[tokio::test]
async fn test_can_get_node_type() {
    let mut test_context = TestContext::new().await;
    let response_bytes = test_context.retrieve_action_bytes("NodeType").await;

    assert_eq!(str::from_utf8(&response_bytes).unwrap(), "server");
}

#[tokio::test]
async fn test_can_create_tables() {
    let mut test_context = TestContext::new().await;

    let table_record_batch = modelardb_storage::test::table_metadata_record_batch();
    let table_record_batch_bytes =
        modelardb_storage::try_convert_record_batch_to_bytes(&table_record_batch).unwrap();

    let action = Action {
        r#type: "CreateTables".to_owned(),
        body: table_record_batch_bytes.into(),
    };

    test_context
        .client
        .do_action(Request::new(action))
        .await
        .unwrap();

    let mut retrieved_table_names = test_context.retrieve_all_table_names().await.unwrap();
    retrieved_table_names.sort();
    assert_eq!(
        retrieved_table_names,
        vec![
            modelardb_storage::test::MODEL_TABLE_NAME.to_owned(),
            modelardb_storage::test::NORMAL_TABLE_NAME.to_owned(),
        ]
    );
}

#[tokio::test]
async fn test_cannot_create_tables_with_invalid_record_batch() {
    let mut test_context = TestContext::new().await;

    let invalid_record_batch = modelardb_storage::test::normal_table_record_batch();
    let invalid_record_batch_bytes =
        modelardb_storage::try_convert_record_batch_to_bytes(&invalid_record_batch).unwrap();

    let action = Action {
        r#type: "CreateTables".to_owned(),
        body: invalid_record_batch_bytes.into(),
    };

    let response = test_context.client.do_action(Request::new(action)).await;
    assert!(response.is_err());
}
