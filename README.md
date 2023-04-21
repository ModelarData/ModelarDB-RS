# ModelarDB
:warning: **The current version of ModelarDB is alpha software and not yet ready for production use.**

[![Cargo Build, Lint, and Test](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/cargo-build-lint-and-test-on-pr-and-push.yml/badge.svg)](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/cargo-build-lint-and-test-on-pr-and-push.yml)
[![Python unittest](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/python-unittest-on-pr-and-push.yml/badge.svg)](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/python-unittest-on-pr-and-push.yml)

ModelarDB is an efficient high-performance time series management system that is designed to efficiently ingest, transfer, store, and analyze high-frequency time series across the edge and cloud. It provides state-of-the-art lossless compression, lossy compression, and query performance by efficiently compressing time series on the edge using multiple different types of models such as constant and linear functions. As a result, the high-frequency time series can be transferred to the cloud through a connection with very limited bandwidth and stored in the cloud at a low cost. The compressed time series can be efficiently queried on both the edge and in the cloud using a relational interface and SQL without any knowledge about the model-based representation. A query optimizer automatically rewrites the queries to exploit the model-based representation.

ModelarDB is designed to be cross-platform and is currently automatically tested on Microsoft Windows, macOS, and Ubuntu through [GitHub Actions](https://github.com/ModelarData/ModelarDB-RS/actions). It is also known to work on FreeBSD which is [currently not supported by GitHub Actions](https://github.com/actions/runner/issues/385). It is implemented in [Rust](https://www.rust-lang.org/) and uses [Apache Arrow Flight](https://github.com/apache/arrow-rs/tree/master/arrow-flight) for communicating with clients, [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) as its query engine, [Apache Arrow](https://github.com/apache/arrow-rs) as its in-memory data format, and [Apache Parquet](https://github.com/apache/arrow-rs/tree/master/parquet) as its on-disk data format.

The prototype of ModelarDB was implemented using [Apache Spark](https://www.h2database.com/html/main.html), [Apache Cassandra](https://cassandra.apache.org/_/index.html), and [H2](https://www.h2database.com/html/main.html) and was developed as part of a [research project](https://github.com/skejserjensen/ModelarDB) at Aalborg University and later as an [open-source project](https://github.com/ModelarData/ModelarDB). While this JVM-based prototype validated the benefits of using a model-based representation for time series, it has been superseded by this much more efficient Rust-based implementation.

ModelarDB intentionally does not gather usage data. So, all users are highly encouraged to post comments, suggestions, and bugs as GitHub issues, especially if a limitation of ModelarDB prevents it from being used in a particular domain. We are also very happy to receive contributions in the form of pull requests, please see [Structure](https://github.com/ModelarData/ModelarDB-RS/tree/master#structure), [Development](https://github.com/ModelarData/ModelarDB-RS/tree/master#development), and [Contributions](https://github.com/ModelarData/ModelarDB-RS/tree/master#contributions) for more information.

## Installation
### Linux
The following commands are for Ubuntu Server. However, equivalent commands should work for other Linux distributions.

1. Install [build-essential](https://packages.ubuntu.com/jammy/build-essential): `sudo apt install build-essential`
2. Install [CMake](https://cmake.org/): `sudo apt install cmake`

### macOS
1. Install the Xcode Command Line Developer Tools: `xcode-select --install`
2. Install [CMake](https://cmake.org/) and follow the _"How to Install For Command Line Use"_ menu item.

### FreeBSD
1. Install [CMake](https://cmake.org/) as the *root* user: `pkg install cmake`
2. Install [cURL](https://curl.se/) as the *root* user: `pkg install curl`

### Windows
1. Install a supported version of [Visual Studio](https://visualstudio.microsoft.com/vs/older-downloads/) with Visual C++:
   * Visual Studio 2019 ([Supported](https://github.com/microsoft/snmalloc/blob/main/docs/BUILDING.md#building-on-windows))
   * Visual Studio 2022 ([Supported](https://github.com/microsoft/snmalloc/blob/main/docs/BUILDING.md#building-on-windows))
2. Install [CMake](https://cmake.org/) and select one of the following options during installation:
   * _Add CMake to the system PATH for all users_
   * _Add CMake to the system PATH for current user_

### All
3. Install the latest stable [Rust Toolchain](https://rustup.rs/).
4. Build, test, and run the system using Cargo:
   * Debug Build: `cargo build`
   * Release Build: `cargo build --release`
   * Run Tests: `cargo test`
   * Run Server: `cargo run --bin modelardbd path_to_local_data_folder`
   * Run Client: `cargo run --bin modelardb [server_address] [query_file]`
5. Move `modelardbd` and `modelardb` from the `target` directory to any directory.

## Usage
`modelardbd` supports two execution modes *edge* and *cloud*. For storage, `modelardbd` uses local storage and an Amazon S3-compatible or Azure Blob Storage object store (optional in edge mode). The execution mode dictates where queries are executed. When `modelardbd` is deployed in edge mode it executes queries against local storage and when it is deployed in cloud mode it executes queries against the object store. For both deployment modes, `modelardbd` automatically compresses the ingested time series using multiple different types of models and continuously transfers this compressed representation from local storage to the object store. Be aware that sharing metadata between multiple instances of `modelardbd` is currently under development, thus only the instance of `modelardbd` that ingested a time series can currently query it.

### Start Server
To run `modelardbd` in edge mode using only local storage, i.e., without transferring the ingested time series to an object store, simply pass the path to the local folder `modelardbd` should use as its data folder:

```shell
modelardbd edge path_to_local_data_folder
```

To automatically transfer ingested time series to an object store the following environment variables must first be set to appropriate values so `modelardbd` knows which object store to connect to, how to authenticate, and if an HTTP connection is allowed or if HTTPS is required:

```shell
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
AWS_ENDPOINT
AWS_ALLOW_HTTP
```

For example, to use a local instance of [MinIO](https://min.io/), assuming the access key id `KURo1eQeMhDeVsrz` and the secret access key `sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p` has been created through [MinIO's web interface](http://127.0.0.1:9001/access-keys), set the environment variables as follows:

```shell
AWS_ACCESS_KEY_ID="KURo1eQeMhDeVsrz"
AWS_SECRET_ACCESS_KEY="sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p"
AWS_DEFAULT_REGION=""
AWS_ENDPOINT="http://127.0.0.1:9000"
AWS_ALLOW_HTTP="true"
```

Then, assuming a bucket named `wind-turbine` has been created through [MinIO's web interface](http://127.0.0.1:9001/buckets), `modelardbd` can be run in edge mode with automatic transfer of the ingested time series to the MinIO bucket `wind-turbine`:

```shell
modelardbd edge path_to_local_data_folder s3://wind-turbine
```

`modelardbd` also supports using [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/) 
for the remote object store. To use [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/), 
set the following environment variables instead:

```shell
AZURE_STORAGE_ACCOUNT_NAME
AZURE_STORAGE_ACCESS_KEY
```

Assuming a container named `wind-turbine` already exists, `modelardbd` can then be run in edge mode with transfer of 
the ingested time series to the [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/) 
container `wind-turbine`:

```shell
modelardbd edge path_to_local_data_folder azureblobstorage://wind-turbine
```

To run `modelardbd` in cloud mode simply replace `edge` with `cloud` as shown below. Be aware that both a local data folder and an object store are required when `modelardbd` is run in cloud mode.

```shell
modelardbd cloud path_to_local_data_folder s3://bucket-name
```

Note that the server uses `9999` as the default port. The port can be changed by specifying a different port with an
environment variable:

```shell
MODELARDBD_PORT=9998
```

### Execute SQL
ModelarDB includes a command-line client in the form of `modelardb`. To interactively execute SQL statements against a local instance of `modelardbd` through a REPL, simply run `modelardb`:

```shell
modelardb
```

If `modelardbd` is not running on the same host, the host `modelardb` should connect to must be specified. `modelardbd`'s Apache Arrow Flight interface accepts requests on port `9999` by default so it is generally not necessary to specify a port:

```shell
modelardb 10.0.0.37
```

However, if `modelardbd` has been configured to use another port using the environment variable `MODELARDBD_PORT`, the same port must also be passed to the client:

```shell
modelardb 10.0.0.37:9998
```

`modelardb` can also execute SQL statements from a file passed as a command-line argument:

```shell
modelardb 10.0.0.37 path_to_file_with_sql_statements.sql
```

`modelardbd` can also be queried programmatically [from many different programming languages](https://arrow.apache.org/docs/index.html) using Apache Arrow Flight. The following Python example shows how to execute a simple SQL query against `modelardbd` and process the resulting stream of data points using [`pyarrow`](https://pypi.org/project/pyarrow/) and [`pandas`](https://pypi.org/project/pandas/). A PEP 249 compatible connector is also available for Python in the form of [PyModelarDB](https://github.com/ModelarData/PyModelarDB).

```python
from pyarrow import flight

flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
ticket = flight.Ticket("SELECT * FROM wind_turbine LIMIT 10")
flight_stream_reader = flight_client.do_get(ticket)

for flight_stream_chunk in flight_stream_reader:
    record_batch = flight_stream_chunk.data
    pandas_data_frame = record_batch.to_pandas()
    print(pandas_data_frame)
```

### Ingest Data
Before time series can be ingested into `modelardbd`, a model table must be created. From a user's perspective a model table functions like any other table and can be queried using SQL. However, the implementation of model table is highly optimized for time series and a model table must contain a single column with timestamps, one or more columns with fields (measurements as floating-point values), and zero or more columns with tags (metadata as strings). Model tables can be created using `CREATE MODEL TABLE` statements with the column types `TIMESTAMP`, `FIELD`, and `TAG`. For `FIELD` an error bound can optionally be specified in parentheses to enable lossy compression with a relative per value error bound, e.g., `FIELD(1.0)` creates a column with a one percent error bound. `FIELD` columns default to an error bound of zero when none is specified. `modelardb` also supports normal tables created through `CREATE TABLE` statements.

As both `CREATE MODEL TABLE` and `CREATE TABLE` are just SQL statements, both types of tables can be created using `modelardb` or programmatically using Apache Arrow Flight. For example, a model table storing a simple multivariate time series with weather data collected at different wind turbines can be created as follows:

```shell
CREATE MODEL TABLE wind_turbine(timestamp TIMESTAMP, wind_turbine TAG, wind_direction FIELD, wind_speed FIELD(1.0))
```

The following example shows how to create the same model table in Python using Apache Arrow Flight:

```python
from pyarrow import flight

flight_client = flight.FlightClient("grpc://127.0.0.1:9999")

sql = "CREATE MODEL TABLE wind_turbine(timestamp TIMESTAMP, wind_turbine TAG, wind_direction FIELD, wind_speed FIELD(1.0))"
action = flight.Action("CommandStatementUpdate", str.encode(sql))
result = flight_client.do_action(action)

print(list(result))
```

After creating a table or a model table, data can be ingested into `modelardbd` using Apache Arrow Flight. For example, the following Python example ingests three data points into the model table `wind_turbine`:

```python
import pyarrow
from pyarrow import flight

# Read the data into a PyArrow table.
timestamp = pyarrow.array([100, 200, 300])
wind_turbine = pyarrow.array(["1026", "1026", "1026"])
wind_direction = pyarrow.array([300.0, 300.0, 300.0])
wind_speed = pyarrow.array([4.0, 4.0, 4.0])
names = ["timestamp", "wind_turbine", "wind_direction", "wind_speed"]
table = pyarrow.Table.from_arrays([timestamp, wind_turbine, wind_direction, wind_speed], names=names)

# Push the table to modelardbd's Apache Arrow Flight do_put() endpoint.
flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
upload_descriptor = flight.FlightDescriptor.for_path("wind_turbine")
writer, _ = flight_client.do_put(upload_descriptor, table.schema)
writer.write(table)
writer.close()
```

While this example simply ingests three data points from memory, it is simple to extend such that it reads from other data sources. For example, [this Python script](https://github.com/ModelarData/Utilities/blob/main/Apache-Parquet-Loader/main.py) makes it simple to bulk load time series from Apache Parquet files with the same schema by reading the Apache Parquet files, creating a model table that matches their schema if it does not exist, and transferring the data in the Apache Parquet files to `modelardbd` using Apache Arrow Flight.

Time series can also be ingested into `modelardbd` using [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) with the [Apache Arrow Flight output plugin](https://github.com/ModelarData/Telegraf-Output-Apache-Arrow-Flight). By using Telegraf, data points can be efficiently streamed into `modelardbd` from a large [collection of data sources](https://www.influxdata.com/time-series-platform/telegraf/telegraf-input-plugin/) such as [MQTT](https://mqtt.org/) and [OPC-UA](https://opcfoundation.org/about/opc-technologies/opc-ua/).

## Structure
The ModelarDB project consists of the following crates:

* [modelardb_client](https://github.com/ModelarData/ModelarDB-RS/tree/master/crates/modelardb_client) - ModelarDB's command-line client in the form of the binary `modelardb`.
* [modelardb_common](https://github.com/ModelarData/ModelarDB-RS/tree/master/crates/modelardb_common) - Library providing shared functions, macros, and types for use by the other crates.
* [modelardb_compression](https://github.com/ModelarData/ModelarDB-RS/tree/master/crates/modelardb_compression) - Library providing lossless and lossy model-based compression of time series.
* [modelardb_compression_python](https://github.com/ModelarData/ModelarDB-RS/tree/master/crates/modelardb_compression_python) - Python interface for the modelardb_compression crate.
* [modelardb_server](https://github.com/ModelarData/ModelarDB-RS/tree/master/crates/modelardb_server) - The ModelarDB server in the form of the binary `modelardbd`.

## Development
All code must be formatted according to the [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md)
using [rustfmt](https://github.com/rust-lang/rustfmt). Subjects not covered in the style guide, or requirements specific to this repository, are covered here.

### Documentation
All public and private functions must have an accompanying doc comment that describes the purpose of the function. For complex functions,
the doc comment can also include a description of each parameter, the return value, and examples.

All modules must have an accompanying doc comment that describes the general functionality of the module. A brief description
of the public functions, structs, enums, or other central elements of the module can be included.

### Testing and Linting
All public and private functions must be appropriately covered by unit tests. Full coverage is intended, which means all
branches of computation within each function should be thoroughly tested.

In addition, the following commands must not return any warnings or errors for the code currently in master: [cargo build --all-targets](https://doc.rust-lang.org/cargo/commands/cargo-build.html), [cargo clippy --all-targets](https://github.com/rust-lang/rust-clippy), [cargo doc](https://doc.rust-lang.org/cargo/commands/cargo-doc.html), [cargo machete --with-metadata](https://github.com/bnjbvr/cargo-machete), and [cargo test --all-targets](https://doc.rust-lang.org/cargo/commands/cargo-test.html).

### Crates
To avoid confusion and unnecessary dependencies, a list of crates is included. Note that this only includes crates
used for purposes such as logging, where multiple crates provide similar functionality.

- Logging - [tracing](https://crates.io/crates/tracing)
- Async Runtime - [tokio](https://crates.io/crates/tokio)
- gRPC - [tonic](https://crates.io/crates/tonic)
- UUID - [uuid](https://crates.io/crates/uuid)
- SQLite - [rusqlite](https://crates.io/crates/rusqlite)
- Memory Allocation - [snmalloc-rs](https://crates.io/crates/snmalloc-rs)
- Hardware Information - [sysinfo](https://crates.io/crates/sysinfo)
- Property-based Testing - [proptest](https://crates.io/crates/proptest)
- Temporary Files and Directories - [tempfile](https://crates.io/crates/tempfile)

## Docker
An environment that includes a local [MinIO](https://min.io/) instance and an edge node using the [MinIO](https://min.io/)
instance as the remote object store, can be set up using [Docker](https://docs.docker.com/). Note that since
[Rust](https://www.rust-lang.org/) is a compiled language and a more dynamic `modelardbd` configuration might be needed,
it is not recommended to use the [Docker](https://docs.docker.com/) environment during active development of `modelardbd`.
It is however ideal to use during more static deployments or when developing components that utilize `modelardbd`.

Downloading [Docker Desktop](https://docs.docker.com/desktop/) is recommended to make maintenance of the created
containers easier. Once [Docker](https://docs.docker.com/) is set up, the [MinIO](https://min.io/) instance can be
created by running the services defined in [docker-compose-minio.yml](docker-compose-minio.yml). The services can
be built and started using the command:

```shell
docker-compose -p modelardata-minio -f docker-compose-minio.yml up
```

After the [MinIO](https://min.io/) service is created, a [MinIO](https://min.io/) client is created to initialize
the development bucket `modelardata`, if it does not already exist. [MinIO](https://min.io/) can be administered through
its [web interface](http://localhost:9001). The default username and password, `minioadmin`, can be used to log in.
A separate compose file is used for [MinIO](https://min.io/) so an existing [MinIO](https://min.io/) instance can be
used when `modelardbd` is deployed using [Docker](https://docker.com/), if necessary.

Similarly, the `modelardbd` instance can be built and started using the command:

```shell
docker-compose -p modelardbd up
```

The instance can then be accessed using the Apache Arrow Flight interface at `grpc://127.0.0.1:9999`.

## Contributions
Contributions to all aspects of ModelarDB are highly appreciated and do not need
to be in the form of code. For example, contributions can be:

- Helping other users.
- Writing documentation.
- Testing features and reporting bugs.
- Writing unit tests and integration tests.
- Fixing bugs in existing functionality.
- Refactoring existing functionality.
- Implementing new functionality.

Any questions or discussions regarding a possible contribution should be posted
in the appropriate GitHub issue if one exists, e.g., the bug report if it is a
bugfix, and as a new GitHub issue otherwise.

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.
