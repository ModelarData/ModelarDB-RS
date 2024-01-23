# ModelarDB Installation and Usage
This document describes how to set up and use ModelarDB. Installation instructions are provided for
Linux, macOS, FreeBSD, and Windows. To support running ModelarDB in a containerized environment, instructions for
setting up a Docker environment are also provided. Once installed, using ModelarDB is consistent across all platforms.

## Installation
### Linux
The following commands are for Ubuntu Server. However, equivalent commands should work for other Linux distributions.

1. Install [build-essential](https://packages.ubuntu.com/jammy/build-essential): `sudo apt install build-essential`

### macOS
1. Install the Xcode Command Line Developer Tools: `xcode-select --install`

### FreeBSD
1. Install [cURL](https://curl.se/) as the *root* user: `pkg install curl`

### Windows
1. Install the latest versions of the Microsoft Visual C++ Prerequisites for Rust:
   - Microsoft Visual C++ Prerequisites for Rust: see [The rustup book](https://rust-lang.github.io/rustup/installation/windows-msvc.html).

### All
2. Install the latest stable [Rust Toolchain](https://rustup.rs/).
3. Build, test, and run the system using Cargo:
   - Debug Build: `cargo build`
   - Release Build: `cargo build --release`
   - Run Tests: `cargo test`
   - Run Server: `cargo run --bin modelardbd path_to_local_data_folder`
   - Run Client: `cargo run --bin modelardb [server_address] [query_file]`
4. Move `modelardbd`, `modelardbm`, and `modelardb` from the `target` directory to any directory.

## Usage
`modelardbd` supports two execution modes, *edge* and *cloud*. For storage, `modelardbd` uses local storage and an
Amazon S3-compatible or Azure Blob Storage object store (optional in edge mode). The execution mode dictates where
queries are executed. When `modelardbd` is deployed in edge mode, it executes queries against local storage and when it
is deployed in cloud mode, it executes queries against the object store. For both deployment modes, `modelardbd`
automatically compresses the ingested time series using multiple different types of models and continuously transfers
this compressed representation from local storage to the object store. Be aware that sharing metadata between multiple
instances of `modelardbd` is currently under development, thus only the instance of `modelardbd` that ingested a time series can currently query it.

### Start Server
To run `modelardbd` in edge mode using only local storage, i.e., without transferring the ingested time series to an
object store, simply pass the path to the local folder `modelardbd` should use as its data folder:

```shell
modelardbd edge path_to_local_data_folder
```

To automatically transfer ingested time series to an object store the following environment variables must first be
set to appropriate values so `modelardbd` knows which object store to connect to, how to authenticate, and if an HTTP
connection is allowed or if HTTPS is required:

```shell
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
AWS_ENDPOINT
AWS_ALLOW_HTTP
```

For example, to use a local instance of [MinIO](https://min.io/), assuming the access key id `KURo1eQeMhDeVsrz` and the
secret access key `sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p` has been created through
MinIO's command line tool or web interface, set the environment variables as follows:

```shell
AWS_ACCESS_KEY_ID="KURo1eQeMhDeVsrz"
AWS_SECRET_ACCESS_KEY="sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p"
AWS_DEFAULT_REGION=""
AWS_ENDPOINT="http://127.0.0.1:9000"
AWS_ALLOW_HTTP="true"
```

Then, assuming a bucket named `wind-turbine` has been created through MinIO's command line tool or web interface, `modelardbd` 
can be run in edge mode with automatic transfer of the ingested time series to the MinIO bucket `wind-turbine`:

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

To run `modelardbd` in cloud mode simply replace `edge` with `cloud` as shown below. Be aware that both a local data
folder and an object store are required when `modelardbd` is run in cloud mode.

```shell
modelardbd cloud path_to_local_data_folder s3://bucket-name
```

Note that the server uses `9999` as the default port. The port can be changed by specifying a different port with an
environment variable:

```shell
MODELARDBD_PORT=9998
```

### Execute SQL
ModelarDB includes a command-line client in the form of `modelardb`. To interactively execute SQL statements against a
local instance of `modelardbd` through a REPL, simply run `modelardb`:

```shell
modelardb
```

Be aware that the REPL currently does not support splitting SQL statements over multiple lines and that the SQL
statements do not need to end with a `;`. In addition to SQL statements, the REPL also supports listing all tables
using `\dt`, printing the schema of a table using `\d table_name`, flushing data in memory to disk using `\f`, flushing
data in memory and on disk to an object store using `\F`, and printing operations supported by the client using `\h`.

```sql
ModelarDB> \dt
wind_turbine
ModelarDB> \d wind_turbine
timestamp: Timestamp(Millisecond, None)
wind_turbine: Utf8
wind_direction: Float32
wind_speed: Float32, Error Bound 1%
ModelarDB> SELECT * FROM wind_turbine LIMIT 5
+-------------------------+--------------+------------+----------------+
| timestamp               | wind_turbine | wind_speed | wind_direction |
+-------------------------+--------------+------------+----------------+
| 2020-01-01T00:00:00.000 | KL3773       | 4.7987617  | 12.195049      |
| 2020-01-01T00:00:00.100 | KL3773       | 4.7562086  | 10.183235      |
| 2020-01-01T00:00:00.200 | KL3773       | 4.7384476  | 9.4519857      |
| 2020-01-01T00:00:00.300 | KL3773       | 4.7108905  | 9.8607325      |
| 2020-01-01T00:00:00.400 | KL3773       | 4.6879335  | 10.083183      |
+-------------------------+--------------+------------+----------------+
ModelarDB>
```

If `modelardbd` is not running on the same host, the host `modelardb` should connect to must be specified.
`modelardbd`'s Apache Arrow Flight interface accepts requests on port `9999` by default, so it is generally not
necessary to specify a port:

```shell
modelardb 10.0.0.37
```

However, if `modelardbd` has been configured to use another port using the environment variable `MODELARDBD_PORT`,
the same port must also be passed to the client:

```shell
modelardb 10.0.0.37:9998
```

`modelardb` can also execute SQL statements from a file passed as a command-line argument:

```shell
modelardb 10.0.0.37 path_to_file_with_sql_statements.sql
```

`modelardbd` can also be queried programmatically [from many different programming languages](https://arrow.apache.org/docs/index.html)
using Apache Arrow Flight. The following Python example shows how to execute a simple SQL query against `modelardbd`
and process the resulting stream of data points using [`pyarrow`](https://pypi.org/project/pyarrow/) and
[`pandas`](https://pypi.org/project/pandas/). A PEP 249 compatible connector is also available for Python in the
form of [PyModelarDB](https://github.com/ModelarData/PyModelarDB).

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
Before time series can be ingested into `modelardbd`, a model table must be created. From a user's perspective a model
table functions like any other table and can be queried using SQL. However, the implementation of model table is highly
optimized for time series and a model table must contain a single column with timestamps, one or more columns with
fields (measurements as floating-point values), and zero or more columns with tags (metadata as strings). Model tables
can be created using `CREATE MODEL TABLE` statements with the column types `TIMESTAMP`, `FIELD`, and `TAG`. For `FIELD`
an error bound can optionally be specified in parentheses to enable lossy compression with a relative per value error
bound, e.g., `FIELD(1.0)` creates a column with a one percent error bound. `FIELD` columns default to an error bound of
zero when none is specified. If the values in a `FIELD` column can be computed from other columns they need not be stored.
Instead, if a `FIELD` column is defined using the syntax `FIELD AS expression`, e.g., `FIELD AS column_one + column_two`,
the values of the `FIELD` column will be the result of the expression. As these generated `FIELD` columns do not store any
data, an error bound cannot be defined. `modelardb` also supports normal tables created through `CREATE TABLE` statements.

As both `CREATE MODEL TABLE` and `CREATE TABLE` are just SQL statements, both types of tables can be created using
`modelardb` or programmatically using Apache Arrow Flight. For example, a model table storing a simple multivariate
time series with weather data collected at different wind turbines can be created as follows:

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

After creating a table or a model table, data can be ingested into `modelardbd` with `INSERT` in `modelardb`. Be aware that
`INSERT` statements currently must contain values for all columns but that the values for generated columns will be dropped
by `modelardbd`. As parsing `INSERT` statements add significant overhead, binary data can also be ingested programmatically
using Apache Arrow Flight. For example, this Python example ingests three data points into the model table `wind_turbine`:

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

While this example simply ingests three data points from memory, it is simple to extend such that it reads from other
data sources. For example, [this Python script](https://github.com/ModelarData/Utilities/blob/main/Apache-Parquet-Loader/main.py)
makes it simple to bulk load time series from Apache Parquet files with the same schema by reading the Apache Parquet
files, creating a model table that matches their schema if it does not exist, and transferring the data in the Apache
Parquet files to `modelardbd` using Apache Arrow Flight.

Time series can also be ingested into `modelardbd` using [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/)
with the [Apache Arrow Flight output plugin](https://github.com/ModelarData/Telegraf-Output-Apache-Arrow-Flight).
By using Telegraf, data points can be efficiently streamed into `modelardbd` from a large
[collection of data sources](https://www.influxdata.com/time-series-platform/telegraf/telegraf-input-plugin/)
such as [MQTT](https://mqtt.org/) and [OPC-UA](https://opcfoundation.org/about/opc-technologies/opc-ua/).

## ModelarDB configuration
`ModelarDB` can be configured before the server is started using environment variables. A full list of the environment 
variables is provided here. If an environment variable is not set, the specified default value will be used.

| **Variable**                                     | **Default** | **Description**                                                                                                                                                                                                                                                                                          |
|--------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MODELARDBD_PORT                                  | 9999        | The port of the Apache Arrow Flight Server.                                                                                                                                                                                                                                                              |
| MODELARDBD_IP_ADDRESS                            | 127.0.0.1   | The IP address of the Apache Arrow Flight Server.                                                                                                                                                                                                                                                        |
| MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES | 512 MB      | The amount of memory to reserve for storing uncompressed data buffers.                                                                                                                                                                                                                                   |
| MODELARDBD_COMPRESSED_RESERVED_MEMORY_IN_BYTES   | 512 MB      | The amount of memory to reserve for storing compressed data buffers.                                                                                                                                                                                                                                     |
| MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES          | 64 MB       | The amount of data that must be collected before transferring a batch of data to the remote object store.                                                                                                                                                                                                |
| MODELARDBD_TRANSFER_TIME_IN_SECONDS              | Disabled    | The number of seconds between each transfer of data to the remote object store.                                                                                                                                                                                                                          |
| MODELARDBD_UNCOMPRESSED_DATA_BUFFER_CAPACITY     | 65536       | The capacity of each uncompressed data buffer as the number of elements in the buffer where each element is a timestamp and a value. Note that the resulting size of the buffer has to be a multiple of 64 bytes to avoid the actual capacity being larger than the requested due to internal alignment. |

## Docker
Two different [Docker](https://docs.docker.com/) environments are included to make it easy to experiment with the 
different use cases of ModelarDB. The first environment sets up a single instance of `modelardbd` that only uses local 
storage. Time series data can be ingested into this instance, compressed, and saved to local storage. The compressed 
data in local storage can then be queried. The second environment covers the more complex use case of ModelarDB where 
multiple instances of `modelardbd` are deployed in a cluster with a manager that is responsible for managing the 
cluster. A single edge node and a single cloud node is set up in the cluster. Data can be ingested into the edge or 
cloud node, compressed, and transferred to a remote object store. The compressed data in the remote object store can 
then be queried through the cloud node or by directing the query through the manager node.

Note that since [Rust](https://www.rust-lang.org/) is a compiled language and a more dynamic ModelarDB configuration
might be needed, it is not recommended to use the [Docker](https://docs.docker.com/) environments during active 
development of ModelarDB. They are however ideal to use for experimenting with ModelarDB or when developing 
components that utilize ModelarDB. Downloading [Docker Desktop](https://docs.docker.com/desktop/) is recommended to 
make maintenance of the created containers easier.
