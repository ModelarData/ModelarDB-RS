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
ModelarDB includes two binaries that can be deployed individually or together to support different use cases.
`modelardbd` is a server that can be deployed in either *edge* or *cloud* mode and supports ingesting, compressing,
and querying time series. `modelardbm` is a manager that can be deployed to manage a cluster of `modelardbd` edge and
cloud instances. Specifically, `modelardbm` is responsible for keeping the database schema and access to external
components consistent across all `modelardbd` instances in the cluster. Furthermore, `modelardbm` implements the
[Arrow Flight RPC protocol](https://arrow.apache.org/docs/format/Flight.html#downloading-data) for querying data from
the cluster, thus providing a single, workload-balanced, interface for querying data from all `modelardbd` instances in
the cluster.

For storage, `modelardbd` uses local storage and an Amazon S3-compatible or Azure Blob Storage object store. The
execution mode dictates where queries are executed. When `modelardbd` is deployed in edge mode, it executes queries
against local storage and when it is deployed in cloud mode, it executes queries against the object store.

### Start Server
There are three options available when starting `modelardbd` depending on the desired deployment use case. Each option
has different requirements and supports different features.

1. Start `modelardbd` in edge mode using only local storage - This is a simple and easy-to-use deployment that is ideal
for testing and experimentation. It does not support transferring ingested time series to an object store and does not
support querying data from the object store. However, it does support ingesting data to and querying data from local
storage.
2. Start `modelardbd` in edge mode with a manager - An already running instance of `modelardbm`, an object store, and a
PostgreSQL database are required to start `modelardbd` in edge mode with a manager. This deployment supports transferring
ingested time series to an object store but still queries data from local storage.
3. Start `modelardbd` in cloud mode with a manager - As above, an already running instance of `modelardbm`, an object
store, and a PostgreSQL database are required to start `modelardbd` in cloud mode with a manager. This deployment
supports transferring ingested time series to an object store and queries data from the same object store.

To run `modelardbd` in edge mode using only local storage, i.e., without transferring the ingested time series to an
object store, simply pass the path to the local folder `modelardbd` should use as its data folder:

```shell
modelardbd edge path_to_local_data_folder
```

To automatically transfer ingested time series to an object store, a manager must first be started. The manager requires
a PostgreSQL database to store metadata and an Amazon S3-compatible or Azure Blob Storage object store to store the
transferred time series. The following environment variables must first be set to appropriate values so `modelardbm`
knows how to connect to the PostgreSQL database:

```shell
METADATA_DB_HOST
METADATA_DB_PASSWORD
METADATA_DB_USER
```

Furthermore, the following environment variables must be set to appropriate values so `modelardbm` knows which object
store to connect to, how to authenticate, and if an HTTP connection is allowed or if HTTPS is required:

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

Then, assuming a bucket named `wind-turbine` has been created through MinIO's command line tool or web interface and
a PostgreSQL database named `metadata` has been created, `modelardbm` can be started with the following command:

```shell
modelardbm metadata s3://wind-turbine
```

`modelardbm` also supports using [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/)
for the remote object store. To use [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/),
set the following environment variables instead:

```shell
AZURE_STORAGE_ACCOUNT_NAME
AZURE_STORAGE_ACCESS_KEY
```

Assuming a container named `wind-turbine` already exists, `modelardbm` can then be started with transfer of the ingested
time series to the [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/) container `wind-turbine`:

```shell
modelardbm metadata azureblobstorage://wind-turbine
```

When a manager is running, `modelardbd` can be started in edge mode with transfer of the ingested time series to the
object store given to `modelardbm` using the following command:

```shell
modelardbd edge path_to_local_data_folder manager_url
````

To run `modelardbd` in cloud mode simply replace `edge` with `cloud` as shown below. Be aware that when running in cloud
mode the `modelardbd` instance will execute queries against the object store given to `modelardbm` and not against local
storage.

```shell
modelardbd cloud path_to_local_data_folder manager_url
```

In both cases, access to the PostgreSQL metadata database and the object store is automatically provided by the manager
when the `modelardbd` instances are connected to it. Note that the manager uses `9998` as the default port and
that the server uses `9999` as the default port. The ports can be changed by specifying different ports with the
following environment variables:

```shell
MODELARDBM_PORT=8888
MODELARDBD_PORT=8889
```

### Ingest Data
Before time series can be ingested into `modelardbd`, a model table must be created. From a user's perspective, a model
table functions like any other table and can be queried using SQL. However, the implementation of a model table is highly
optimized for time series and a model table must contain a single column with timestamps, one or more columns with
fields (measurements as floating-point values), and zero or more columns with tags (metadata as strings). Model tables
can be created using `CREATE MODEL TABLE` statements with the column types `TIMESTAMP`, `FIELD`, and `TAG`. For `FIELD`
an error bound can optionally be specified in parentheses to enable lossy compression with a per-value error bound.
The error bound can be absolute or relative, e.g., `FIELD(1.0)` creates a column with an absolute per-value error bound that
allows each value to deviate by at most 1.0 while `FIELD(1.0%)` creates a column with a relative per-value error bound that
allows each value to deviate by at most 1.0%. `FIELD` columns default to an error bound of zero when none is specified.
If the values in a `FIELD` column can be computed from other columns they need not be stored. Instead, if a `FIELD` column
is defined using the syntax `FIELD AS expression`, e.g., `FIELD AS column_one + column_two`, the values of the `FIELD` column
will be the result of the expression. As these generated `FIELD` columns do not store any data, an error bound cannot be
defined. `modelardb` also supports normal tables created through `CREATE TABLE` statements.

As both `CREATE MODEL TABLE` and `CREATE TABLE` are just SQL statements, both types of tables can be created using
`modelardb` or programmatically using Apache Arrow Flight. For example, a model table storing a simple multivariate
time series with weather data collected at different wind turbines can be created as follows:

```shell
CREATE MODEL TABLE wind_turbine(timestamp TIMESTAMP, wind_turbine TAG, wind_direction FIELD, wind_speed FIELD(1.0%))
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

When running a larger cluster of `modelardbd` instances, it is required to use `modelardbm` to create tables and model
tables. This is a necessity as `modelardbm` is responsible for keeping the database schema consistent across all
`modelardbd` instances in the cluster. The process for creating a table on the manager is the same as when creating the
table directly on a `modelardbd` instance, as shown above. The only difference is that the gRPC URL should be changed to
connect to the manager instead. When the table is created through `modelardbm`, the table is created in all `modelardbd`
instances managed by `modelardbm` automatically.

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

It should be noted that the ingested data is only transferred to the remote object store when `modelardbd` is deployed
in edge mode with a manager or in cloud mode with a manager. When `modelardbd` is deployed in edge mode without a
manager, the ingested data is only stored in local storage.

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
timestamp: Timestamp(Microsecond, None)
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

When running a larger cluster of `modelardbd` instances, it is recommended to use `modelardbm` to query the data in the
remote object store. As mentioned above, `modelardbm` implements the
[Arrow Flight RPC protocol](https://arrow.apache.org/docs/format/Flight.html#downloading-data) for querying data. This
means that a request is made to the manager first to determine which `modelardbd` cloud instance in the cluster should
be queried. The workload of the queries sent to the manager is balanced across all `modelardbd` cloud instances and
at least one `modelardbd` cloud instance must be running for the manager to accept queries. The following Python example
shows how to execute a simple SQL query using a `modelardbm` instance and how to process the resulting stream of data
points using [`pyarrow`](https://pypi.org/project/pyarrow/) and [`pandas`](https://pypi.org/project/pandas/). It should
be noted that the manager is only responsible for workload balancing and that the query is sent directly to the `modelardbd`
cloud instance chosen by the manager which then sends the result set directly back to the requesting client.

```python
from pyarrow import flight

manager_client = flight.FlightClient("grpc://127.0.0.1:9998")
query_descriptor = flight.FlightDescriptor.for_command("SELECT * FROM wind_turbine LIMIT 10")
flight_info = manager_client.get_flight_info(query_descriptor)

endpoint = flight_info.endpoints[0]
cloud_node_url = endpoint.locations[0]

cloud_client = flight.FlightClient(cloud_node_url)
flight_stream_reader = cloud_client.do_get(endpoint.ticket)

for flight_stream_chunk in flight_stream_reader:
    record_batch = flight_stream_chunk.data
    pandas_data_frame = record_batch.to_pandas()
    print(pandas_data_frame)
```

## ModelarDB configuration
`ModelarDB` can be configured before the server is started using environment variables. A full list of the environment
variables is provided here. If an environment variable is not set, the specified default value will be used.

| **Variable**                                     | **Default** | **Description**                                                                                                                                                                                                                                                                                          |
|--------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MODELARDBM_PORT                                  | 9998        | The port of the manager Apache Arrow Flight Server.                                                                                                                                                                                                                                                      |
| MODELARDBD_PORT                                  | 9999        | The port of the server Apache Arrow Flight Server.                                                                                                                                                                                                                                                       |
| MODELARDBD_IP_ADDRESS                            | 127.0.0.1   | The IP address of the Apache Arrow Flight Server.                                                                                                                                                                                                                                                        |
| MODELARDBD_MULTIVARIATE_RESERVED_MEMORY_IN_BYTES | 512 MB      | The amount of memory to reserve for storing multivariate time series.                                                                                                                                                                                                                                    |
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
software that utilizes ModelarDB. Downloading [Docker Desktop](https://docs.docker.com/desktop/) is recommended to
make maintenance of the created containers easier.

### Single edge deployment
Once [Docker](https://docs.docker.com/) is set up, the single edge deployment can be started by running the following
command from the root of the ModelarDB-RS repository:

```shell
docker-compose -p modelardb-single -f docker-compose-single.yml up
```

Note that `-p modelardb-single` is only used to name the project to make it easier to manage in
[Docker Desktop](https://docs.docker.com/desktop/). Once created, the container can be started and stopped using
[Docker Desktop](https://docs.docker.com/desktop/) or by using the corresponding commands:

```console
docker-compose -p modelardb-single start
docker-compose -p modelardb-single stop
```

The single edge is running locally on port `9999` and can be accessed using the `modelardb` client or through
Apache Arrow Flight as described above. Tables can be created and data can be ingested, compressed, and saved to local
disk. The compressed data on local disk can then be queried.

### Cluster deployment
Once [Docker](https://docs.docker.com/) is set up, the cluster deployment can be started by running the following
command from the root of the ModelarDB-RS repository:

```shell
docker-compose -p modelardb-cluster -f docker-compose-cluster.yml up
```

Note that `-p modelardb-cluster` is only used to name the project to make it easier to manage in
[Docker Desktop](https://docs.docker.com/desktop/). Once created, the containers can be started and stopped using
[Docker Desktop](https://docs.docker.com/desktop/) or by using the corresponding commands:

```console
docker-compose -p modelardb-cluster start
docker-compose -p modelardb-cluster stop
```

The cluster deployment sets up a [MinIO](https://min.io/) object store and a [MinIO](https://min.io/) client is used to
initialize the bucket `modelardb` in the object store. [MinIO](https://min.io/) can be administered through its
[web interface](http://127.0.0.1:9001). The default username and password, `minioadmin`, can be used to log in. The
deployment also sets up a PostgreSQL database named `metadata` that is used as the metadata database for the cluster.
The username `modelardb_user` and the password `modelardb_password` can be used to access the database.

The cluster itself consists of a manager node, an edge node, and a cloud node. The manager node can be accessed using
the URL `grpc://127.0.0.1:9998`, the edge node using the URL `grpc://127.0.0.1:9999`, and the cloud node using the URL
`grpc://127.0.0.1:9997`. Tables can be created through the manager node and data can be ingested, compressed, and
transferred to the object store through the edge node or the cloud node. The compressed data in the
[MinIO](https://min.io/) object store can then be queried through the cloud node or by directing the query through
the manager node.
