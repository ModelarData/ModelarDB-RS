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
1. Install a supported version of [Visual Studio](https://visualstudio.microsoft.com/vs/older-downloads/) with Visual C++:
   - [Visual Studio 2019](https://visualstudio.microsoft.com/vs/older-downloads/#visual-studio-2019-and-other-products) ([Supported](https://github.com/microsoft/snmalloc/blob/main/docs/BUILDING.md#building-on-windows))
   - [Visual Studio 2022](https://visualstudio.microsoft.com/vs/) ([Supported](https://github.com/microsoft/snmalloc/blob/main/docs/BUILDING.md#building-on-windows))

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
[MinIO's web interface](http://127.0.0.1:9001/access-keys), set the environment variables as follows:

```shell
AWS_ACCESS_KEY_ID="KURo1eQeMhDeVsrz"
AWS_SECRET_ACCESS_KEY="sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p"
AWS_DEFAULT_REGION=""
AWS_ENDPOINT="http://127.0.0.1:9000"
AWS_ALLOW_HTTP="true"
```

Then, assuming a bucket named `wind-turbine` has been created through
[MinIO's web interface](http://127.0.0.1:9001/buckets), `modelardbd` can be run in edge mode with automatic transfer of
the ingested time series to the MinIO bucket `wind-turbine`:

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

After creating a table or a model table, data can be ingested into `modelardbd` using Apache Arrow Flight. For example,
the following Python example ingests three data points into the model table `wind_turbine`:

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

## Docker
An environment that includes a local [MinIO](https://min.io/) instance and an edge node using the [MinIO](https://min.io/)
instance as the remote object store, can be set up using [Docker](https://docs.docker.com/). Note that since
[Rust](https://www.rust-lang.org/) is a compiled language and a more dynamic `modelardbd` configuration might be needed,
it is not recommended to use the [Docker](https://docs.docker.com/) environment during active development of `modelardbd`.
It is however ideal to use for experimenting with `modelardbd` or when developing components that utilize `modelardbd`.

Downloading [Docker Desktop](https://docs.docker.com/desktop/) is recommended to make maintenance of the created
containers easier. Once [Docker](https://docs.docker.com/) is set up, the [MinIO](https://min.io/) instance can be
created by running the services defined in [docker-compose-minio.yml](docker-compose-minio.yml). The services can
be built and started using the command:

```shell
docker-compose -p modelardata-minio -f docker-compose-minio.yml up
```

After the [MinIO](https://min.io/) service is created, a [MinIO](https://min.io/) client is used to initialize
the development bucket `modelardata`, if it does not already exist. [MinIO](https://min.io/) can be administered through
its [web interface](http://localhost:9001). The default username and password, `minioadmin`, can be used to log in.
A separate compose file is used for [MinIO](https://min.io/) so an existing [MinIO](https://min.io/) instance can be
used when `modelardbd` is deployed using [Docker](https://docker.com/), if necessary.

Similarly, the `modelardbd` instance can be built and started using the command:

```shell
docker-compose -p modelardbd up
```

The instance can then be accessed using the Apache Arrow Flight interface at `grpc://127.0.0.1:9999`.
