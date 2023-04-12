# ModelarDB
:warning: **The current version of ModelarDB is alpha software and not yet ready for production use.**

[![Cargo Build, Lint, and Test](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/cargo-build-lint-and-test-on-pr-and-push.yml/badge.svg)](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/cargo-build-lint-and-test-on-pr-and-push.yml)
[![Python unittest](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/python-unittest-on-pr-and-push.yml/badge.svg)](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/python-unittest-on-pr-and-push.yml)

ModelarDB is an efficient high-performance time series management system that is designed to efficiently ingest, transfer, store, and analyze high-frequency time series across the edge and cloud. It provides state-of-the-art lossless compression, lossy compression, and query performance by efficiently compressing time series on the edge using multiple different types of models such as constant and linear functions. As a result, the high-frequency time series can be transferred to the cloud through a connection with very limited bandwidth and stored in the cloud at a low cost. The compressed time series can be efficiently queried on both the edge and in the cloud using a relational interface and SQL without any knowledge about the model-based representation. A query optimizer automatically rewrites the queries to exploit the model-based representation.

ModelarDB is designed to be cross-platform and is currently automatically tested on Microsoft Windows, macOS, and Ubuntu through [GitHub Actions](https://github.com/ModelarData/ModelarDB-RS/actions). It is also known to work on FreeBSD which is [currently not supported by GitHub Actions](https://github.com/actions/runner/issues/385). It is implemented in [Rust](https://www.rust-lang.org/) and uses [Apache Arrow Flight](https://github.com/apache/arrow-rs/tree/master/arrow-flight) for communicating with clients, [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) as its query engine, [Apache Arrow](https://github.com/apache/arrow-rs) as its in-memory data format, and [Apache Parquet](https://github.com/apache/arrow-rs/tree/master/parquet) as its on-disk data format.

The prototype of ModelarDB was implemented using [Apache Spark](https://www.h2database.com/html/main.html), [Apache Cassandra](https://cassandra.apache.org/_/index.html), and [H2](https://www.h2database.com/html/main.html) and was developed as part of a [research project](https://github.com/skejserjensen/ModelarDB) at Aalborg University and later as an [open-source project](https://github.com/ModelarData/ModelarDB). While this JVM-based prototype validated the benefits of using a model-based representation for time series, it has been superseded by this much more efficient Rust-based implementation.

We are also very happy to receive contributions in the form of pull requests, please see [Structure](https://github.com/ModelarData/ModelarDB-RS/tree/master#structure), [Development](https://github.com/ModelarData/ModelarDB-RS/tree/master#development), and [Contributions](https://github.com/ModelarData/ModelarDB-RS/tree/master#contributions) for more information.

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
