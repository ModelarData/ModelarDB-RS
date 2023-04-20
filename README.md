# ModelarDB
:warning: **The current version of ModelarDB is alpha software and not yet ready for production use.**

[![Cargo Build, Lint, and Test](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/cargo-build-lint-and-test-on-pr-and-push.yml/badge.svg)](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/cargo-build-lint-and-test-on-pr-and-push.yml)
[![Python unittest](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/python-unittest-on-pr-and-push.yml/badge.svg)](https://github.com/ModelarData/ModelarDB-RS/actions/workflows/python-unittest-on-pr-and-push.yml)

ModelarDB is an efficient high-performance time series management system that is designed to efficiently ingest, 
transfer, store, and analyze high-frequency time series across the edge and cloud. It provides state-of-the-art 
lossless compression, lossy compression, and query performance by efficiently compressing time series on the edge 
using multiple different types of models such as constant and linear functions. As a result, the high-frequency time 
series can be transferred to the cloud through a connection with very limited bandwidth and stored in the cloud at 
a low cost. The compressed time series can be efficiently queried on both the edge and in the cloud using a relational 
interface and SQL without any knowledge about the model-based representation. A query optimizer automatically rewrites 
the queries to exploit the model-based representation.

ModelarDB is designed to be cross-platform and is currently automatically tested on Microsoft Windows, macOS, and Ubuntu 
through [GitHub Actions](https://github.com/ModelarData/ModelarDB-RS/actions). It is also known to work on FreeBSD which 
is [currently not supported by GitHub Actions](https://github.com/actions/runner/issues/385). It is implemented in 
[Rust](https://www.rust-lang.org/) and uses [Apache Arrow Flight](https://github.com/apache/arrow-rs/tree/master/arrow-flight) 
for communicating with clients, [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) as its query 
engine, [Apache Arrow](https://github.com/apache/arrow-rs) as its in-memory data format, and 
[Apache Parquet](https://github.com/apache/arrow-rs/tree/master/parquet) as its on-disk data format.

ModelarDB intentionally does not gather usage data. So, all users are highly encouraged to post comments, suggestions,
and bugs as GitHub issues, especially if a limitation of ModelarDB prevents it from being used in a particular domain.

## Installation
Refer to the [Installation](docs/user/README.md#installation) section of the [User](docs/user/README.md) documentation 
for installation instructions on setting up ModelarDB on four major operating systems. To easily experiment with 
ModelarDB, instructions for setting up a [Docker](https://docs.docker.com/) environment are included in the 
[Docker](docs/user/README.md#docker) section.

## Usage
Usage instructions for running a server, ingesting data, and querying data using ModelarDB are included in the 
[Usage](docs/user/README.md#usage) section of the [User](docs/user/README.md) documentation.

## Development
Refer to the [Development](docs/dev/README.md) section of the documentation for an overview of the structure of the 
project, a detailed description of each major component, and an outline of the guidelines that should be adhered 
to when contributing to the project.

## Research-Based
The deprecated prototype of ModelarDB was implemented using [Apache Spark](https://www.h2database.com/html/main.html),
[Apache Cassandra](https://cassandra.apache.org/_/index.html), and [H2](https://www.h2database.com/html/main.html)
and was developed as part of a [research project](https://github.com/skejserjensen/ModelarDB) at Aalborg University and
later as an [open-source project](https://github.com/ModelarData/ModelarDB). While the deprecated JVM-based prototype 
validated the benefits of using a model-based representation for time series, it has been superseded by this current,
much more efficient, Rust-based implementation.

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.
