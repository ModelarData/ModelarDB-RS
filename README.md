# ModelarDB
ModelarDB is an efficient high-performance time series management system. It
provides state-of-the-art lossless compression, lossy compression, and query
performance by representing time series using multiple different types of models
such as constant and linear functions. These compressed time series can be
efficiently queried using a relational interface and SQL without any knowledge
about the model-based representation. A query optimizer automatically rewrites
the queries to exploit the model-based representation.

ModelarDB is designed to be cross-platform and is tested on Microsoft Windows,
macOS, and Ubuntu. It is implemented in [Rust](https://www.rust-lang.org/) and
uses [Apache Arrow
Flight](https://github.com/apache/arrow-rs/tree/master/arrow-flight) for
communicating with clients, [Apache Arrow
DataFusion](https://github.com/apache/arrow-datafusion) as its query engine,
[Apache Arrow](https://github.com/apache/arrow-rs) as its in-memory data format,
and [Apache Parquet](https://github.com/apache/arrow-rs/tree/master/parquet) as
its on-disk data format.

The first prototype of ModelarDB was implemented using [Apache
Spark](https://www.h2database.com/html/main.html), [Apache
Cassandra](https://cassandra.apache.org/_/index.html), and
[H2](https://www.h2database.com/html/main.html) and was developed as part of a
[research project](https://github.com/skejserjensen/ModelarDB) at Aalborg
University and later as an [open-source
project](https://github.com/ModelarData/ModelarDB). While this JVM-based
prototype validated the benefits of using a model-based representation for time
series, it has been superseded by this much more efficient Rust-based
implementation.

ModelarDB intentionally does not gather usage data. So, all users are highly
encouraged to post comments, suggestions, and bugs as GitHub issues, especially
if a limitation of ModelarDB prevents it from being used in a particular domain.

## Installation
### Linux
The following commands are for Ubuntu Server. However, equivalent commands should work for other Linux distributions.

1. Install [build-essential](https://packages.ubuntu.com/jammy/build-essential): `sudo apt install build-essential`
2. Install [CMake](https://cmake.org/): `sudo apt install cmake`

### macOS
1. Install the Xcode Command Line Developer Tools: `xcode-select --install`
2. Install [CMake](https://cmake.org/) and follow the _"How to Install For Command Line Use"_ menu item.

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
   * Run Server: `cargo run --bin modelardbd path_to_data_folder`
   * Run Client: `cargo run --bin modelardb [server_address] [query_file]`
5. Move `modelardbd` and `modelardb` from the `target` directory to any directory.

## Structure
The ModelarDB project consists of the following crates:

* [modelardb_client](https://github.com/ModelarData/ModelarDB-RS/tree/dev/split-into-crates/crates/modelardb_client) - ModelarDB's command-line client in the form of the binary `modelardb`.
* [modelardb_common](https://github.com/ModelarData/ModelarDB-RS/tree/dev/split-into-crates/crates/modelardb_common) - Library providing shared functions, macros, and types for use by the other crates.
* [modelardb_compression](https://github.com/ModelarData/ModelarDB-RS/tree/dev/split-into-crates/crates/modelardb_compression) - Library providing lossless and lossy model-based compression of time series.
* [modelardb_compression_python](https://github.com/ModelarData/ModelarDB-RS/tree/dev/split-into-crates/crates/modelardb_compression_python) - Python interface for the modelardb_compression crate.
* [modelardb_server](https://github.com/ModelarData/ModelarDB-RS/tree/dev/split-into-crates/crates/modelardb_server) - The ModelarDB server in the form of the binary `modelardbd`.

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
To avoid confusion and unnecessary dependencies, a list of crates are included. Note that this only includes crates
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