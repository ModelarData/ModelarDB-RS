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
   * Visual Studio 2017 ([Supported](https://github.com/microsoft/snmalloc/blob/main/docs/BUILDING.md#building-on-windows))
   * Visual Studio 2019 ([Supported](https://github.com/microsoft/snmalloc/issues/525#issuecomment-1128901104))
   * Note that Visual Studio 2022 is **not** supported.
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

## Development
All code must be formatted according to the [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md)
using [rustfmt](https://github.com/rust-lang/rustfmt). Subjects not covered in the style guide, or requirements specific to this repository, are covered here.

### Documentation
All public and private functions must have an accompanying doc comment that describes the purpose of the function. For complex functions,
the doc comment can also include a description of each parameter, the return value, and examples.

All modules must have an accompanying doc comment that describes the general functionality of the module. A brief description
of the public functions, structs, enums, or other central elements of the module can be included.

### Testing
All public and private functions must be covered by unit tests. Full coverage is required, which means all
branches of computation within the function needs to be tested.

### Crates
To avoid confusion and unnecessary dependencies, a list of utility crates are included. Note that this only includes crates
used for purposes such as logging, where multiple crates provide similar functionality.

- Logging - [tracing](https://crates.io/crates/tracing)
- Async Runtime - [tokio](https://crates.io/crates/tokio)
- Property-based Testing - [proptest](https://crates.io/crates/proptest)

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
