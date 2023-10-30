# ModelarDB Development
This document describes the structure of the code and general considerations to consider when doing further development.
As such, this document should be used as a guideline when contributing to the repository.

Contributions to all aspects of ModelarDB are highly appreciated and do not need to be in the form of code.
For example, contributions can be:

- Helping other users.
- Writing documentation.
- Testing features and reporting bugs.
- Writing unit tests and integration tests.
- Fixing bugs in existing functionality.
- Refactoring existing functionality.
- Implementing new functionality.

Any questions or discussions regarding a possible contribution should be posted in the appropriate GitHub issue if
one exists, e.g., the bug report if it is a bugfix, and as a new GitHub issue otherwise.

## Structure
The ModelarDB project consists of the following crates:

- [modelardb_client](/crates/modelardb_client) - ModelarDB's command-line client in the form of the binary `modelardb`.
- [modelardb_common](/crates/modelardb_common) - Library providing shared functions, macros, and types for use by the other crates.
- [modelardb_compression](/crates/modelardb_compression) - Library providing lossless and lossy model-based compression of time series.
- [modelardb_compression_python](/crates/modelardb_compression_python) - Python interface for the modelardb_compression crate.
- [modelardb_server](/crates/modelardb_server) - The ModelarDB server in the form of the binary `modelardbd`.

## Components
Each major component in the ModelarDB server is described in detail to support further development of the components
and ease integration between components. We also provide a larger overview of the architecture of ModelarDB, focusing
on the integration between the components to accomplish the goals of the system.

TODO: Add link to architecture documentation when new architecture is implemented and documented.

The ModelarDB server consists of the following major components:
- [Arrow Flight API]() - TODO: Write documentation for Arrow Flight API.
- [Storage Engine](storage-engine.md) - Component to manage all uncompressed and compressed data in the ModelarDB server.
- [Compression]() - TODO: Write documentation for Compression component.
- [Query Engine]() - TODO: Write documentation for Query Engine.
- [Metadata]() - TODO: Write documentation for Metadata component.

## Development
All code must be formatted according to the [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md)
using [rustfmt](https://github.com/rust-lang/rustfmt). Subjects not covered in the style guide, or requirements specific
to this repository, are covered here.

### Documentation
All public and private functions must have an accompanying doc comment that describes the purpose of the function. For
complex functions, the doc comment should also include a description of each parameter, the return value,
and, if beneficial, examples.

All modules must have an accompanying doc comment that describes the general functionality of the module. A brief
description of the public functions, structs, enums, or other central elements of the module can be included.

### Testing and Linting
All public and private functions must be appropriately covered by unit tests. Full coverage is intended, which means all
branches of computation within each function should be thoroughly tested.

In addition, the following commands must not return any warnings or errors for the code currently in main:
- [cargo build --all-targets](https://doc.rust-lang.org/cargo/commands/cargo-build.html)
- [cargo clippy --all-targets](https://github.com/rust-lang/rust-clippy)
- [cargo doc](https://doc.rust-lang.org/cargo/commands/cargo-doc.html)
- [cargo machete --with-metadata](https://github.com/bnjbvr/cargo-machete)
- [cargo test --all-targets](https://doc.rust-lang.org/cargo/commands/cargo-test.html)

### Crates
To avoid confusion and unnecessary dependencies, a list of crates used in the project is included. Note that this only
includes crates used for purposes such as logging, where multiple crates provide similar functionality.

- Logging - [tracing](https://crates.io/crates/tracing)
- Async Runtime - [tokio](https://crates.io/crates/tokio)
- gRPC - [tonic](https://crates.io/crates/tonic)
- UUID - [uuid](https://crates.io/crates/uuid)
- Database Access - [sqlx](https://crates.io/crates/sqlx)
- TLS - [rustls](https://crates.io/crates/rustls)
- Memory Allocation - [snmalloc-rs](https://crates.io/crates/snmalloc-rs)
- Hardware Information - [sysinfo](https://crates.io/crates/sysinfo)
- Property-based Testing - [proptest](https://crates.io/crates/proptest)
- Temporary Files and Directories - [tempfile](https://crates.io/crates/tempfile)
