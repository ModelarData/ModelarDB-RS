# MiniModelarDB
MiniModelarDB is a model-based time series management system that is primarily
built as a re-design and re-implementation of
[ModelarDB](https://github.com/ModelarData/ModelarDB). It is implemented in
[Rust](https://www.rust-lang.org/) and uses
[DataFusion](https://github.com/apache/arrow-datafusion) as the query engine,
[Apache Arrow](https://github.com/apache/arrow-rs) as the in-memory format, and
[Apache Parquet](https://github.com/apache/arrow-rs) as the on-disk format. The
primary goals of the project are to experiment with low-level optimizations
that are not easily possible on the JVM, design methods for efficiently
representing irregular time series using a model-based approach while only
requiring users to specify an error bound, develop a query optimizer that
allows queries to automatically exploit the model-based representation, and
provide a single easy to use relational query interface. MiniModelarDB is
designed for Unix-like operating systems and is tested on Linux.

MiniModelarDB intentionally does not gather usage data. So, all users are highly
encouraged to post comments, suggestions, and bugs as GitHub issues, especially
if a limitation of MiniModelarDB prevents it from being used in a particular domain.

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
Contributions to all aspects of MiniModelarDB are highly appreciated and do not
need to be in the form of code. For example, contributions can be:

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
MiniModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.