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

## Development
All code should follow the official [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md). 
Subjects not covered in the style guide, or requirements specific to this repository, is covered here. 

### Documentation
All public functions should have an accompanying doc comment that describes the purpose of the function. If necessary, 
the doc comment can also include a description of each argument, the return value and examples.

All modules should have an accompanying doc comment that describes the general functionality of the module. A brief description 
of the public functions, structs, enums or other central elements of the module can be included.

All private functions should have an accompanying comment that describes the purpose of the function.

### Packages
To avoid confusion and unnecessary dependencies, a list of utility packages are included. Note that this only includes packages
used for purposes such as logging, where multiple packages provide similar functionality.

- For logging, [tracing-log](https://crates.io/crates/tracing-log) is used.