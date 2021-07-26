# MiniModelarDB
MiniModelarDB is a model-based time series management system built as a
re-design and re-implementation of
[ModelarDB](https://github.com/ModelarData/ModelarDB). It is implemented in
[Rust](https://www.rust-lang.org/) and use
[DataFusion](https://github.com/apache/arrow-datafusion) as the query engine,
[Apache Arrow](https://github.com/apache/arrow-rs) as the in-memory format, and
[Apache Parquet](https://github.com/apache/arrow-rs) as the on-disk format. The
primary goals of the project are to experiment with low-level optimizations
that are not easily possible on the JVM, design methods for efficiently
representing irregular time series using a model-based approach, develop a
query optimizer that allows queries to automatically exploit the model-based
representation, and provide a single easy to use relational interface.
MiniModelarDB is designed for Unix-like operating systems and tested on Linux
and macOS.
