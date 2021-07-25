# MiniModelarDB
MiniModelarDB is a model-based time series management system build as a
re-design and re-implementation of
[ModelarDB](https://github.com/ModelarData/ModelarDB) in
[Rust](https://www.rust-lang.org/) using
[DataFusion](https://github.com/apache/arrow-datafusion) as the query engine,
and [Apache Arrow](https://github.com/apache/arrow-rs) as in the in-memory
format, and [Apache Parquet](https://github.com/apache/arrow-rs) as the on disk
format. The primary goals of the project is to experiment with low level
optimizations for ModelarDB that are not easily available or possible on the
JVM, design methods for efficiently irregular time series using a model-based
approach, develop a query optimizer that automatically exploits the model-based
representation, and provide a single easy to use relational interface.
MiniModelarDB is designed for Unix-like operating systems and tested on Linux
and macOS.
