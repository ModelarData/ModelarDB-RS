# ModelarDB Compression Python
This crate is a Python module that wraps ModelarDB's compression library, thus
allowing it to be used through Python. The code for converting Apache Arrow
constructs from Python to Rust and Rust to Python is based on code released
under Apache2 from
[apache/arrow-rs](https://github.com/apache/arrow-rs/blob/master/arrow/src/pyarrow.rs)
and
[jhoekx/python-rust-arrow-interop-example](https://github.com/jhoekx/python-rust-arrow-interop-example/blob/master/src/lib.rs).

## Installation
1. Install the [dependencies](/docs/user/README.md#installation) required to build ModelarDB's compression library.
2. Install and test the Python module using Python:
   * Install: `python3 -m pip install .`
   * Run Tests: `python3 -m unittest`

## Example
```python
import pyarrow
from pyarrow import RecordBatch

from modelardb_compression_python import compress
from modelardb_compression_python import decompress

timestamps = pyarrow.array([100, 200, 300, 400, 500], pyarrow.timestamp('ms'))
values = pyarrow.array([10.2, 10.3, 10.2, 10.3, 10.2], pyarrow.float32())
time_series = RecordBatch.from_arrays([timestamps, values], names=["timestamps", "values"])

error_bound = 1.0  # Per value error bound in percentage.
univariate_id = 1  # User-defined id for the time series.

compressed = compress(time_series, error_bound, univariate_id)
decompressed = decompress(compressed)
```