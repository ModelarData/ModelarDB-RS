import unittest
import datetime

import pyarrow
from pyarrow import RecordBatch

from modelardb_compression_python import compress
from modelardb_compression_python import decompress


class ModelarDBCompressionPythonTest(unittest.TestCase):
    def test_compress_without_univariate_id(self):
        compressed = compress(self.__get_time_series(), 0.0)
        self.assertEqual(0, compressed.column(0)[0].as_py())

    def test_compress_with_univariate_id(self):
        compressed = compress(self.__get_time_series(), 0.0, 1)
        self.assertEqual(1, compressed.column(0)[0].as_py())

    def test_compress_with_univariate_id(self):
        compressed = compress(self.__get_time_series(), 0.0, 1)
        self.assertEqual(1, compressed.column(0)[0].as_py())

    def test_compress_decompress_lossless(self):
        time_series = self.__get_time_series()
        compressed = compress(time_series, 0.0, 1)
        decompressed = decompress(compressed)

        self.assertEqual(
            time_series.column(0).to_pylist(), decompressed.column(1).to_pylist()
        )
        self.assertEqual(
            time_series.column(1).to_pylist(), decompressed.column(2).to_pylist()
        )

    def test_compress_decompress_lossy(self):
        time_series = self.__get_time_series()
        compressed = compress(time_series, 1.0, 1)
        decompressed = decompress(compressed)

        self.assertEqual(
            time_series.column(0).to_pylist(), decompressed.column(1).to_pylist()
        )
        self.assertEqual(5 * [10.239999771118164], decompressed.column(2).to_pylist())

    def __get_time_series(self):
        timestamps = pyarrow.array([100, 200, 300, 400, 500], pyarrow.timestamp("ms"))
        values = pyarrow.array([10.2, 10.3, 10.2, 10.3, 10.2], pyarrow.float32())
        return RecordBatch.from_arrays(
            [timestamps, values], names=["timestamps", "values"]
        )
