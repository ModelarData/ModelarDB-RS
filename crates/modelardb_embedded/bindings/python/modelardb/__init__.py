"""Python bindings for the C-API of ModelarDB Embedded."""

# Copyright 2025 The ModelarDB Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import os
import platform
import warnings
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Self

import pyarrow
from pyarrow import MapArray, RecordBatch, Schema, StringArray, Array
from pyarrow.cffi import ffi


@dataclass
class AbsoluteErrorBound:
    """A per value absolute error bound.

    param value: A positive finite value that specifies the error bound.
    :type value: float
    :raises ValueError: If `value` is not a positive normal value.
    """

    def __init__(self, value: float):
        # Error checking is required as the C-API depends on the error bounds being valid.
        if not math.isfinite(value) or value < 0:
            raise ValueError("An absolute error bound must be a positive finite value.")
        self.value = value


@dataclass
class RelativeErrorBound:
    """A per value relative error bound.

    param value: A value between 0 and 100 that specifies the error bound.
    :type value: float
    :raises ValueError: If `value` is not between 0 an 100.
    """

    def __init__(self, value: float):
        # Error checking is required as the C-API depends on the error bounds being valid.
        if not 0 <= value <= 100:
            raise ValueError(
                "A relative error bound must be a value from 0.0% to 100.0%."
            )
        self.value = value


@dataclass
class NormalTable:
    """A relational table storing any combination of columns.

    :param schema: The schema of the normal table.
    :type schema: Schema
    """

    def __init__(self, schema: Schema):
        self.schema: Schema = schema


@dataclass
class ModelTable:
    """A relational table storing time series with metadata more efficiently
    than :class:`Table` by storing the time series with metadata as models.

    :param schema: The schema of the model table. It must contain a timestamp
    column with the type :class:`pyarrow.timestamp("us")`, one or more field columns
    with the type :class:`pyarrow.float32()`, and zero or more tag columns with the
    type :class:`pyarrow.string()`.
    :type schema: Schema
    :param error_bounds: Absolute or relative error bounds for the field columns
    with type :class:`pyarrow.float32()` in `schema`. If no error bound is
    specified for a column, it will be zero so the values will be stored
    losslessly.
    :type error_bounds: dict[str, AbsoluteErrorBound | RelativeErrorBound],
    optional
    :param generated_columns: SQL expressions for generating field columns of
    type :class:`pyarrow.float32()` in `schema`. If no generated column is
    specified for a field column, it will be stored.
    :type error_bounds: dict[str, str], optional
    """

    def __init__(
        self,
        schema: Schema,
        error_bounds: None | dict[str, AbsoluteErrorBound | RelativeErrorBound] = None,
        generated_columns: None | dict[str, str] = None,
    ):
        self.schema = schema
        self.error_bounds = {} if error_bounds is None else error_bounds
        self.__check_if_column_exists(self.error_bounds.keys(), schema)
        self.generated_columns = {} if generated_columns is None else generated_columns
        self.__check_if_column_exists(self.generated_columns.keys(), schema)

    def __check_if_column_exists(self, columns: [str], schema: Schema):
        """Raise an error if a column in :attr:`columns` does not exist in
        :attr:`schema`


        :raises ValueError: If a column does not exist.
        """
        for column in columns:
            if schema.get_field_index(column) == -1:
                raise ValueError(f"The column {column} does not exist.")


@dataclass
class Server:
    """A ModelarDB edge or cloud server node.

    :param url: The URL of the ModelarDB server node.
    :type url: str
    """

    def __init__(self, url: str):
        self.url: str = url


@dataclass
class Manager:
    """A ModelarDB manager node.

    :param url: The URL of the ModelarDB manager node.
    :type url: str
    """

    def __init__(self, url: str):
        self.url: str = url


class Aggregate(Enum):
    """Aggregate operations supported by :meth:`ModelarDB.read`."""

    NONE = 0
    COUNT = 1
    MIN = 2
    MAX = 3
    SUM = 4
    AVG = 5


class FFIArray:
    """An Apache Arrow array that can be accessed as both a PyArrow
    :class:`Array` and as two pointers to the data and schema using
    Apache Arrow's C Data Interface.
    """

    @classmethod
    def from_array(cls, array: Array | RecordBatch):
        """Create a :class:`FFIArray` from an existing PyArrow :class:`Array`
        or :class:`RecordBatch`.

        :param array: PyArrow :class:`Array` or :class:`RecordBatch` to convert
        to pointers using Apache Arrow's C Data Interface.
        :type array: Array | RecordBatch
        """
        # PyArrow Array to C Data Interface pointers to array and schema.
        self: FFIArray = cls()

        self.__array = array
        self.array_type = type(self.__array)
        self.array_ptr = ffi.new("struct ArrowArray*")
        self.schema_ptr = ffi.new("struct ArrowSchema*")
        self.array_ptr_int = int(ffi.cast("uintptr_t", self.array_ptr))
        self.schema_ptr_int = int(ffi.cast("uintptr_t", self.schema_ptr))
        self.__array._export_to_c(self.array_ptr_int, self.schema_ptr_int)

        return self

    @classmethod
    def from_ffi(cls, array_type: type, array_ptr, schema_ptr):
        """Create a :class:`FFIArray` from a pointer to an Apache Arrow array
        and schema using Apache Arrow's C Data Interface.

        :param array_type: Type of PyArrow :class:`Array` or
        :class:`RecordBatch` in :attr:`array_ptr`.
        :type array_type: type
        :param array_ptr: Pointer to Apache Arrow C Data Interface ArrowArray to
        convert from.
        :type array_ptr: cdata 'struct ArrowArray*'
        :param schema_ptr: Pointer to Apache Arrow C Data Interface ArrowSchema to
        convert from.
        :type schema_ptr: cdata 'struct ArrowSchema*'
        """
        # C Data Interface pointers to array and schema to PyArrow Array.
        self: FFIArray = cls()

        self.__array = None
        self.array_type = array_type
        self.array_ptr = array_ptr
        self.schema_ptr = schema_ptr
        self.array_ptr_int = int(ffi.cast("uintptr_t", self.array_ptr))
        self.schema_ptr_int = int(ffi.cast("uintptr_t", self.schema_ptr))
        self.__array = self.array_type._import_from_c(
            self.array_ptr_int, self.schema_ptr_int
        )

        return self

    @classmethod
    def from_type(cls, array_type: type):
        """Create a :class:`FFIArray` for a specific PyArrow :class:`Array` or
        :class:`RecordBatch` that a matching Apache Arrow array and schema using
        Apache Arrow's C Data Interface can be written to.

        :param array_type: Type of PyArrow :class:`Array` or
        :class:`RecordBatch`.
        :type array_type: type
        """
        # Create C Data Interface pointers to array and schema to be converted
        # to a PyArrow Array. Designed to simplify returning Arrays from Rust.
        self: FFIArray = cls()

        self.__array = None
        self.array_type = array_type
        self.array_ptr = ffi.new("struct ArrowArray*")
        self.schema_ptr = ffi.new("struct ArrowSchema*")
        self.array_ptr_int = int(ffi.cast("uintptr_t", self.array_ptr))
        self.schema_ptr_int = int(ffi.cast("uintptr_t", self.schema_ptr))

        return self

    def array(self) -> Array | RecordBatch:
        """Returns the contents of :attr:`array_ptr` as a :attr:`array_type`
        with the schema in :attr:`array_schema_ptr.`
        """
        # If FFIArray was constructed by passing only array_type, no data was
        # available when it was created, so the conversion must be done now.
        if self.__array is None:
            self.__array = self.array_type._import_from_c(
                self.array_ptr_int, self.schema_ptr_int
            )
        return self.__array


class ModelarDB:
    """Class used to interact with a ModelarDB data folder or a ModelarDB node."""

    @staticmethod
    def __find_and_load_library():
        """Finds and loads the correct version of ModelarDB Embedded on this system.

        :raises RuntimeError: If there is no compatible version of ModelarDB Embedded.
        """

        # Inner function so __find_and_load_library() can call it despite being static.
        def __find_library(build: str) -> str:
            """Finds the correct version of ModelarDB Embedded on this system.

            :raises RuntimeError: If it is not executed on Linux, macOS, or Windows.
            """
            script_folder = os.path.dirname(os.path.abspath(__file__))

            # Select the library to use based on the operating system.
            match platform.system():
                case "Linux":
                    library_path = (
                        script_folder
                        + f"/../../../../../target/{build}/libmodelardb_embedded.so"
                    )
                case "Darwin":
                    library_path = (
                        script_folder
                        + f"/../../../../../target/{build}/libmodelardb_embedded.dylib"
                    )
                case "Windows":
                    library_path = (
                        script_folder
                        + f"\\..\\..\\..\\..\\..\\target\\{build}\\modelardb_embedded.dll"
                    )
                case _:
                    raise RuntimeError("Only Linux, macOS, and Windows are supported.")

            if not os.path.isfile(library_path):
                raise RuntimeError("The Rust library has not been compiled.")

            return library_path

        # Compute the path to the current working directory to locate the library.
        try:
            library_path = __find_library("release")
        except RuntimeError:
            library_path = __find_library("debug")
            warnings.warn("Using debug build, compile with --release.", RuntimeWarning)

        # cffi is used instead of ctypes as it is being used by pyarrow.cffi.
        ffi.cdef(
            """
            static int RETURN_SUCCESS;
            static int RETURN_FAILURE;

            void* modelardb_embedded_open_memory();
            void* modelardb_embedded_open_local(char* data_folder_path_ptr);
            void* modelardb_embedded_open_s3(char* endpoint_ptr,
                                             char* bucket_name_ptr,
                                             char* access_key_id_ptr,
                                             char* secret_access_key_ptr);
            void* modelardb_embedded_open_azure(char* account_name_ptr,
                                                char* access_key_ptr,
                                                char* container_name_ptr);

            void* modelardb_embedded_connect(char* node_url_ptr,
                                             bool is_server_node);

            int modelardb_embedded_close(void* maybe_modelardb_ptr, 
                                         bool is_data_folder);

            int modelardb_embedded_create(void* maybe_modelardb_ptr,
                                          bool is_data_folder,
                                          char* table_name_ptr,
                                          bool is_model_table,
                                          struct ArrowSchema* schema_ptr,
                                          struct ArrowArray* error_bound_array_ptr,
                                          struct ArrowSchema* error_bound_array_schema_ptr,
                                          struct ArrowArray* generated_columns_array_ptr,
                                          struct ArrowSchema* generated_columns_array_schema_ptr);

            int modelardb_embedded_tables(void* maybe_modelardb_ptr,
                                          bool is_data_folder,
                                          struct ArrowArray* tables_array_ptr,
                                          struct ArrowSchema* tables_array_schema_ptr);

            int modelardb_embedded_schema(void* maybe_modelardb_ptr,
                                          bool is_data_folder,
                                          char* table_name_ptr,
                                          struct ArrowArray* schema_struct_array_ptr,
                                          struct ArrowSchema* schema_struct_array_schema_ptr);

            int modelardb_embedded_write(void* maybe_modelardb_ptr,
                                         bool is_data_folder,
                                         char* table_name_ptr,
                                         struct ArrowArray* uncompressed_struct_ptr,
                                         struct ArrowSchema* uncompressed_struct_schema_ptr);

            int modelardb_embedded_read_model_table(void* maybe_modelardb_ptr,
                                                    bool is_data_folder,
                                                    char* table_name_ptr,
                                                    struct ArrowArray* columns_array_ptr,
                                                    struct ArrowSchema* columns_array_schema_ptr,
                                                    struct ArrowArray* group_by_array_ptr,
                                                    struct ArrowSchema* group_by_array_schema_ptr,
                                                    char* start_time_ptr,
                                                    char* end_time_ptr,
                                                    struct ArrowArray* tags_array_ptr,
                                                    struct ArrowSchema* tags_array_schema_ptr,
                                                    struct ArrowArray* decompressed_struct_array_ptr,
                                                    struct ArrowSchema* decompressed_struct_array_schema_ptr);

            int modelardb_embedded_copy_model_table(void* maybe_from_modelardb_ptr,
                                                    bool is_data_folder,
                                                    char* from_table_name_ptr,
                                                    void* maybe_to_modelardb_ptr,
                                                    char* to_table_name_ptr,
                                                    char* start_time_ptr,
                                                    char* end_time_ptr,
                                                    struct ArrowArray* tags_array_ptr,
                                                    struct ArrowSchema* tags_array_schema_ptr);

            int modelardb_embedded_read(void* maybe_modelardb_ptr,
                                        bool is_data_folder,
                                        char* sql_ptr,
                                        struct ArrowArray* decompressed_struct_ptr,
                                        struct ArrowSchema* decompressed_struct_schema_ptr);

            int modelardb_embedded_copy_normal_table(void* maybe_from_modelardb_ptr,
                                                     bool is_data_folder,
                                                     char* sql_ptr,
                                                     void* maybe_to_modelardb_ptr,
                                                     char* to_table_name_ptr);

            int modelardb_embedded_drop(void* maybe_modelardb_ptr,
                                        bool is_data_folder,
                                        char* table_name_ptr);

            int modelardb_embedded_truncate(void* maybe_modelardb_ptr,
                                            bool is_data_folder,
                                            char* table_name_ptr);

            int modelardb_embedded_move(void* maybe_from_modelardb_ptr,
                                        bool is_data_folder,
                                        char* from_table_name_ptr,
                                        void* maybe_to_modelardb_ptr,
                                        char* to_table_name_ptr);

            char* modelardb_embedded_error();
            """
        )

        return ffi.dlopen(library_path)

    # __library is a class variable to ensure the dynamic library's interface is
    # only defined once by ffi.cdef() and that it is loaded by ffi.dlopen() once.
    __library = __find_and_load_library()

    @classmethod
    def open_memory(cls):
        """Create a :obj:`ModelarDB` data folder that manages data in memory.

        :return: The constructed :obj:`ModelarDB`.
        :rtype: ModelarDB
        """
        self: ModelarDB = cls()

        self.__modelardb_ptr = self.__library.modelardb_embedded_open_memory()
        self.__is_data_folder = True

        if self.__modelardb_ptr == ffi.NULL:
            raise ValueError("Failed to read from or write to memory data folder.")

        return self

    @classmethod
    def open_local(cls, data_folder_path: str):
        """Create a :obj:`ModelarDB` data folder that manages data in the local folder at `data_folder_path`.

        :param data_folder_path: The path of the data folder.
        :type data_folder_path: str
        :return: The constructed :obj:`ModelarDB`.
        :rtype: ModelarDB
        """
        self: ModelarDB = cls()

        data_folder_path_ptr = ffi.new("char[]", bytes(data_folder_path, "UTF-8"))

        self.__modelardb_ptr = self.__library.modelardb_embedded_open_local(
            data_folder_path_ptr
        )
        self.__is_data_folder = True

        if self.__modelardb_ptr == ffi.NULL:
            raise ValueError("Failed to read from or write to local data folder.")

        return self

    @classmethod
    def open_s3(
        cls, endpoint: str, bucket_name: str, access_key_id: str, secret_access_key: str
    ):
        """Create a :obj:`ModelarDB` data folder that manages data in an object store with a S3-compatible API.

        :param endpoint: The endpoint of the S3-compatible object store.
        :type endpoint: str
        :param bucket_name: The name of the bucket to read data from and write data to.
        :type bucket_name: str
        :param access_key_id: The access key id to use for authentication.
        :type access_key_id: str
        :param secret_access_key: The secret access key to use for authentication.
        :type secret_access_key: str
        :return: The constructed :obj:`ModelarDB`.
        :rtype: ModelarDB
        """
        self: ModelarDB = cls()

        endpoint_ptr = ffi.new("char[]", bytes(endpoint, "UTF-8"))
        bucket_name_ptr = ffi.new("char[]", bytes(bucket_name, "UTF-8"))
        access_key_id_ptr = ffi.new("char[]", bytes(access_key_id, "UTF-8"))
        secret_access_key_ptr = ffi.new("char[]", bytes(secret_access_key, "UTF-8"))

        self.__modelardb_ptr = self.__library.modelardb_embedded_open_s3(
            endpoint_ptr, bucket_name_ptr, access_key_id_ptr, secret_access_key_ptr
        )
        self.__is_data_folder = True

        if self.__modelardb_ptr == ffi.NULL:
            raise ValueError("Failed to read from or write to S3 data folder.")

        return self

    @classmethod
    def open_azure(cls, account_name: str, access_key: str, container_name: str):
        """Create a :obj:`ModelarDB` data folder that manages data in an object store with an Azure-compatible API.

        :param account_name: The account name to use for authentication.
        :type account_name: str
        :param access_key: The secret access key to use for authentication.
        :type access_key: str
        :param container_name: The name of the container to read data from and write data to.
        :type container_name: str
        :return: The constructed :obj:`ModelarDB`.
        :rtype: ModelarDB
        """
        self: ModelarDB = cls()

        account_name_ptr = ffi.new("char[]", bytes(account_name, "UTF-8"))
        access_key_ptr = ffi.new("char[]", bytes(access_key, "UTF-8"))
        container_name_ptr = ffi.new("char[]", bytes(container_name, "UTF-8"))

        self.__modelardb_ptr = self.__library.modelardb_embedded_open_azure(
            account_name_ptr, access_key_ptr, container_name_ptr
        )
        self.__is_data_folder = True

        if self.__modelardb_ptr == ffi.NULL:
            raise ValueError("Failed to read from or write to Azure data folder.")

        return self

    @classmethod
    def connect(cls, node: Server | Manager):
        """Create a connection to a :obj:`ModelarDB` node.

        :param node: The ModelarDB node to connect to.
        :type node: Server | Manager
        """
        self: ModelarDB = cls()

        node_url_ptr = ffi.new("char[]", bytes(node.url, "UTF-8"))

        self.__modelardb_ptr = self.__library.modelardb_embedded_connect(
            node_url_ptr, isinstance(node, Server)
        )
        self.__is_data_folder = False

        if self.__modelardb_ptr == ffi.NULL:
            raise ValueError("Failed to connect to ModelarDB node.")

        return self

    def __del__(self):
        """Close the connection to the local folder, object store, or node and deallocate the memory."""
        return_code = self.__library.modelardb_embedded_close(
            self.__modelardb_ptr, self.__is_data_folder
        )
        self.__check_return_code_and_raise_error(return_code)

    def create(self, table_name: str, table_type: NormalTable | ModelTable):
        """Creates a table with `table_name`, `schema`, and `error_bounds`.

        :param table_name: The name of the table to create.
        :type table_name: str
        :param table_type: The type of the table to create.
        :type table_name: NormalTable or ModelTable
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        schema_ptr = ffi.new("struct ArrowSchema*")
        schema_ptr_int = int(ffi.cast("uintptr_t", schema_ptr))
        table_type.schema._export_to_c(schema_ptr_int)

        if isinstance(table_type, NormalTable):
            is_model_table = False
            error_bounds = {}
            generated_columns = {}
        elif isinstance(table_type, ModelTable):
            is_model_table = True

            # AbsoluteErrorBound is encoded as positive values while
            # RelativeErrorBound is encoded as negative values.
            error_bounds = {
                (column_name, error_bound.value)
                if type(error_bound) is AbsoluteErrorBound
                else (column_name, -error_bound.value)
                for column_name, error_bound in table_type.error_bounds.items()
            }
            generated_columns = table_type.generated_columns
        else:
            raise ValueError("table_type must be a NormalTable or a ModelTable")

        error_bounds_array: MapArray = pyarrow.array(
            [error_bounds], pyarrow.map_(pyarrow.string(), pyarrow.float32())
        )
        error_bounds_ffi = FFIArray.from_array(error_bounds_array)

        generated_columns_array: MapArray = pyarrow.array(
            [generated_columns], pyarrow.map_(pyarrow.string(), pyarrow.string())
        )
        generated_columns_ffi = FFIArray.from_array(generated_columns_array)

        return_code = self.__library.modelardb_embedded_create(
            self.__modelardb_ptr,
            self.__is_data_folder,
            table_name_ptr,
            is_model_table,
            schema_ptr,
            error_bounds_ffi.array_ptr,
            error_bounds_ffi.schema_ptr,
            generated_columns_ffi.array_ptr,
            generated_columns_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

    def tables(self) -> list[str]:
        """Returns the name of all the tables.

        :return: The name of all the tables.
        :rtype: list[str]
        :raises ValueError: If incorrect arguments are provided.
        """

        tables_ffi = FFIArray.from_type(StringArray)

        return_code = self.__library.modelardb_embedded_tables(
            self.__modelardb_ptr,
            self.__is_data_folder,
            tables_ffi.array_ptr,
            tables_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        return tables_ffi.array().to_pylist()

    def schema(self, table_name: str) -> Schema:
        """Returns the schema of the table with `table_name`.

        :param table_name: The name of the table to return a schema for.
        :type table_name: str
        :return: The schema of the table with `table_name`.
        :rtype: Schema
        :raises ValueError: If a table with `table_name` does not exist.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        # The schema is retrieved using an empty record batch since using a pointer to the schema causes an
        # ArrowInvalid error.
        schema_batch_ffi = FFIArray.from_type(RecordBatch)

        return_code = self.__library.modelardb_embedded_schema(
            self.__modelardb_ptr,
            self.__is_data_folder,
            table_name_ptr,
            schema_batch_ffi.array_ptr,
            schema_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        schema_batch: RecordBatch = schema_batch_ffi.array()
        return schema_batch.schema

    def write(self, table_name: str, uncompressed_batch: RecordBatch):
        """Writes the data in `uncompressed_batch` to the table with
        `table_name`.

        :param table_name: The name of the table to write to.
        :type table_name: str
        :param uncompressed_batch: The data to write.
        :type uncompressed_batch: RecordBatch
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        uncompressed_batch_ffi = FFIArray.from_array(uncompressed_batch)

        return_code = self.__library.modelardb_embedded_write(
            self.__modelardb_ptr,
            self.__is_data_folder,
            table_name_ptr,
            uncompressed_batch_ffi.array_ptr,
            uncompressed_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

    def read_model_table(
        self,
        table_name: str,
        columns: None | list[str] | list[tuple[str, Aggregate]] = None,
        group_by: None | list[str] = None,
        start_time: None | datetime | str = None,
        end_time: None | datetime | str = None,
        tags: None | dict[str, str] = None,
    ) -> RecordBatch:
        """Reads data from the model table with `table_name` and returns it. The
        remaining parameters optionally specify which subset of the data to
        read.

        :param table_name: The name of the model table to read data from.
        :type table_name: str
        :param columns: A subset of columns to read or aggregate by.
        :type columns: list[str] | list[tuple[str, Aggregate]], optional
        :param group_by: A subset of columns to group by.
        :type group_by: list[str], optional
        :param start_time: A start time to filter by as a `datetime` or an ISO 8601 `str`.
        :type start_time: datetime | str, optional
        :param end_time: An end time to filter by as a `datetime` or an ISO 8601 `str`.
        :type end_time: datetime | str, optional
        :param tags: One or more tag and tag value pairs to filter by.
        :type tags: dict[str, str], optional
        :return: The data from the model table with `table_name`.
        :rtype: RecordBatch
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        column_names = []
        aggregates = []
        if type(columns) is list and all(type(column) is str for column in columns):
            column_names = columns
            aggregates = [Aggregate.NONE.value] * len(columns)
        elif type(columns) is list and all(type(column) is tuple for column in columns):
            column_names = [column_name for column_name, _aggregate in columns]
            aggregates = [aggregate.value for _column_name, aggregate in columns]

        arrays = [
            pyarrow.array(column_names, pyarrow.string()),
            pyarrow.array(aggregates, pyarrow.int8()),
        ]
        columns_array = pyarrow.StructArray.from_arrays(
            arrays, names=["columns", "aggregates"]
        )
        columns_ffi = FFIArray.from_array(columns_array)

        if group_by is None:
            group_by = []
        group_by_array = pyarrow.array(group_by, pyarrow.string())
        group_by_ffi = FFIArray.from_array(group_by_array)

        start_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(start_time))
        end_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(end_time))

        if tags is None:
            tags = {}
        tags_array = pyarrow.array(
            [tags], pyarrow.map_(pyarrow.string(), pyarrow.string())
        )
        tags_ffi = FFIArray.from_array(tags_array)

        decompressed_batch_ffi = FFIArray.from_type(RecordBatch)

        return_code = self.__library.modelardb_embedded_read_model_table(
            self.__modelardb_ptr,
            self.__is_data_folder,
            table_name_ptr,
            columns_ffi.array_ptr,
            columns_ffi.schema_ptr,
            group_by_ffi.array_ptr,
            group_by_ffi.schema_ptr,
            start_time_ptr,
            end_time_ptr,
            tags_ffi.array_ptr,
            tags_ffi.schema_ptr,
            decompressed_batch_ffi.array_ptr,
            decompressed_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        return decompressed_batch_ffi.array()

    def copy_model_table(
        self,
        from_table_name: str,
        to_modelardb: Self,
        to_table_name: str,
        start_time: None | datetime | str = None,
        end_time: None | datetime | str = None,
        tags: None | dict[str, str] = None,
    ):
        """Copies data from the model table with `from_table_name` in `self` to
        the model table with `to_table_name` in `to_modelardb`. The remaining
        parameters optionally specify which subset of the data to copy.
        Duplicate data is not dropped.

        :param from_table_name: Name of the model table to read data from.
        :type from_table_name: str
        :param to_modelardb: :obj:`ModelarDB` to write data from `self` to.
        :type to_modelardb: ModelarDB
        :param to_table_name: Name of the model table to write data to.
        :type to_table_name: str
        :param start_time: A start time to filter by as a `datetime` or an ISO 8601 `str`.
        :type start_time: datetime | str, optional
        :param end_time: An end time to filter by as a `datetime` or an ISO 8601 `str`.
        :type end_time: datetime | str, optional
        :param tags: One or more tag and tag value pairs to filter by.
        :type tags: dict[str, str], optional
        :raises ValueError: If incorrect arguments are provided.
        """
        from_modelardb = self.__modelardb_ptr
        from_table_name = ffi.new("char[]", bytes(from_table_name, "UTF-8"))

        to_modelardb = to_modelardb.__modelardb_ptr
        to_table_name = ffi.new("char[]", bytes(to_table_name, "UTF-8"))

        start_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(start_time))
        end_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(end_time))

        if tags is None:
            tags = {}
        tags_array = pyarrow.array(
            [tags], pyarrow.map_(pyarrow.string(), pyarrow.string())
        )
        tags_ffi = FFIArray.from_array(tags_array)

        return_code = self.__library.modelardb_embedded_copy_model_table(
            from_modelardb,
            self.__is_data_folder,
            from_table_name,
            to_modelardb,
            to_table_name,
            start_time_ptr,
            end_time_ptr,
            tags_ffi.array_ptr,
            tags_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

    def __get_timestamp(self, timestamp: None | str | datetime) -> str:
        """Return `timestamp` if it is `None` or `str`, if `datetime` return it
        as an ISO 8601 `str`."""
        if type(timestamp) is datetime:
            return timestamp.isoformat()
        return timestamp

    def __str_to_c_char_ptr(self, string: None | str):
        """Return `string` as a char pointer if it is not `None` or the empty `str`,
        otherwise return the null pointer."""
        if string:
            return ffi.new("char[]", bytes(string, "UTF-8"))
        else:
            return ffi.NULL

    def read(self, sql: str) -> RecordBatch:
        """Executes an `sql` statement and returns the result.

        :param sql: An SQL statement.
        :type sql: str
        :return: The result of executing `sql`.
        :rtype: RecordBatch
        :raises ValueError: If incorrect arguments are provided.
        """
        sql_ptr = ffi.new("char[]", bytes(sql, "UTF-8"))

        decompressed_batch_ffi = FFIArray.from_type(RecordBatch)

        return_code = self.__library.modelardb_embedded_read(
            self.__modelardb_ptr,
            self.__is_data_folder,
            sql_ptr,
            decompressed_batch_ffi.array_ptr,
            decompressed_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        return decompressed_batch_ffi.array()

    def copy_normal_table(self, sql: str, to_modelardb: Self, to_table_name: str):
        """Executes an `sql` statement and copies the result to the normal
        table with `to_table_name` in `to_modelardb`. Data can be copied from
        both normal tables and model tables but only to normal tables. Duplicate
        data is not dropped.

        :param sql: An SQL statement.
        :type sql: str
        :param to_modelardb: :obj:`ModelarDB` to write data from `self` to.
        :type to_modelardb: ModelarDB
        :param to_table_name: Name of the normal table to write data to.
        :type to_table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        sql_ptr = ffi.new("char[]", bytes(sql, "UTF-8"))

        to_modelardb = to_modelardb.__modelardb_ptr
        to_table_name = ffi.new("char[]", bytes(to_table_name, "UTF-8"))

        return_code = self.__library.modelardb_embedded_copy_normal_table(
            self.__modelardb_ptr,
            self.__is_data_folder,
            sql_ptr,
            to_modelardb,
            to_table_name,
        )
        self.__check_return_code_and_raise_error(return_code)

    def move(
        self,
        from_table_name: str,
        to_modelardb: Self,
        to_table_name: str,
    ):
        """Moves all data from the table with `from_table_name` in `self`
        to the table with `to_table_name` in `to_modelardb`.

        :param from_table_name: Name of the table to read data from.
        :type from_table_name: str
        :param to_modelardb: :obj:`ModelarDB` to write data from `self` to.
        :type to_modelardb: ModelarDB
        :param to_table_name: Name of the table to write data to.
        :type to_table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        from_modelardb = self.__modelardb_ptr
        from_table_name = ffi.new("char[]", bytes(from_table_name, "UTF-8"))

        to_modelardb = to_modelardb.__modelardb_ptr
        to_table_name = ffi.new("char[]", bytes(to_table_name, "UTF-8"))

        return_code = self.__library.modelardb_embedded_move(
            from_modelardb,
            self.__is_data_folder,
            from_table_name,
            to_modelardb,
            to_table_name,
        )
        self.__check_return_code_and_raise_error(return_code)

    def drop(self, table_name: str):
        """Drops the table with `table_name`.

        :param table_name: The name of the table to drop.
        :type table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))
        return_code = self.__library.modelardb_embedded_drop(
            self.__modelardb_ptr, self.__is_data_folder, table_name_ptr
        )
        self.__check_return_code_and_raise_error(return_code)

    def truncate(self, table_name: str):
        """Truncates the table with `table_name`.

        :param table_name: The name of the table to truncate.
        :type table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))
        return_code = self.__library.modelardb_embedded_truncate(
            self.__modelardb_ptr, self.__is_data_folder, table_name_ptr
        )
        self.__check_return_code_and_raise_error(return_code)

    def __check_return_code_and_raise_error(self, return_code: int):
        """Raises an appropriate exception based on the return code.

        param return_code: The return code returned by a Rust function.
        :type return_code: int
        :raises RuntimeError or ValueError: If `return_code` is not 0.
        """
        match return_code:
            case self.__library.RETURN_SUCCESS:
                pass  # No errors.
            case self.__library.RETURN_FAILURE:
                raise RuntimeError(
                    ffi.string(self.__library.modelardb_embedded_error()).decode(
                        "UTF-8"
                    )
                )
            case _:
                raise ValueError("Unknown return code.")


def open_memory() -> ModelarDB:
    """Create a :obj:`ModelarDB` data folder that manages data in memory.

    :return: The constructed :obj:`ModelarDB`.
    :rtype: ModelarDB
    """
    return ModelarDB.open_memory()


def open_local(data_folder_path: str) -> ModelarDB:
    """Create a :obj:`ModelarDB` data folder that manages data in the local folder at `data_folder_path`.

    :param data_folder_path: The path of the data folder.
    :type data_folder_path: str
    :return: The constructed :obj:`ModelarDB`.
    :rtype: ModelarDB
    """
    return ModelarDB.open_local(data_folder_path)


def open_s3(
    endpoint: str, bucket_name: str, access_key_id: str, secret_access_key: str
) -> ModelarDB:
    """Create a :obj:`ModelarDB` data folder that manages data in an object store with a S3-compatible API.

    :param endpoint: The endpoint of the S3-compatible object store.
    :type endpoint: str
    :param bucket_name: The name of the bucket to read data from and write data to.
    :type bucket_name: str
    :param access_key_id: The access key id to use for authentication.
    :type access_key_id: str
    :param secret_access_key: The secret access key to use for authentication.
    :type secret_access_key: str
    :return: The constructed :obj:`ModelarDB`.
    :rtype: ModelarDB
    """
    return ModelarDB.open_s3(endpoint, bucket_name, access_key_id, secret_access_key)


def open_azure(account_name: str, access_key: str, container_name: str) -> ModelarDB:
    """Create a :obj:`ModelarDB` data folder that manages data in an object store with an Azure-compatible API.

    :param account_name: The account name to use for authentication.
    :type account_name: str
    :param access_key: The secret access key to use for authentication.
    :type access_key: str
    :param container_name: The name of the container to read data from and write data to.
    :type container_name: str
    :return: The constructed :obj:`ModelarDB`.
    :rtype: ModelarDB
    """
    return ModelarDB.open_azure(account_name, access_key, container_name)


def connect(node: Server | Manager):
    """Create a connection to a :obj:`ModelarDB` node.

    :param node: The ModelarDB node to connect to.
    :type node: Server | Manager
    """
    return ModelarDB.connect(node)
