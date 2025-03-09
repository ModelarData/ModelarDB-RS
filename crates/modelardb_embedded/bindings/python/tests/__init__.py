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

import datetime
import os
import unittest
from tempfile import TemporaryDirectory

import modelardbe
import pyarrow
from modelardbe import (
    AbsoluteErrorBound,
    ModelTable,
    NormalTable,
    ModelarDB,
    FFIArray,
    RelativeErrorBound,
)
from pyarrow import Int64Array, RecordBatch, Schema
from pyarrow.cffi import ffi

NORMAL_TABLE_NAME = "normal_table"
MODEL_TABLE_NAME = "model_table"
MISSING_TABLE_NAME = "missing_table"


class ModelarDBEmbeddedPythonTest(unittest.TestCase):
    # Tests for AbsoluteErrorBound.
    def test_can_create_absolute_error_bound_with_float_zero(self):
        _ = AbsoluteErrorBound(0)

    def test_can_create_absolute_error_bound_with_normal_positive_float(self):
        _ = AbsoluteErrorBound(1)

    def test_cannot_create_absolute_error_bound_with_normal_negative_float(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(-1))

    def test_cannot_create_absolute_error_bound_with_negative_infinity(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(float("-inf")))

    def test_cannot_create_absolute_error_bound_with_positive_infinity(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(float("inf")))

    def test_cannot_create_absolute_error_bound_with_nan(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(float("nan")))

    # Tests for RelativeErrorBound.
    def test_can_create_relative_error_bound_with_float_zero(self):
        _ = RelativeErrorBound(0)

    def test_can_create_relative_error_bound_with_normal_positive_float(self):
        _ = RelativeErrorBound(1)

    def test_cannot_create_relative_error_bound_with_normal_negative_float(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(-1))

    def test_cannot_create_relative_error_bound_with_float_above_one_hundred(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(101))

    def test_cannot_create_relative_error_bound_with_negative_infinity(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(float("-inf")))

    def test_cannot_create_relative_error_bound_with_positive_infinity(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(float("inf")))

    def test_cannot_create_relative_error_bound_with_nan(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(float("nan")))

    # Tests for ModelTable.
    def test_cannot_specify_error_bound_for_missing_columns(self):
        schema = model_table_query_schema()
        self.assertRaises(
            ValueError,
            lambda: ModelTable(
                schema, {"field_that_does_not_exist": AbsoluteErrorBound(0)}
            ),
        )

    def test_cannot_specify_generated_column_for_missing_columns(self):
        schema = model_table_query_schema()
        self.assertRaises(
            ValueError,
            lambda: ModelTable(
                schema, {}, {"field_that_does_not_exist": "field_one + field_two"}
            ),
        )

    # Tests for FFIArray.
    def test_can_create_ffiarray_from_array(self):
        array = pyarrow.array([1, 2, 3, 4, 5])
        array_ffi = FFIArray.from_array(array)
        self.assertEqual(array, array_ffi.array())

    def test_can_create_ffiarray_from_array_type_array_ptr_and_schema_ptr(self):
        array = pyarrow.array([1, 2, 3, 4, 5])
        array_ptr = ffi.new("struct ArrowArray*")
        schema_ptr = ffi.new("struct ArrowSchema*")
        array_ptr_int = int(ffi.cast("uintptr_t", array_ptr))
        schema_ptr_int = int(ffi.cast("uintptr_t", schema_ptr))
        array._export_to_c(array_ptr_int, schema_ptr_int)

        array_ffi = FFIArray.from_ffi(
            Int64Array, array_ptr=array_ptr, schema_ptr=schema_ptr
        )

        self.assertEqual(array, array_ffi.array())

    def test_can_create_ffiarray_from_array_type(self):
        _ = FFIArray.from_type(Int64Array)

    # Tests for DataFolder.
    def test_data_folder_init(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            new_data_folder = ModelarDB.open_local(temp_dir)
            self.assertEqual(data_folder.tables(), new_data_folder.tables())

    def test_data_folder_create_normal_table(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            table_folder = os.path.join(temp_dir, "tables", NORMAL_TABLE_NAME)
            self.assertFalse(os.path.exists(table_folder))

            data_folder.create(NORMAL_TABLE_NAME, NormalTable(normal_table_schema()))

            self.assertTrue(os.path.exists(table_folder))
            self.assertEqual(len(os.listdir(table_folder)), 1)

    def test_data_folder_create_model_table(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            expected_schema = model_table_query_schema()
            model_table_type = modelardbe.ModelTable(
                expected_schema,
                {
                    "field_one": modelardbe.AbsoluteErrorBound(1),
                    "field_two": modelardbe.RelativeErrorBound(10),
                },
                {"field_three": "field_one + field_two"},
            )

            model_table_folder = os.path.join(temp_dir, "tables", MODEL_TABLE_NAME)
            self.assertFalse(os.path.exists(model_table_folder))

            data_folder.create(MODEL_TABLE_NAME, model_table_type)

            self.assertTrue(os.path.exists(model_table_folder))
            self.assertEqual(len(os.listdir(model_table_folder)), 1)

    def test_data_folder_create_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.create(NORMAL_TABLE_NAME, NormalTable(pyarrow.schema([])))

            error_message = (
                "ModelarDB Storage Error: Delta Lake Error: Generic error: At least one column must be "
                "defined to create a table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_tables(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            self.assertEqual(
                data_folder.tables(), [NORMAL_TABLE_NAME, MODEL_TABLE_NAME]
            )

    def test_data_folder_schema(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            actual_normal_table_schema = data_folder.schema(NORMAL_TABLE_NAME)
            self.assertEqual(actual_normal_table_schema, normal_table_schema())

            actual_model_table_schema = data_folder.schema(MODEL_TABLE_NAME)
            self.assertEqual(actual_model_table_schema, model_table_query_schema())

    def test_data_folder_schema_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.schema(MISSING_TABLE_NAME)

            error_message = (
                f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_write(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            model_table_folder = os.path.join(temp_dir, "tables", MODEL_TABLE_NAME)
            self.assertEqual(len(os.listdir(model_table_folder)), 1)

            data_folder.write(MODEL_TABLE_NAME, model_table_data())
            self.assertEqual(len(os.listdir(model_table_folder)), 3)

    def test_data_folder_write_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.write(MISSING_TABLE_NAME, model_table_data())

            error_message = (
                f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_read_model_table(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(MODEL_TABLE_NAME, model_table_data())

            actual_result = data_folder.read_model_table(MODEL_TABLE_NAME)
            self.assertEqual(
                actual_result, sorted_model_table_data_with_generated_column()
            )

    def test_data_folder_read_model_table_with_columns_tags_timestamps(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(MODEL_TABLE_NAME, model_table_data())

            end_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
            actual_result = data_folder.read_model_table(
                MODEL_TABLE_NAME,
                columns=["tag", "field_one"],
                tags={"tag": "tag_one"},
                start_time="1970-01-01T00:00:00.000150",
                end_time=end_time + datetime.timedelta(microseconds=250),
            )
            expected_result = sorted_model_table_data_with_generated_column()
            self.assertEqual(
                actual_result, expected_result.select(["tag", "field_one"]).slice(1, 1)
            )

    def test_data_folder_read_model_table_with_aggregates(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(MODEL_TABLE_NAME, model_table_data())

            columns = [
                ("tag", modelardbe.Aggregate.NONE),
                ("field_one", modelardbe.Aggregate.SUM),
            ]
            actual_result = data_folder.read_model_table(
                MODEL_TABLE_NAME, columns, ["tag"]
            )

            expected_result = [
                {"tag": "tag_one", f"sum({MODEL_TABLE_NAME}.field_one)": 111.0},
                {"tag": "tag_two", f"sum({MODEL_TABLE_NAME}.field_one)": 219.0},
            ]
            self.assertEqual(
                sorted(actual_result.to_pylist(), key=lambda x: x["tag"]),
                expected_result,
            )

    def test_data_folder_read_model_table_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.read_model_table(MISSING_TABLE_NAME)

            error_message = (
                f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a model table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_copy_model_table(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                create_tables_in_data_folder(from_data_folder)

                from_data_folder.write(MODEL_TABLE_NAME, model_table_data())

                to_data_folder = ModelarDB.open_local(to_temp_dir)
                create_tables_in_data_folder(to_data_folder)

                from_data_folder.copy_model_table(
                    MODEL_TABLE_NAME, to_data_folder, MODEL_TABLE_NAME
                )

                # After copying the data it should also be in to_table.
                expected_result = sorted_model_table_data_with_generated_column()
                self.assertEqual(
                    from_data_folder.read_model_table(MODEL_TABLE_NAME), expected_result
                )
                self.assertEqual(
                    to_data_folder.read_model_table(MODEL_TABLE_NAME), expected_result
                )

    def test_data_folder_copy_model_table_with_timestamps(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                create_tables_in_data_folder(from_data_folder)

                # Force the physical data to have multiple segments by writing the data in three parts.
                for i in range(3):
                    from_data_folder.write(
                        MODEL_TABLE_NAME, model_table_data().slice(i * 2, 2)
                    )

                to_data_folder = ModelarDB.open_local(to_temp_dir)
                create_tables_in_data_folder(to_data_folder)

                end_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
                from_data_folder.copy_model_table(
                    MODEL_TABLE_NAME,
                    to_data_folder,
                    MODEL_TABLE_NAME,
                    start_time="1970-01-01T00:00:00.000150",
                    end_time=end_time + datetime.timedelta(microseconds=250),
                )

                # After copying the data it should also be in to_table.
                from_expected_result = model_table_data_with_generated_column()
                self.assertEqual(
                    from_data_folder.read_model_table(MODEL_TABLE_NAME),
                    from_expected_result,
                )

                to_expected_result = from_expected_result.slice(2, 2)
                self.assertEqual(
                    to_data_folder.read_model_table(MODEL_TABLE_NAME),
                    to_expected_result,
                )

    def test_data_folder_copy_model_table_error(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                to_data_folder = ModelarDB.open_local(to_temp_dir)

                with self.assertRaises(RuntimeError) as context:
                    from_data_folder.copy_model_table(
                        MISSING_TABLE_NAME, to_data_folder, MISSING_TABLE_NAME
                    )

                error_message = f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a model table."
                self.assertEqual(error_message, str(context.exception))

    def test_data_folder_read(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(MODEL_TABLE_NAME, model_table_data())

            actual_result = data_folder.read(f"SELECT * FROM {MODEL_TABLE_NAME}")
            self.assertEqual(
                actual_result, sorted_model_table_data_with_generated_column()
            )

    def test_data_folder_read_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.read(f"SELECT * FROM {MISSING_TABLE_NAME}")

            error_message = (
                f"DataFusion Error: Error during planning: table 'datafusion.public.{MISSING_TABLE_NAME}' "
                f"not found"
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_copy_normal_table(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                create_tables_in_data_folder(from_data_folder)

                expected_result = normal_table_data()
                from_data_folder.write(NORMAL_TABLE_NAME, expected_result)

                to_data_folder = ModelarDB.open_local(to_temp_dir)
                create_tables_in_data_folder(to_data_folder)

                sql = f"SELECT * FROM {NORMAL_TABLE_NAME}"
                from_data_folder.copy_normal_table(
                    sql, to_data_folder, NORMAL_TABLE_NAME
                )

                # After copying the data it should also be in to_table.
                self.assertEqual(from_data_folder.read(sql), expected_result)
                self.assertEqual(to_data_folder.read(sql), expected_result)

    def test_data_folder_copy_normal_table_error(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                to_data_folder = ModelarDB.open_local(to_temp_dir)

                with self.assertRaises(RuntimeError) as context:
                    sql = f"SELECT * FROM {MISSING_TABLE_NAME}"
                    from_data_folder.copy_normal_table(
                        sql, to_data_folder, MISSING_TABLE_NAME
                    )

                error_message = f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a normal table."
                self.assertEqual(error_message, str(context.exception))

    def test_data_folder_move(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                create_tables_in_data_folder(from_data_folder)

                from_data_folder.write(MODEL_TABLE_NAME, model_table_data())

                to_data_folder = ModelarDB.open_local(to_temp_dir)
                create_tables_in_data_folder(to_data_folder)

                from_data_folder.move(
                    MODEL_TABLE_NAME, to_data_folder, MODEL_TABLE_NAME
                )

                self.assertEqual(
                    from_data_folder.read_model_table(MODEL_TABLE_NAME).num_rows, 0
                )

                expected_result = sorted_model_table_data_with_generated_column()
                self.assertEqual(
                    to_data_folder.read_model_table(MODEL_TABLE_NAME), expected_result
                )

    def test_data_folder_move_error(self):
        with TemporaryDirectory() as from_temp_dir:
            with TemporaryDirectory() as to_temp_dir:
                from_data_folder = ModelarDB.open_local(from_temp_dir)
                to_data_folder = ModelarDB.open_local(to_temp_dir)

                with self.assertRaises(RuntimeError) as context:
                    from_data_folder.move(
                        MISSING_TABLE_NAME, to_data_folder, MISSING_TABLE_NAME
                    )

                error_message = (
                    f"Invalid Argument Error: {MISSING_TABLE_NAME} and {MISSING_TABLE_NAME} are not both "
                    f"normal tables or model tables."
                )
                self.assertEqual(error_message, str(context.exception))

    def test_data_folder_drop(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)
            self.assertTrue(MODEL_TABLE_NAME in data_folder.tables())

            data_folder.drop(MODEL_TABLE_NAME)

            self.assertFalse(
                os.path.exists(os.path.join(temp_dir, "tables", MODEL_TABLE_NAME))
            )
            self.assertFalse(MODEL_TABLE_NAME in data_folder.tables())

    def test_data_folder_drop_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.drop(MISSING_TABLE_NAME)

            error_message = (
                f"ModelarDB Storage Error: Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' "
                f"does not exist."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_truncate(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(MODEL_TABLE_NAME, model_table_data())

            data_folder.truncate(MODEL_TABLE_NAME)

            self.assertTrue(MODEL_TABLE_NAME in data_folder.tables())
            self.assertEqual(data_folder.read_model_table(MODEL_TABLE_NAME).num_rows, 0)

    def test_data_folder_truncate_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = ModelarDB.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.truncate(MISSING_TABLE_NAME)

            error_message = f"Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' does not exist."
            self.assertEqual(error_message, str(context.exception))


def create_tables_in_data_folder(data_folder: ModelarDB):
    table_type = NormalTable(normal_table_schema())
    data_folder.create(NORMAL_TABLE_NAME, table_type)

    model_table_type = modelardbe.ModelTable(
        model_table_query_schema(),
        {
            "field_one": modelardbe.AbsoluteErrorBound(1),
            "field_two": modelardbe.RelativeErrorBound(10),
        },
        {"field_three": "field_one + field_two"},
    )
    data_folder.create(MODEL_TABLE_NAME, model_table_type)


def normal_table_schema() -> Schema:
    return pyarrow.schema(
        [
            ("id", pyarrow.int32()),
            ("name", pyarrow.string()),
            ("age", pyarrow.int8()),
            ("height", pyarrow.int16()),
            ("weight", pyarrow.int16()),
        ]
    )


def normal_table_data() -> RecordBatch:
    schema = normal_table_schema()
    return pyarrow.RecordBatch.from_pylist(
        [{"id": 1, "name": "Mister Smith", "age": 25, "height": 193, "weight": 95}],
        schema,
    )


def model_table_schema() -> Schema:
    return pyarrow.schema(
        [
            ("timestamp", pyarrow.timestamp("us")),
            ("tag", pyarrow.string()),
            ("field_one", pyarrow.float32()),
            ("field_two", pyarrow.float32()),
        ]
    )


def model_table_query_schema() -> Schema:
    return pyarrow.schema(
        [
            ("timestamp", pyarrow.timestamp("us")),
            ("tag", pyarrow.string()),
            ("field_one", pyarrow.float32()),
            ("field_two", pyarrow.float32()),
            ("field_three", pyarrow.float32()),
        ]
    )


def sorted_model_table_data_with_generated_column() -> RecordBatch:
    unsorted_data = model_table_data_with_generated_column()
    return unsorted_data.sort_by([("tag", "ascending"), ("timestamp", "ascending")])


def model_table_data_with_generated_column() -> RecordBatch:
    data = model_table_data()

    generated_column_array = pyarrow.array(
        [74, 146, 74, 146, 74, 146], pyarrow.float32()
    )
    return data.append_column("field_three", generated_column_array)


def model_table_data() -> RecordBatch:
    return pyarrow.RecordBatch.from_pylist(
        [
            {
                "timestamp": 100,
                "field_one": 37.0,
                "field_two": 37.0,
                "tag": "tag_one",
            },
            {
                "timestamp": 100,
                "field_one": 73.0,
                "field_two": 73.0,
                "tag": "tag_two",
            },
            {
                "timestamp": 200,
                "field_one": 37.0,
                "field_two": 37.0,
                "tag": "tag_one",
            },
            {
                "timestamp": 200,
                "field_one": 73.0,
                "field_two": 73.0,
                "tag": "tag_two",
            },
            {
                "timestamp": 300,
                "field_one": 37.0,
                "field_two": 37.0,
                "tag": "tag_one",
            },
            {
                "timestamp": 300,
                "field_one": 73.0,
                "field_two": 73.0,
                "tag": "tag_two",
            },
        ],
        model_table_schema(),
    )
