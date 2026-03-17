/* Copyright 2026 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* C header for the C-API of modelardb_embedded. Apache Arrow's C Data Interface
 * is used to pass data between the caller and modelardb_embedded. The required
 * struct definitions from the Arrow C Data Interface are included below. The full
 * definitions can be found at https://arrow.apache.org/docs/format/CDataInterface.html
 * and are licensed under the Apache License, Version 2.0.
 */

#pragma once
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Apache Arrow C Data Interface (from https://arrow.apache.org/docs/format/CDataInterface.html).
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description.
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback.
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data.
  void* private_data;
};

struct ArrowArray {
  // Array data description.
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback.
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data.
  void* private_data;
};

#endif // ARROW_C_DATA_INTERFACE

// Return code to use if no errors occurred.
extern int RETURN_SUCCESS;

// Return code to use if an error occurred.
extern int RETURN_FAILURE;

// Open a data folder that manages data in memory.
void* modelardb_embedded_open_memory();

// Open a data folder that manages data in a local folder.
void* modelardb_embedded_open_local(const char* data_folder_path_ptr);

// Open a data folder that manages data in an S3-compatible object store.
void* modelardb_embedded_open_s3(const char* endpoint_ptr,
                                 const char* bucket_name_ptr,
                                 const char* access_key_id_ptr,
                                 const char* secret_access_key_ptr);

// Open a data folder that manages data in an Azure-compatible object store.
void* modelardb_embedded_open_azure(const char* account_name_ptr,
                                    const char* access_key_ptr,
                                    const char* container_name_ptr);

// Connect to a ModelarDB server at the given Arrow Flight URL.
void* modelardb_embedded_connect(const char* url_ptr);

// Close and deallocate the data folder or client.
int modelardb_embedded_close(void* maybe_operations_ptr,
                             bool is_data_folder);

// Return the ModelarDB type of the data folder or client.
int modelardb_embedded_modelardb_type(void* maybe_operations_ptr,
                                      bool is_data_folder,
                                      int* modelardb_type_ptr);

// Create a table with the given name, schema, error bounds, and generated columns.
int modelardb_embedded_create(void* maybe_operations_ptr,
                              bool is_data_folder,
                              const char* table_name_ptr,
                              bool is_time_series_table,
                              struct ArrowSchema* schema_ptr,
                              struct ArrowArray* error_bounds_array_ptr,
                              struct ArrowSchema* error_bounds_array_schema_ptr,
                              struct ArrowArray* generated_columns_array_ptr,
                              struct ArrowSchema* generated_columns_array_schema_ptr);

// Return the names of all tables.
int modelardb_embedded_tables(void* maybe_operations_ptr,
                              bool is_data_folder,
                              struct ArrowArray* tables_array_ptr,
                              struct ArrowSchema* tables_array_schema_ptr);

// Return the schema of the table with the given name.
int modelardb_embedded_schema(void* maybe_operations_ptr,
                              bool is_data_folder,
                              const char* table_name_ptr,
                              struct ArrowArray* schema_struct_array_ptr,
                              struct ArrowSchema* schema_struct_array_schema_ptr);

// Write data to the table with the given name.
int modelardb_embedded_write(void* maybe_operations_ptr,
                             bool is_data_folder,
                             const char* table_name_ptr,
                             struct ArrowArray* uncompressed_struct_array_ptr,
                             struct ArrowSchema* uncompressed_struct_array_schema_ptr);

// Execute SQL and return the result.
int modelardb_embedded_read(void* maybe_operations_ptr,
                            bool is_data_folder,
                            const char* sql_ptr,
                            struct ArrowArray* decompressed_struct_array_ptr,
                            struct ArrowSchema* decompressed_struct_array_schema_ptr);

// Execute SQL and copy the result to a target table.
int modelardb_embedded_copy(void* maybe_source_operations_ptr,
                            bool is_data_folder,
                            const char* sql_ptr,
                            void* maybe_target_operations_ptr,
                            const char* target_table_name_ptr);

// Read data from a time series table with optional filters.
int modelardb_embedded_read_time_series_table(void* maybe_operations_ptr,
                                              bool is_data_folder,
                                              const char* table_name_ptr,
                                              struct ArrowArray* columns_array_ptr,
                                              struct ArrowSchema* columns_array_schema_ptr,
                                              struct ArrowArray* group_by_array_ptr,
                                              struct ArrowSchema* group_by_array_schema_ptr,
                                              const char* start_time_ptr,
                                              const char* end_time_ptr,
                                              struct ArrowArray* tags_array_ptr,
                                              struct ArrowSchema* tags_array_schema_ptr,
                                              struct ArrowArray* decompressed_struct_array_ptr,
                                              struct ArrowSchema* decompressed_struct_array_schema_ptr);

// Copy data between time series tables with optional filters.
int modelardb_embedded_copy_time_series_table(void* maybe_source_operations_ptr,
                                              bool is_data_folder,
                                              const char* source_table_name_ptr,
                                              void* maybe_target_operations_ptr,
                                              const char* target_table_name_ptr,
                                              const char* start_time_ptr,
                                              const char* end_time_ptr,
                                              struct ArrowArray* tags_array_ptr,
                                              struct ArrowSchema* tags_array_schema_ptr);

// Move all data from a source table to a target table.
int modelardb_embedded_move(void* maybe_source_operations_ptr,
                            bool is_data_folder,
                            const char* source_table_name_ptr,
                            void* maybe_target_operations_ptr,
                            const char* target_table_name_ptr);

// Truncate the table with the given name.
int modelardb_embedded_truncate(void* maybe_operations_ptr,
                                bool is_data_folder,
                                const char* table_name_ptr);

// Drop the table with the given name.
int modelardb_embedded_drop(void* maybe_operations_ptr,
                            bool is_data_folder,
                            const char* table_name_ptr);

// Vacuum the table by deleting stale data older than the retention period.
int modelardb_embedded_vacuum(void* maybe_operations_ptr,
                              bool is_data_folder,
                              const char* table_name_ptr,
                              const char* retention_period_in_seconds_ptr);

// Return a human-readable representation of the last error on this thread.
const char* modelardb_embedded_error();

#ifdef __cplusplus
} // extern "C"
#endif
