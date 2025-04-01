/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of [`TimeSeriesTableMetadata`](crate::TimeSeriesTableMetadata) which contains
//! metadata required to interact with time series tables and
//! [`TableMetadataManager`](table_metadata_manager::TableMetadataManager) which provides
//! functionality to access table related metadata in the metadata Delta Lake.

pub mod table_metadata_manager;
pub mod time_series_table_metadata;
