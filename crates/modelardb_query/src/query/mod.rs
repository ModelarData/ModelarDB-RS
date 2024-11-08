/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of types which allows both normal tables and model tables to be added to Apache
//! DataFusion. This allows them to be queried and small amounts of data to be added with INSERT.

use std::sync::{Arc, LazyLock};

use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use deltalake::arrow::compute::SortOptions;

// grid_exec and sorted_join_exec are pub(crate) so the rules added to Apache DataFusion's physical
// optimizer can access them.
mod generated_as_exec;
pub(crate) mod grid_exec;
pub(crate) mod model_table;
pub(crate) mod sorted_join_exec;
pub(crate) mod normal_table;

/// The global sort order [`datafusion::datasource::physical_plan::parquet::ParquetExec`] guarantees
/// for the segments it produces and that [`grid_exec::GridExec`] requires for the segments its
/// receives as its input. It is guaranteed by
/// [`datafusion::datasource::physical_plan::parquet::ParquetExec`] because the storage engine uses
/// this sort order for each Apache Parquet file and these files are read sequentially by
/// [`datafusion::datasource::physical_plan::parquet::ParquetExec`]. Another sort order could also
/// be used, the current query pipeline simply requires that the
/// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch)
/// [`sorted_join_exec::SortedJoinExec`] receives from its inputs all contain data points for the
/// same time interval and that they are sorted the same.
static QUERY_ORDER_SEGMENT: LazyLock<Vec<PhysicalSortExpr>> = LazyLock::new(|| {
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("univariate_id", 0)),
            options: sort_options,
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("start_time", 2)),
            options: sort_options,
        },
    ]
});

/// The global sort order [`grid_exec::GridExec`] guarantees for the data points it produces and
/// that [`sorted_join_exec::SortedJoinExec`] requires for the data points it receives as its input.
/// It is guaranteed by [`grid_exec::GridExec`] because it receives segments sorted by
/// [`QUERY_ORDER_SEGMENT`] from [`datafusion::datasource::physical_plan::parquet::ParquetExec`] and
/// because these segments cannot contain data points for overlapping time intervals. Another sort
/// order could also be used, the current query pipeline simply requires that the
/// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch)
/// [`sorted_join_exec::SortedJoinExec`] receives from its inputs all contain data points for the
/// same time interval and that they are sorted the same.
static QUERY_ORDER_DATA_POINT: LazyLock<Vec<PhysicalSortExpr>> = LazyLock::new(|| {
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("univariate_id", 0)),
            options: sort_options,
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("timestamp", 1)),
            options: sort_options,
        },
    ]
});
