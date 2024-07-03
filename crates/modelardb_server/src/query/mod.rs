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

//! Implementation of [`ModelTable`] which allows model tables to be queried through Apache Arrow
//! DataFusion. It takes the projection, filters as [`Exprs`](Expr), and limit of a query as input
//! and returns a physical query plan that produces all the data points required for the query.

use std::sync::Arc;

use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use deltalake::arrow::compute::SortOptions;
use once_cell::sync::Lazy;

// Public so the rules added to Apache Arrow DataFusion's physical optimizer can access GridExec.
pub mod data_sinks;
pub mod generated_as_exec;
pub mod grid_exec;
pub mod model_table;
pub mod sorted_join_exec;
pub mod table;

/// The global sort order [`ParquetExec`] guarantees for the segments it produces and that
/// [`GridExec`] requires for the segments its receives as its input. It is guaranteed by
/// [`ParquetExec`] because the storage engine uses this sort order for each Apache Parquet file and
/// these files are read sequentially by [`ParquetExec`]. Another sort order could also be used, the
/// current query pipeline simply requires that the
/// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) [`SortedJoinExec`] receive from
/// its inputs all contain data points for the same time interval and that they are sorted the same.
static QUERY_ORDER_SEGMENT: Lazy<Vec<PhysicalSortExpr>> = Lazy::new(|| {
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

/// The global sort order [`GridExec`] guarantees for the data points it produces and that
/// [`SortedJoinExec`] requires for the data points it receives as its input. It is guaranteed by
/// [`GridExec`] because it receives segments sorted by [`QUERY_ORDER_SEGMENT`] from [`ParquetExec`]
/// and because these segments cannot contain data points for overlapping time intervals. Another
/// sort order could also be used, the current query pipeline simply requires that the
/// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) [`SortedJoinExec`] receive from
/// its inputs all contain data points for the same time interval and that they are sorted the same.
static QUERY_ORDER_DATA_POINT: Lazy<Vec<PhysicalSortExpr>> = Lazy::new(|| {
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
