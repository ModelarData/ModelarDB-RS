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

//! Implementation of the Apache Arrow DataFusion operator [`SortedJoinExec`] and
//! [`SortedJoinStream`] which joins multiple sorted array produced by [`GridExecs`](GridExec) and
//! combines them with the time series tags retrieved from the
//! [`MetadataManager`](crate::metadata::MetadataManager) to create the complete results containing
//! a timestamp column, one or more field columns, and zero or more tag columns.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::stream::Stream;

#[derive(Debug)]
pub struct SortedJoinExec {}

impl ExecutionPlan for SortedJoinExec {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<(dyn ExecutionPlan)>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<(dyn ExecutionPlan)>>,
    ) -> Result<Arc<(dyn ExecutionPlan)>> {
        todo!()
    }

    fn execute(
        &self,
        _partition: usize,
        _task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct SortedJoinStream {}

impl Stream for SortedJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut StdTaskContext<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

impl RecordBatchStream for SortedJoinStream {
    fn schema(&self) -> SchemaRef {
        todo!()
    }
}
