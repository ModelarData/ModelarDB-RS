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

//! Implementation of a physical optimizer rule that rewrites aggregates that are computed from
//! reconstructed values from a single column without filtering so they are computed directly from
//! segments instead of the reconstructed values.

use std::any::Any;
use std::cmp::PartialEq;
use std::fmt::{Display, Formatter};
use std::mem;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Float32Array, Int64Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::expressions::{self, Avg, Count, Max, Min, Sum};
use datafusion::physical_plan::file_format::ParquetExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    Accumulator, AggregateExpr, ColumnarValue, ExecutionPlan, PhysicalExpr,
};
use datafusion::scalar::ScalarValue;
use modelardb_common::types::{ArrowValue, TimestampArray, Value, ValueArray};
use modelardb_compression::models;

use crate::query::sorted_join_exec::SortedJoinExec;

/// Rewrite aggregates that are computed from reconstructed values from a single column without
/// filtering so they are computed directly from segments instead of the reconstructed values.
pub struct ModelSimpleAggregatesPhysicalOptimizerRule {}

impl PhysicalOptimizerRule for ModelSimpleAggregatesPhysicalOptimizerRule {
    /// Rewrite `execution_plan` so it computes simple aggregates without filtering from segments
    /// instead of reconstructed values.
    fn optimize(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        execution_plan.transform_down(&rewrite_aggregates_to_use_segments)
    }

    /// Return the name of the [`PhysicalOptimizerRule`].
    fn name(&self) -> &str {
        "ModelSimpleAggregatesPhysicalOptimizerRule"
    }

    /// Specify if the physical planner should validate that the rule does not change the schema.
    fn schema_check(&self) -> bool {
        true
    }
}

/// Matches the pattern [`AggregateExpr`] <- [`SortedJoinExec`] <-
/// [`GridExec`](crate::query::grid_exec::GridExec) <- [`ParquetExec`] and rewrites it to
/// [`AggregateExpr`] <- [`ParquetExec`] if [`AggregateExpr`] only computes aggregates for a single
/// FIELD column and no filtering is performed by [`ParquetExec`].
fn rewrite_aggregates_to_use_segments(
    execution_plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // The rule tries to match the sub-tree of execution_plan so execution_plan can be updated.
    let execution_plan_children = execution_plan.children();

    if execution_plan_children.len() == 1 {
        if let Some(aggregate_exec) = execution_plan_children[0]
            .as_any()
            .downcast_ref::<AggregateExec>()
        {
            // Currently, only aggregates on one FIELD column without predicates are supported.
            let aggregate_exec_children = aggregate_exec.children();
            if aggregate_exec.input_schema().fields.len() == 1
                && *aggregate_exec.input_schema().field(0).data_type() == ArrowValue::DATA_TYPE
                && aggregate_exec.filter_expr().iter().all(Option::is_none)
                && aggregate_exec.group_expr().is_empty()
            {
                // An AggregateExec can only have one child, so it is not necessary to check it.
                let aggregate_exec_input = aggregate_exec_children[0].clone();
                if let Some(sorted_join_exec) = aggregate_exec_input
                    .as_any()
                    .downcast_ref::<SortedJoinExec>()
                {
                    // Remove RepartitionExec if added by Apache Arrow DataFusion
                    let sorted_join_exec_children = sorted_join_exec.children();
                    let grid_execs = if sorted_join_exec_children[0]
                        .as_any()
                        .is::<RepartitionExec>()
                    {
                        sorted_join_exec_children
                            .iter()
                            .flat_map(|repartition_exec| repartition_exec.children())
                            .collect::<Vec<Arc<dyn ExecutionPlan>>>()
                    } else {
                        sorted_join_exec_children
                    };

                    // Try to create new AggregateExec that compute aggregates directly from segments.
                    if let Ok(input) = try_new_aggregate_exec(aggregate_exec, grid_execs) {
                        // unwrap() is safe as the inputs are constructed from sorted_join_exec.
                        return Ok(Transformed::Yes(
                            execution_plan.with_new_children(vec![input]).unwrap(),
                        ));
                    };
                }
            }
        }
    }

    Ok(Transformed::No(execution_plan))
}

/// Return an [`AggregateExec`] that computes the same aggregates as `aggregate_exec` if no
/// predicates have been pushed to `inputs` and the aggregates are all computed for the same FIELD
/// column, otherwise [`DataFusionError`] is returned.
fn try_new_aggregate_exec(
    aggregate_exec: &AggregateExec,
    grid_execs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // aggregate_exec.input_schema().fields.len() == 1 should prevent this, but SortedJoinExec can
    // be constructed with multiple inputs so the number of children is checked just to be sure.
    if grid_execs.len() > 1 {
        return Err(DataFusionError::NotImplemented(
            "All aggregates must be for the same FIELD column.".to_owned(),
        ));
    }

    // A GridExec can only have one child, so it is not necessary to check it.
    let grid_exec_child = grid_execs[0].children().remove(0);
    if let Some(parquet_exec) = grid_exec_child.as_any().downcast_ref::<ParquetExec>() {
        if parquet_exec.predicate().is_some() || parquet_exec.pruning_predicate().is_some() {
            return Err(DataFusionError::NotImplemented(
                "Predicates cannot be pushed before the aggregate.".to_owned(),
            ));
        }
    } else {
        return Err(DataFusionError::Plan(
            "The input to GridExec must be a ParquetExec.".to_owned(),
        ));
    }

    let model_based_aggregate_exprs = try_rewrite_aggregate_exprs(aggregate_exec)?;

    // unwrap() is safe as the input is from the existing AggregateExec.
    Ok(Arc::new(
        AggregateExec::try_new(
            *aggregate_exec.mode(),
            aggregate_exec.group_expr().clone(),
            model_based_aggregate_exprs,
            aggregate_exec.filter_expr().to_vec(),
            grid_execs[0].children().remove(0),
            aggregate_exec.schema(),
        )
        .unwrap(),
    ))
}

/// Return [`AggregateExprs`](AggregateExpr) that computes the same aggregates as `aggregate_exec`
/// if the aggregates are all computed for the same FIELD column, otherwise [`DataFusionError`] is
/// returned.
fn try_rewrite_aggregate_exprs(
    aggregate_exec: &AggregateExec,
) -> Result<Vec<Arc<dyn AggregateExpr>>> {
    let aggregate_exprs = aggregate_exec.aggr_expr();
    let mut rewritten_aggregate_exprs: Vec<Arc<dyn AggregateExpr>> =
        Vec::with_capacity(aggregate_exprs.len());

    for aggregate_expr in aggregate_exprs {
        if aggregate_expr.as_any().is::<Count>() {
            rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Count));
        } else if aggregate_expr.as_any().is::<Min>() {
            rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Min));
        } else if aggregate_expr.as_any().is::<Max>() {
            rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Max));
        } else if aggregate_expr.as_any().is::<Sum>() {
            rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Sum));
        } else if aggregate_expr.as_any().is::<Avg>() {
            rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Avg));
        } else {
            return Err(DataFusionError::Internal(format!(
                "Aggregate expression {} is currently not supported.",
                aggregate_expr.name()
            )));
        }
    }

    Ok(rewritten_aggregate_exprs)
}

/// Model-based aggregates supported by [`ModelAggregateExpr`].
#[derive(Debug, PartialEq)]
enum ModelAggregateType {
    Count,
    Min,
    Max,
    Sum,
    Avg,
}

/// Generic [`AggregateExpr`] for computing the model-based aggregates in [`ModelAggregateType`].
#[derive(Debug)]
pub struct ModelAggregateExpr {
    /// Name of the model-based aggregate.
    name: String,
    /// The model-based aggregate to compute.
    aggregate_type: ModelAggregateType,
    /// Return type of the model-based aggregate.
    data_type: DataType,
}

impl ModelAggregateExpr {
    fn new(aggregate_type: ModelAggregateType) -> Arc<Self> {
        let data_type = match &aggregate_type {
            ModelAggregateType::Count => DataType::Int64,
            ModelAggregateType::Min => DataType::Float32,
            ModelAggregateType::Max => DataType::Float32,
            ModelAggregateType::Sum => DataType::Float32,
            ModelAggregateType::Avg => DataType::Float32,
        };

        Arc::new(Self {
            name: format!("Model{aggregate_type:?}AggregateExpr"),
            aggregate_type,
            data_type,
        })
    }
}

/// Enable equal and not equal for [`Any`] and [`ModelAggregateExpr`].
impl PartialEq<dyn Any> for ModelAggregateExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other_model_based_aggregate_expr) = other.downcast_ref::<ModelAggregateExpr>() {
            self.name == other_model_based_aggregate_expr.name
                && self.aggregate_type == other_model_based_aggregate_expr.aggregate_type
                && self.data_type == other_model_based_aggregate_expr.data_type
        } else {
            false
        }
    }
}

impl AggregateExpr for ModelAggregateExpr {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return a [`Field`] that specifies the name and [`DataType`] of the final aggregate.
    fn field(&self) -> Result<Field> {
        Ok(Field::new(self.name(), self.data_type.clone(), false))
    }

    /// Return [`Fields`](Field) that specify the names and [`DataTypes`](DataType) of the
    /// [`Accumulator's`](Accumulator) intermediate state.
    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(match &self.aggregate_type {
            ModelAggregateType::Sum => vec![
                Field::new("SUM", DataType::Float32, false),
                Field::new("COUNT", DataType::UInt64, false),
            ],
            ModelAggregateType::Avg => vec![
                Field::new("COUNT", DataType::UInt64, false),
                Field::new("SUM", DataType::Float32, false),
            ],
            _ => vec![Field::new(
                expressions::format_state_name(self.name(), "NOT NULL"),
                self.data_type.clone(),
                false,
            )],
        })
    }

    /// Return the [`PhysicalExpr`] that is passed to the [`Accumulator`](Accumulator).
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![match &self.aggregate_type {
            ModelAggregateType::Count => Arc::new(ModelCountPhysicalExpr {}),
            ModelAggregateType::Min => Arc::new(ModelMinPhysicalExpr {}),
            ModelAggregateType::Max => Arc::new(ModelMaxPhysicalExpr {}),
            ModelAggregateType::Sum => Arc::new(ModelSumPhysicalExpr {}),
            ModelAggregateType::Avg => Arc::new(ModelAvgPhysicalExpr {}),
        }]
    }

    /// Return the [`Accumulator`].
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(match self.aggregate_type {
            ModelAggregateType::Count => Box::new(ModelCountAccumulator { count: 0 }),
            ModelAggregateType::Min => Box::new(ModelMinAccumulator { min: f32::MAX }),
            ModelAggregateType::Max => Box::new(ModelMaxAccumulator { max: f32::MIN }),
            ModelAggregateType::Sum => Box::new(ModelSumAccumulator { sum: 0.0, count: 0 }),
            ModelAggregateType::Avg => Box::new(ModelAvgAccumulator { sum: 0.0, count: 0 }),
        })
    }

    /// Return the name of the [`AggregateExpr`].
    fn name(&self) -> &str {
        &self.name
    }
}

/// [`PhysicalExpr`] that computes `COUNT` directly from segments containing metadata and models.
#[derive(Debug)]
pub struct ModelCountPhysicalExpr {}

impl Display for ModelCountPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelCountPhysicalExpr(Int64)")
    }
}

/// Enable equal and not equal for [`Any`] and [`ModelCountPhysicalExpr`].
impl PartialEq<dyn Any> for ModelCountPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelCountPhysicalExpr {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the return [`DataType`] of this [`PhysicalExpr`].
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    /// Return [`false`] as this expression cannot return `NULL`.
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> Result<ColumnarValue> {
        modelardb_common::arrays!(
            record_batch,
            _univariate_ids,
            _model_type_ids,
            start_times,
            end_times,
            timestamps,
            _min_values,
            _max_values,
            _values,
            _residuals,
            _error_array
        );

        let mut count: i64 = 0;
        for row_index in 0..record_batch.num_rows() {
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);

            count += models::len(start_time, end_time, timestamps) as i64;
        }

        // Returning a Scalar fills an array with the value.
        let mut result = Int64Array::builder(1);
        result.append_value(count);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelCountPhysicalExpr does not support children.".to_owned(),
        ))
    }
}

/// [`Accumulator`] that accumulates `COUNT` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelCountAccumulator {
    /// Current count stored by the [`Accumulator`].
    count: i64,
}

impl Accumulator for ModelCountAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.count += array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    /// Panics as this method should never be called.
    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `MIN` directly from segments containing metadata and models.
#[derive(Debug)]
pub struct ModelMinPhysicalExpr {}

impl Display for ModelMinPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelMinPhysicalExpr(Float32)")
    }
}

/// Enable equal and not equal for [`Any`] and [`ModelMinPhysicalExpr`].
impl PartialEq<dyn Any> for ModelMinPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelMinPhysicalExpr {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the return [`DataType`] of this [`PhysicalExpr`].
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut min = Value::MAX;
        let min_value_array = modelardb_common::array!(record_batch, 5, ValueArray);
        for row_index in 0..record_batch.num_rows() {
            min = Value::min(min, min_value_array.value(row_index));
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(1);
        result.append_value(min);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelMinPhysicalExpr does not support children.".to_owned(),
        ))
    }
}

/// [`Accumulator`] that accumulates `MIN` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelMinAccumulator {
    /// Current minimum value stored by the [`Accumulator`].
    min: f32,
}

impl Accumulator for ModelMinAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.min = f32::min(
                self.min,
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(0),
            );
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float32(Some(self.min))])
    }

    /// Panics as this method should never be called.
    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `MAX` directly from segments containing metadata and models.
#[derive(Debug)]
pub struct ModelMaxPhysicalExpr {}

impl Display for ModelMaxPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelMaxPhysicalExpr(Float32)")
    }
}

/// Enable equal and not equal for [`Any`] and [`ModelMaxPhysicalExpr`].
impl PartialEq<dyn Any> for ModelMaxPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelMaxPhysicalExpr {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the return [`DataType`] of this [`PhysicalExpr`].
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut max = Value::MIN;
        let max_value_array = modelardb_common::array!(record_batch, 6, ValueArray);
        for row_index in 0..record_batch.num_rows() {
            max = Value::max(max, max_value_array.value(row_index));
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(1);
        result.append_value(max);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelMaxPhysicalExpr does not support children.".to_owned(),
        ))
    }
}

/// [`Accumulator`] that accumulates `MAX` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelMaxAccumulator {
    /// Current maximum value stored by the [`Accumulator`].
    max: f32,
}

impl Accumulator for ModelMaxAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.max = f32::max(
                self.max,
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(0),
            );
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float32(Some(self.max))])
    }

    /// Panics as this method should never be called.
    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `SUM` directly from segments containing metadata and models.
#[derive(Debug)]
pub struct ModelSumPhysicalExpr {}

impl Display for ModelSumPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelSumPhysicalExpr(Float32)")
    }
}

/// Enable equal and not equal for [`Any`] and [`ModelSumPhysicalExpr`].
impl PartialEq<dyn Any> for ModelSumPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelSumPhysicalExpr {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the return [`DataType`] of this [`PhysicalExpr`].
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> Result<ColumnarValue> {
        modelardb_common::arrays!(
            record_batch,
            _univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            _residuals,
            _error_array
        );

        let mut sum = 0.0;
        for row_index in 0..record_batch.num_rows() {
            let model_type_id = model_type_ids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);
            let min_value = min_values.value(row_index);
            let max_value = max_values.value(row_index);
            let values = values.value(row_index);

            sum += models::sum(
                model_type_id,
                start_time,
                end_time,
                timestamps,
                min_value,
                max_value,
                values,
            );
        }

        // Returning a Scalar fills an array with the value.
        let mut result = Float32Array::builder(1);
        result.append_value(sum);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelSumPhysicalExpr does not support children.".to_owned(),
        ))
    }
}

/// [`Accumulator`] that accumulates `SUM` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelSumAccumulator {
    /// Current sum stored by the [`Accumulator`].
    sum: f32,
    /// Current count stored by the [`Accumulator`]. It is stored so it can be passed to
    /// [SumAccumulator](repository/datafusion/physical-expr/src/aggregate/sum.rs).
    count: u64,
}

impl Accumulator for ModelSumAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.sum += array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0);
            self.count += 1;
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`]. It must match
    /// [SumAccumulator](repository/datafusion/physical-expr/src/aggregate/sum.rs).
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float32(Some(self.sum)),
            ScalarValue::from(self.count),
        ])
    }

    /// Panics as this method should never be called.
    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `AVG` directly from segments containing metadata and models.
#[derive(Debug)]
pub struct ModelAvgPhysicalExpr {}

impl Display for ModelAvgPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelAvgPhysicalExpr(Float32)")
    }
}

/// Enable equal and not equal for [`Any`] and [`ModelAvgPhysicalExpr`].
impl PartialEq<dyn Any> for ModelAvgPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelAvgPhysicalExpr {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the return [`DataType`] of this [`PhysicalExpr`].
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> Result<ColumnarValue> {
        modelardb_common::arrays!(
            record_batch,
            _univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            _residuals,
            _error_array
        );

        let mut sum = 0.0;
        let mut count: usize = 0;
        for row_index in 0..record_batch.num_rows() {
            let model_type_id = model_type_ids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);
            let min_value = min_values.value(row_index);
            let max_value = max_values.value(row_index);
            let values = values.value(row_index);

            sum += models::sum(
                model_type_id,
                start_time,
                end_time,
                timestamps,
                min_value,
                max_value,
                values,
            );

            count += models::len(start_time, end_time, timestamps);
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(2);
        result.append_value(sum as Value);
        result.append_value(count as Value);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelAvgPhysicalExpr does not support children.".to_owned(),
        ))
    }
}

/// [`Accumulator`] that accumulates `AVG` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelAvgAccumulator {
    /// The current sum stored in the [`Accumulator`].
    sum: f32,
    /// The current count stored in the [`Accumulator`].
    count: u64,
}

impl Accumulator for ModelAvgAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            self.sum += array.value(0);
            self.count += array.value(1) as u64;
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`]. It must match
    /// [AvgAccumulator](repository/datafusion/physical-expr/src/aggregate/average.rs).
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float32(Some(self.sum)),
        ])
    }

    /// Panics as this method should never be called.
    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::file_format::ParquetExec;
    use datafusion::physical_plan::filter::FilterExec;

    use crate::optimizer::test_util;
    use crate::query::grid_exec::GridExec;

    use super::*;

    // Tests for ModelSimpleAggregatesPhysicalOptimizerRule.
    #[tokio::test]
    async fn test_rewrite_aggregate_on_one_column_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = "SELECT COUNT(field_1) FROM model_table";
        let physical_plan =
            test_util::query_optimized_physical_query_plan(temp_dir.path(), query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<ParquetExec>()],
        ];

        test_util::assert_eq_physical_plan_expected(physical_plan, expected_plan);
    }

    #[tokio::test]
    async fn test_rewrite_aggregates_on_one_column_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = "SELECT COUNT(field_1), MIN(field_1), MAX(field_1),
                                  SUM(field_1), AVG(field_1) FROM model_table";
        let physical_plan =
            test_util::query_optimized_physical_query_plan(temp_dir.path(), query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<ParquetExec>()],
        ];

        test_util::assert_eq_physical_plan_expected(physical_plan, expected_plan);
    }

    #[tokio::test]
    async fn test_do_not_rewrite_aggregate_on_one_column_with_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = "SELECT COUNT(field_1) FROM model_table WHERE field_1 = 37.0";
        let physical_plan =
            test_util::query_optimized_physical_query_plan(temp_dir.path(), query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalesceBatchesExec>()],
            vec![TypeId::of::<FilterExec>()],
            vec![TypeId::of::<SortedJoinExec>()],
            vec![TypeId::of::<RepartitionExec>()],
            vec![TypeId::of::<GridExec>()],
            vec![TypeId::of::<ParquetExec>()],
        ];

        test_util::assert_eq_physical_plan_expected(physical_plan, expected_plan);
    }

    #[tokio::test]
    async fn test_do_not_rewrite_aggregate_on_multiple_columns_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = "SELECT COUNT(field_1), COUNT(field_2) FROM model_table";
        let physical_plan =
            test_util::query_optimized_physical_query_plan(temp_dir.path(), query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<SortedJoinExec>()],
            vec![
                TypeId::of::<RepartitionExec>(),
                TypeId::of::<RepartitionExec>(),
            ],
            vec![TypeId::of::<GridExec>(), TypeId::of::<GridExec>()],
            vec![TypeId::of::<ParquetExec>(), TypeId::of::<ParquetExec>()],
        ];

        test_util::assert_eq_physical_plan_expected(physical_plan, expected_plan);
    }
}
