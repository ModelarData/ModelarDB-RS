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
//! reconstructed values from a single column without filtering, so they are computed directly from
//! segments instead of the reconstructed values.

#![allow(clippy::unconditional_recursion)]

use std::any::Any;
use std::cmp::PartialEq;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Float32Array, Float64Array, Int64Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::parquet::ParquetExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::min_max::{Max, Min};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::expressions;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{
    Accumulator, AggregateExpr, ColumnarValue, ExecutionPlan, PhysicalExpr,
};
use datafusion::scalar::ScalarValue;
use modelardb_types::types::{ArrowValue, TimestampArray, Value, ValueArray};

use crate::query::sorted_join_exec::SortedJoinExec;
use crate::univariate_ids_int64_to_uint64;

/// Rewrite aggregates that are computed from reconstructed values from a single column without
/// filtering, so they are computed directly from segments instead of the reconstructed values.
pub struct ModelSimpleAggregatesPhysicalOptimizerRule {}

impl PhysicalOptimizerRule for ModelSimpleAggregatesPhysicalOptimizerRule {
    /// Rewrite `execution_plan` so it computes simple aggregates without filtering from segments
    /// instead of reconstructed values.
    fn optimize(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        execution_plan
            .transform_down(&rewrite_aggregates_to_use_segments)
            .map(|transformed| transformed.data)
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

/// Matches the pattern [`AggregateExpr`] <- [RepartitionExec] (if it exists) <- [`SortedJoinExec`]
/// <- [`GridExec`](crate::query::grid_exec::GridExec) <- [`ParquetExec`] and rewrites it to
/// [`AggregateExpr`] <- [`ParquetExec`] if [`AggregateExpr`] only computes aggregates for a single
/// FIELD column and no filtering is performed by [`ParquetExec`].
fn rewrite_aggregates_to_use_segments(
    execution_plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Transformed<Arc<dyn ExecutionPlan>>> {
    // The rule tries to match the subtree of execution_plan so execution_plan can be updated.
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
                // Remove RepartitionExec if added by Apache Arrow DataFusion. Both AggregateExec
                // and RepartitionExec can only have one child, so it is not necessary to check it.
                let maybe_repartition_exec = &aggregate_exec_children[0];
                let aggregate_exec_input = if let Some(repartition_exec) = maybe_repartition_exec
                    .as_any()
                    .downcast_ref::<RepartitionExec>()
                {
                    repartition_exec.children()[0].clone()
                } else {
                    (*maybe_repartition_exec).clone()
                };

                if let Some(sorted_join_exec) = aggregate_exec_input
                    .as_any()
                    .downcast_ref::<SortedJoinExec>()
                {
                    // Try to create new AggregateExec that compute aggregates directly from segments.
                    if let Ok(input) =
                        try_new_aggregate_exec(aggregate_exec, sorted_join_exec.children())
                    {
                        return Ok(Transformed::yes(
                            execution_plan.with_new_children(vec![input])?,
                        ));
                    };
                }
            }
        }
    }

    Ok(Transformed::no(execution_plan))
}

/// Return an [`AggregateExec`] that computes the same aggregates as `aggregate_exec` if no
/// predicates have been pushed to `inputs` and the aggregates are all computed for the same FIELD
/// column, otherwise [`DataFusionError`] is returned.
fn try_new_aggregate_exec(
    aggregate_exec: &AggregateExec,
    grid_execs: Vec<&Arc<dyn ExecutionPlan>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
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

    Ok(Arc::new(AggregateExec::try_new(
        *aggregate_exec.mode(),
        aggregate_exec.group_expr().clone(),
        model_based_aggregate_exprs,
        aggregate_exec.filter_expr().to_vec(),
        grid_execs[0].children()[0].clone(),
        aggregate_exec.schema(),
    )?))
}

/// Return [`AggregateExprs`](AggregateExpr) that computes the same aggregates as `aggregate_exec`
/// if the aggregates are all computed for the same FIELD column, otherwise [`DataFusionError`] is
/// returned.
fn try_rewrite_aggregate_exprs(
    aggregate_exec: &AggregateExec,
) -> DataFusionResult<Vec<Arc<dyn AggregateExpr>>> {
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
        } else if let Some(aggregate_function_expr) = aggregate_expr
            .as_any()
            .downcast_ref::<AggregateFunctionExpr>()
        {
            // An aggregate query can be an AggregateFunctionExpr instead of built-in aggregate.
            let aggregate_function_expr_name = aggregate_function_expr.fun().name();
            match aggregate_function_expr_name {
                "count" => rewritten_aggregate_exprs
                    .push(ModelAggregateExpr::new(ModelAggregateType::Count)),
                "min" => {
                    rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Min))
                }
                "max" => {
                    rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Max))
                }
                "sum" => {
                    rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Sum))
                }
                "avg" => {
                    rewritten_aggregate_exprs.push(ModelAggregateExpr::new(ModelAggregateType::Avg))
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Aggregate function expression {} is currently not supported.",
                        aggregate_function_expr_name
                    )))
                }
            }
        } else {
            return Err(DataFusionError::NotImplemented(format!(
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
            ModelAggregateType::Sum => DataType::Float64,
            ModelAggregateType::Avg => DataType::Float64,
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
    fn field(&self) -> DataFusionResult<Field> {
        Ok(Field::new(self.name(), self.data_type.clone(), false))
    }

    /// Return [`Fields`](Field) that specify the names and [`DataTypes`](DataType) of the
    /// [`Accumulator's`](Accumulator) intermediate state.
    fn state_fields(&self) -> DataFusionResult<Vec<Field>> {
        Ok(match &self.aggregate_type {
            ModelAggregateType::Sum => vec![Field::new("SUM", DataType::Float64, false)],
            ModelAggregateType::Avg => vec![
                Field::new("COUNT", DataType::UInt64, false),
                Field::new("SUM", DataType::Float64, false),
            ],
            _ => vec![Field::new(
                expressions::format_state_name(self.name(), "NOT NULL"),
                self.data_type.clone(),
                false,
            )],
        })
    }

    /// Return the [`PhysicalExpr`] that is passed to the [`Accumulator`].
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
    fn create_accumulator(&self) -> DataFusionResult<Box<dyn Accumulator>> {
        Ok(match self.aggregate_type {
            ModelAggregateType::Count => Box::new(ModelCountAccumulator { count: 0 }),
            ModelAggregateType::Min => Box::new(ModelMinAccumulator { min: f32::MAX }),
            ModelAggregateType::Max => Box::new(ModelMaxAccumulator { max: f32::MIN }),
            ModelAggregateType::Sum => Box::new(ModelSumAccumulator { sum: 0.0 }),
            ModelAggregateType::Avg => Box::new(ModelAvgAccumulator { sum: 0.0, count: 0 }),
        })
    }

    /// Return the name of the [`AggregateExpr`].
    fn name(&self) -> &str {
        &self.name
    }
}

/// [`PhysicalExpr`] that computes `COUNT` directly from segments containing metadata and models.
#[derive(Debug, Hash)]
pub struct ModelCountPhysicalExpr {}

impl Display for ModelCountPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
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
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    /// Return [`false`] as this expression cannot return `NULL`.
    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(false)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        // Reinterpret univariate_ids from int64 to uint64 to fix #187 as a stopgap until #197.
        let record_batch = univariate_ids_int64_to_uint64(record_batch);

        modelardb_types::arrays!(
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

            count += modelardb_compression::len(start_time, end_time, timestamps) as i64;
        }

        // Returning a Scalar fills an array with the value.
        let mut result = Int64Array::builder(1);
        result.append_value(count);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelCountPhysicalExpr does not support children.".to_owned(),
        ))
    }

    /// Feed this [`PhysicalExpr`] into `state`.
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
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
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
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
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Int64(Some(self.count))];
        self.count = 0;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `MIN` directly from segments containing metadata and models.
#[derive(Debug, Hash)]
pub struct ModelMinPhysicalExpr {}

impl Display for ModelMinPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
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
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Float32)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let mut min = Value::MAX;
        let min_value_array = modelardb_types::array!(record_batch, 5, ValueArray);
        for row_index in 0..record_batch.num_rows() {
            min = Value::min(min, min_value_array.value(row_index));
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(1);
        result.append_value(min);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelMinPhysicalExpr does not support children.".to_owned(),
        ))
    }

    /// Feed this [`PhysicalExpr`] into `state`.
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
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
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
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
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Float32(Some(self.min))];
        self.min = f32::MAX;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `MAX` directly from segments containing metadata and models.
#[derive(Debug, Hash)]
pub struct ModelMaxPhysicalExpr {}

impl Display for ModelMaxPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
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
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Float32)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let mut max = Value::MIN;
        let max_value_array = modelardb_types::array!(record_batch, 6, ValueArray);
        for row_index in 0..record_batch.num_rows() {
            max = Value::max(max, max_value_array.value(row_index));
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(1);
        result.append_value(max);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelMaxPhysicalExpr does not support children.".to_owned(),
        ))
    }

    /// Feed this [`PhysicalExpr`] into `state`.
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
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
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
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
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Float32(Some(self.max))];
        self.max = f32::MIN;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `SUM` directly from segments containing metadata and models.
#[derive(Debug, Hash)]
pub struct ModelSumPhysicalExpr {}

impl Display for ModelSumPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "ModelSumPhysicalExpr(Float64)")
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
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        // Reinterpret univariate_ids from int64 to uint64 to fix #187 as a stopgap until #197.
        let record_batch = univariate_ids_int64_to_uint64(record_batch);

        modelardb_types::arrays!(
            record_batch,
            _univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            residuals,
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
            let residuals = residuals.value(row_index);

            sum += modelardb_compression::sum(
                model_type_id,
                start_time,
                end_time,
                timestamps,
                min_value,
                max_value,
                values,
                residuals,
            ) as f64;
        }

        // Returning a Scalar fills an array with the value.
        let mut result = Float64Array::builder(1);
        result.append_value(sum);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelSumPhysicalExpr does not support children.".to_owned(),
        ))
    }

    /// Feed this [`PhysicalExpr`] into `state`.
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

/// [`Accumulator`] that accumulates `SUM` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelSumAccumulator {
    /// Current sum stored by the [`Accumulator`].
    sum: f64,
}

impl Accumulator for ModelSumAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        for array in values {
            self.sum += array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`]. It must match
    /// [SumAccumulator](repository/datafusion/physical-expr/src/aggregate/sum.rs).
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Float64(Some(self.sum))];
        self.sum = 0.0;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`PhysicalExpr`] that computes `AVG` directly from segments containing metadata and models.
#[derive(Debug, Hash)]
pub struct ModelAvgPhysicalExpr {}

impl Display for ModelAvgPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "ModelAvgPhysicalExpr(Float64)")
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
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    /// Return [`true`] as this expression returns `NULL` for empty [`RecordBatches`](RecordBatch).
    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    /// Evaluate this [`PhysicalExpr`] against `record_batch`.
    fn evaluate(&self, record_batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        // Reinterpret univariate_ids from int64 to uint64 to fix #187 as a stopgap until #197.
        let record_batch = univariate_ids_int64_to_uint64(record_batch);

        modelardb_types::arrays!(
            record_batch,
            _univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            residuals,
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
            let residuals = residuals.value(row_index);

            sum += modelardb_compression::sum(
                model_type_id,
                start_time,
                end_time,
                timestamps,
                min_value,
                max_value,
                values,
                residuals,
            );

            count += modelardb_compression::len(start_time, end_time, timestamps);
        }

        // Returning a Scalar fills an array with the value.
        let mut result = Float64Array::builder(2);
        result.append_value(sum as f64);
        result.append_value(count as f64);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    /// Return the list of [`PhysicalExprs`](PhysicalExpr) that provide input to this [`PhysicalExpr`].
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Return [`DataFusionError`] as this [`PhysicalExpr`] never reads any input.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Plan(
            "ModelAvgPhysicalExpr does not support children.".to_owned(),
        ))
    }

    /// Feed this [`PhysicalExpr`] into `state`.
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

/// [`Accumulator`] that accumulates `AVG` computed directly from segments containing metadata and
/// models.
#[derive(Debug)]
struct ModelAvgAccumulator {
    /// The current sum stored in the [`Accumulator`].
    sum: f64,
    /// The current count stored in the [`Accumulator`].
    count: u64,
}

impl Accumulator for ModelAvgAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        for array in values {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            self.sum += array.value(0);
            self.count += array.value(1) as u64;
        }
        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`]. It must match
    /// [AvgAccumulator](repository/datafusion/physical-expr/src/aggregate/average.rs).
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float64(Some(self.sum)),
        ];
        self.sum = 0.0;
        self.count = 0;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::any::TypeId;
    use std::fmt::Debug;

    use datafusion::datasource::physical_plan::parquet::ParquetExec;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::insert::DataSink;
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;
    use tonic::async_trait;

    use crate::delta_lake::DeltaLake;
    use crate::metadata::table_metadata_manager::TableMetadataManager;
    use crate::optimizer;
    use crate::query::grid_exec::GridExec;
    use crate::query::model_table::ModelTable;
    use crate::test;

    // DataSink for testing.
    struct NoOpDataSink {}

    #[async_trait]
    impl DataSink for NoOpDataSink {
        fn as_any(&self) -> &dyn Any {
            unimplemented!();
        }

        fn metrics(&self) -> Option<MetricsSet> {
            unimplemented!();
        }

        async fn write_all(
            &self,
            _data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> DataFusionResult<u64> {
            unimplemented!();
        }
    }

    impl Debug for NoOpDataSink {
        fn fmt(&self, _f: &mut Formatter<'_>) -> FmtResult {
            unimplemented!();
        }
    }

    impl DisplayAs for NoOpDataSink {
        fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter<'_>) -> FmtResult {
            unimplemented!();
        }
    }

    // Tests for ModelSimpleAggregatesPhysicalOptimizerRule.
    #[tokio::test]
    async fn test_rewrite_aggregate_on_one_column_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = &format!("SELECT COUNT(field_1) FROM {}", test::MODEL_TABLE_NAME);
        let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<ParquetExec>()],
        ];

        assert_eq_physical_plan_expected(physical_plan, &expected_plan);
    }

    #[tokio::test]
    async fn test_rewrite_aggregates_on_one_column_without_predicates() {
        // Apache Arrow DataFusion 30 creates two input columns to AggregateExec when both SUM and
        // AVG is computed in the same query, so for now, multiple queries are used for the test.
        let query_no_avg = &format!(
            "SELECT COUNT(field_1), MIN(field_1), MAX(field_1), SUM(field_1) FROM {}",
            test::MODEL_TABLE_NAME
        );
        let query_only_avg = &format!("SELECT AVG(field_1) FROM {}", test::MODEL_TABLE_NAME);

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<ParquetExec>()],
        ];

        for query in [query_no_avg, query_only_avg] {
            let temp_dir = tempfile::tempdir().unwrap();

            let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;
            assert_eq_physical_plan_expected(physical_plan, &expected_plan);
        }
    }

    #[tokio::test]
    async fn test_do_not_rewrite_aggregate_on_one_column_with_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = &format!(
            "SELECT COUNT(field_1) FROM {} WHERE field_1 = 37.0",
            test::MODEL_TABLE_NAME
        );
        let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalesceBatchesExec>()],
            vec![TypeId::of::<FilterExec>()],
            vec![TypeId::of::<RepartitionExec>()],
            vec![TypeId::of::<SortedJoinExec>()],
            vec![TypeId::of::<GridExec>()],
            vec![TypeId::of::<ParquetExec>()],
        ];

        assert_eq_physical_plan_expected(physical_plan, &expected_plan);
    }

    #[tokio::test]
    async fn test_do_not_rewrite_aggregate_on_multiple_columns_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = &format!(
            "SELECT COUNT(field_1), COUNT(field_2) FROM {}",
            test::MODEL_TABLE_NAME
        );
        let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<RepartitionExec>()],
            vec![TypeId::of::<SortedJoinExec>()],
            vec![TypeId::of::<GridExec>(), TypeId::of::<GridExec>()],
            vec![TypeId::of::<ParquetExec>(), TypeId::of::<ParquetExec>()],
        ];

        assert_eq_physical_plan_expected(physical_plan, &expected_plan);
    }

    /// Parse, plan, and optimize the `query` for execution on data in `path`.
    async fn query_optimized_physical_query_plan(
        temp_dir: &TempDir,
        query: &str,
    ) -> Arc<dyn ExecutionPlan> {
        // Setup access to data and metadata in data folder.
        let data_folder_path = temp_dir.path();
        let delta_lake = DeltaLake::try_from_local_path(data_folder_path).unwrap();
        let table_metadata_manager = Arc::new(
            TableMetadataManager::try_from_path(data_folder_path, None)
                .await
                .unwrap(),
        );

        // Setup access to Apache DataFusion.
        let mut session_state_builder = SessionStateBuilder::new().with_default_features();

        // Uses the rule method instead of the rules method as the rules method replaces the built-ins.
        for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
            session_state_builder =
                session_state_builder.with_physical_optimizer_rule(physical_optimizer_rule);
        }

        let session_state = session_state_builder.build();
        let session_context = SessionContext::new_with_state(session_state);

        // Create model table.
        let model_table_metadata = test::model_table_metadata_arc();

        let delta_table = delta_lake
            .create_model_table(&model_table_metadata.name)
            .await
            .unwrap();

        table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let model_table_data_sink = Arc::new(NoOpDataSink {});

        let model_table = ModelTable::new(
            delta_table,
            table_metadata_manager,
            model_table_metadata.clone(),
            model_table_data_sink,
        );

        session_context
            .register_table(test::MODEL_TABLE_NAME, model_table)
            .unwrap();

        session_context
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// Assert that `physical_plan` and `expected_plan` contain the same operators. `expected_plan`
    /// only contains the type ids so the tests do not have to construct the actual operators.
    fn assert_eq_physical_plan_expected(
        physical_plan: Arc<dyn ExecutionPlan>,
        expected_plan: &[Vec<TypeId>],
    ) {
        let mut level = 0;
        let mut current_execs = vec![physical_plan];
        let mut next_execs = vec![];

        while !current_execs.is_empty() {
            let expected_execs = &expected_plan[level];
            assert_eq!(current_execs.len(), expected_execs.len());

            for (current, expected) in current_execs.iter().zip(expected_execs) {
                assert_eq!(current.as_any().type_id(), *expected);
                next_execs.extend(current.children().iter().map(|exec| (*exec).clone()));
            }

            level += 1;
            current_execs = next_execs;
            next_execs = vec![];
        }
    }
}
