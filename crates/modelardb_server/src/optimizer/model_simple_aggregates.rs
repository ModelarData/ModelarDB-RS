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
use datafusion::physical_plan::expressions::{self, Avg, Column, Count, Max, Min, Sum};
use datafusion::physical_plan::file_format::ParquetExec;
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

    /// Return the name of the physical optimizer rule.
    fn name(&self) -> &str {
        "ModelSimpleAggregatesPhysicalOptimizerRule"
    }

    /// Specify if the physical planner should valid that the rule does not change the schema.
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
    // The rule tries matches to match the sub-tree of execution_plan so execution_plan can be updated.
    let execution_plan_children = execution_plan.children();

    if execution_plan_children.len() == 1 {
        if let Some(aggregate_exec) = execution_plan_children[0]
            .as_any()
            .downcast_ref::<AggregateExec>()
        {
            let aggregate_exec_children = aggregate_exec.children();
            if aggregate_exec_children.len() == 1
                && aggregate_exec.group_expr().is_empty()
                && aggregate_exec.filter_expr().iter().all(Option::is_none)
            {
                let aggregate_exec_input = aggregate_exec_children[0].clone();
                if let Some(sorted_join_exec) = aggregate_exec_input
                    .as_any()
                    .downcast_ref::<SortedJoinExec>()
                {
                    if let Ok(input) =
                        try_new_aggregate_exec(aggregate_exec, sorted_join_exec.children())
                    {
                        // unwrap() is safe as the inputs are constructed from sorted_join_exec.
                        return Ok(Transformed::Yes(
                            execution_plan.with_new_children(vec![input]).unwrap(),
                        ));
                    }
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
    if inputs_evaluates_no_predicates(&grid_execs) {
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
    } else {
        Err(DataFusionError::NotImplemented(
            "Each input must produce a single aggregate.".to_owned(),
        ))
    }
}

/// Return [`AggregateExprs`](AggregateExpr) that computes the same aggregates as `aggregate_exec`
/// if the aggregates are all computed for the same FIELD column, otherwise [`DataFusionError`] is
/// returned.
fn try_rewrite_aggregate_exprs(
    aggregate_exec: &AggregateExec,
) -> Result<Vec<Arc<dyn AggregateExpr>>> {
    let aggregate_exprs = aggregate_exec.aggr_expr();

    if aggregates_are_for_different_columns(aggregate_exprs) {
        return Err(DataFusionError::Internal(
            "SortedJoinExec currently does not support joining aggregates.".to_owned(),
        ));
    }

    // input_schema has one column as it is checked by rewrite_aggregates_to_use_segments().
    if *aggregate_exec.input_schema().field(0).data_type() != ArrowValue::DATA_TYPE {
        return Err(DataFusionError::Internal(
            "Model-based aggregates can only be computed for FIELD columns.".to_owned(),
        ));
    }

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

fn inputs_evaluates_no_predicates(inputs: &Vec<Arc<dyn ExecutionPlan>>) -> bool {
    for input in inputs {
        for child in input.children() {
            if let Some(parquet_exec) = child.as_any().downcast_ref::<ParquetExec>() {
                if parquet_exec.predicate().is_some() || parquet_exec.pruning_predicate().is_some()
                {
                    return false;
                }
            }
        }
    }
    true
}

fn aggregates_are_for_different_columns(aggregate_exprs: &[Arc<dyn AggregateExpr>]) -> bool {
    if aggregate_exprs.is_empty() {
        false
    } else {
        if let Some(first_aggregate_expr) = aggregate_exprs.get(0) {
            let first_expressions = first_aggregate_expr.expressions();
            if let Some(first_column) = column_if_only_expression(&first_expressions) {
                for aggregate_expr in aggregate_exprs {
                    let expressions = aggregate_expr.expressions();
                    if let Some(column) = column_if_only_expression(&expressions) {
                        if first_column.index() != column.index() {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}

fn column_if_only_expression(expressions: &Vec<Arc<dyn PhysicalExpr>>) -> Option<&Column> {
    if expressions.len() == 1 {
        expressions[0].as_any().downcast_ref::<Column>()
    } else {
        None
    }
}

#[derive(Debug, PartialEq)]
enum ModelAggregateType {
    Count,
    Min,
    Max,
    Sum,
    Avg,
}

#[derive(Debug)]
pub struct ModelAggregateExpr {
    name: String,
    aggregate_type: ModelAggregateType,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(self.name(), self.data_type.clone(), false))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let fields = match &self.aggregate_type {
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
        };
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let expr: Arc<dyn PhysicalExpr> = match &self.aggregate_type {
            ModelAggregateType::Count => Arc::new(ModelCountPhysicalExpr {}),
            ModelAggregateType::Min => Arc::new(ModelMinPhysicalExpr {}),
            ModelAggregateType::Max => Arc::new(ModelMaxPhysicalExpr {}),
            ModelAggregateType::Sum => Arc::new(ModelSumPhysicalExpr {}),
            ModelAggregateType::Avg => Arc::new(ModelAvgPhysicalExpr {}),
        };
        vec![expr]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accum: Box<dyn Accumulator> = match self.aggregate_type {
            ModelAggregateType::Count => Box::new(ModelCountAccumulator { count: 0 }),
            ModelAggregateType::Min => Box::new(ModelMinAccumulator { min: f32::MAX }),
            ModelAggregateType::Max => Box::new(ModelMaxAccumulator { max: f32::MIN }),
            ModelAggregateType::Sum => Box::new(ModelSumAccumulator { sum: 0.0, count: 0 }),
            ModelAggregateType::Avg => Box::new(ModelAvgAccumulator { sum: 0.0, count: 0 }),
        };
        Ok(accum)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub struct ModelCountPhysicalExpr {}

impl Display for ModelCountPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelCountPhysicalExpr(Int64)")
    }
}

impl PartialEq<dyn Any> for ModelCountPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelCountPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        modelardb_common::arrays!(
            batch,
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
        for row_index in 0..batch.num_rows() {
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

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[derive(Debug)]
struct ModelCountAccumulator {
    count: i64,
}

impl Accumulator for ModelCountAccumulator {
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

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[derive(Debug)]
pub struct ModelMinPhysicalExpr {}

impl Display for ModelMinPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelMinPhysicalExpr(Float32)")
    }
}

impl PartialEq<dyn Any> for ModelMinPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelMinPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut min = Value::MAX;
        let min_value_array = modelardb_common::array!(batch, 5, ValueArray);
        for row_index in 0..batch.num_rows() {
            min = Value::min(min, min_value_array.value(row_index));
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(1);
        result.append_value(min);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[derive(Debug)]
struct ModelMinAccumulator {
    min: f32,
}

impl Accumulator for ModelMinAccumulator {
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

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float32(Some(self.min))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[derive(Debug)]
pub struct ModelMaxPhysicalExpr {}

impl Display for ModelMaxPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelMaxPhysicalExpr(Float32)")
    }
}

impl PartialEq<dyn Any> for ModelMaxPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelMaxPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut max = Value::MIN;
        let max_value_array = modelardb_common::array!(batch, 6, ValueArray);
        for row_index in 0..batch.num_rows() {
            max = Value::max(max, max_value_array.value(row_index));
        }

        // Returning a Scalar fills an array with the value.
        let mut result = ValueArray::builder(1);
        result.append_value(max);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[derive(Debug)]
struct ModelMaxAccumulator {
    max: f32,
}

impl Accumulator for ModelMaxAccumulator {
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

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float32(Some(self.max))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[derive(Debug)]
pub struct ModelSumPhysicalExpr {}

impl Display for ModelSumPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelSumPhysicalExpr(Float32)")
    }
}

impl PartialEq<dyn Any> for ModelSumPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelSumPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        modelardb_common::arrays!(
            batch,
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
        for row_index in 0..batch.num_rows() {
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

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[derive(Debug)]
struct ModelSumAccumulator {
    sum: f32,
    count: u64,
}

impl Accumulator for ModelSumAccumulator {
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

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float32(Some(self.sum)),
            ScalarValue::from(self.count),
        ])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[derive(Debug)]
pub struct ModelAvgPhysicalExpr {}

impl Display for ModelAvgPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelAvgPhysicalExpr(Float32)")
    }
}

impl PartialEq<dyn Any> for ModelAvgPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        self == other
    }
}

impl PhysicalExpr for ModelAvgPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        modelardb_common::arrays!(
            batch,
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
        for row_index in 0..batch.num_rows() {
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

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[derive(Debug)]
struct ModelAvgAccumulator {
    sum: f32,
    count: u64,
}

impl Accumulator for ModelAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            self.sum += array.value(0);
            self.count += array.value(1) as u64;
        }
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        //Must match datafusion::physical_plan::expressions::AvgAccumulator
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float32(Some(self.sum)),
        ])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}
