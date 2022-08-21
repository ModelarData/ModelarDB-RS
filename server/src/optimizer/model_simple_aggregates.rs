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
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::ModelTableMetadata;
use crate::models;
use crate::tables::GridExec;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Float32Array, Int32Array, Int64Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::error::Result;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::expressions::format_state_name;
use datafusion::physical_plan::expressions::{Avg, Count, Max, Min, Sum};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use datafusion_expr::AggregateState;

// Helper Functions.
fn new_aggregate(
    aggregate_exec: &AggregateExec,
    model_aggregate_expr: Arc<ModelAggregateExpr>,
    grid_exec: &GridExec,
) -> Arc<AggregateExec> {
    // Assumes the GridExec only have a single child.
    Arc::new(
        AggregateExec::try_new(
            *aggregate_exec.mode(),
            aggregate_exec.group_expr().clone(),
            vec![model_aggregate_expr],
            grid_exec.children()[0].clone(), //Removes the GridExec
            aggregate_exec.input_schema(),
        )
        .unwrap(),
    )
}

// Optimizer Rule.
pub struct ModelSimpleAggregatesPhysicalOptimizerRule {}

impl ModelSimpleAggregatesPhysicalOptimizerRule {
    fn optimize(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        // Matches a simple aggregate performed without filtering out segments.
        if let Some(hae) = plan.as_any().downcast_ref::<AggregateExec>() {
            let children = &hae.children();
            if children.len() == 1 {
                if let Some(rp) = children[0].as_any().downcast_ref::<RepartitionExec>() {
                    let children = &rp.children();
                    if children.len() == 1 {
                        let ae = hae.aggr_expr();
                        if ae.len() == 1 {
                            // TODO: simplify and factor out shared code using macros or functions.
                            if ae[0].as_any().downcast_ref::<Count>().is_some() {
                                if let Some(ge) = children[0].as_any().downcast_ref::<GridExec>() {
                                    let mae = ModelAggregateExpr::new(
                                        ModelAggregateType::Count,
                                        ge.model_table_metadata.clone(),
                                    );
                                    return Some(new_aggregate(hae, mae, ge));
                                }
                            } else if ae[0].as_any().downcast_ref::<Min>().is_some() {
                                if let Some(ge) = children[0].as_any().downcast_ref::<GridExec>() {
                                    let mae = ModelAggregateExpr::new(
                                        ModelAggregateType::Min,
                                        ge.model_table_metadata.clone(),
                                    );
                                    return Some(new_aggregate(hae, mae, ge));
                                }
                            } else if ae[0].as_any().downcast_ref::<Max>().is_some() {
                                if let Some(ge) = children[0].as_any().downcast_ref::<GridExec>() {
                                    let mae = ModelAggregateExpr::new(
                                        ModelAggregateType::Max,
                                        ge.model_table_metadata.clone(),
                                    );
                                    return Some(new_aggregate(hae, mae, ge));
                                }
                            } else if ae[0].as_any().downcast_ref::<Sum>().is_some() {
                                if let Some(ge) = children[0].as_any().downcast_ref::<GridExec>() {
                                    let mae = ModelAggregateExpr::new(
                                        ModelAggregateType::Sum,
                                        ge.model_table_metadata.clone(),
                                    );
                                    return Some(new_aggregate(hae, mae, ge));
                                }
                            } else if ae[0].as_any().downcast_ref::<Avg>().is_some() {
                                if let Some(ge) = children[0].as_any().downcast_ref::<GridExec>() {
                                    let mae = ModelAggregateExpr::new(
                                        ModelAggregateType::Avg,
                                        ge.model_table_metadata.clone(),
                                    );
                                    return Some(new_aggregate(hae, mae, ge));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Visit the children.
        // TODO: handle plans were multiple children must be updated.
        for child in plan.children() {
            if let Some(new_child) = self.optimize(&child) {
                return Some(plan.clone().with_new_children(vec![new_child]).unwrap());
            }
        }
        None
    }
}

// TODO: determine if some structs or traits can be removed or parametrized?
impl PhysicalOptimizerRule for ModelSimpleAggregatesPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(optimized_plan) = self.optimize(&plan) {
            Ok(optimized_plan)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "ModelSimpleAggregatesPhysicalOptimizerRule"
    }
}

// Aggregate Expressions.
#[derive(Debug)]
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
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl ModelAggregateExpr {
    fn new(
        aggregate_type: ModelAggregateType,
        model_table_metadata: Arc<ModelTableMetadata>,
    ) -> Arc<Self> {
        let data_type = match &aggregate_type {
            ModelAggregateType::Count => DataType::UInt64,
            ModelAggregateType::Min => DataType::Float32,
            ModelAggregateType::Max => DataType::Float32,
            ModelAggregateType::Sum => DataType::Float32,
            ModelAggregateType::Avg => DataType::Float32,
        };

        Arc::new(Self {
            name: format!("Model{:?}AggregateExpr", aggregate_type),
            aggregate_type,
            data_type,
            model_table_metadata,
        })
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
            ModelAggregateType::Sum => vec![Field::new("SUM", DataType::Float32, false)],
            ModelAggregateType::Avg => vec![
                Field::new("COUNT", DataType::UInt64, false),
                Field::new("SUM", DataType::Float32, false),
            ],
            _ => vec![Field::new(
                &format_state_name(self.name(), "NOT NULL"),
                self.data_type.clone(),
                false,
            )],
        };
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let expr: Arc<dyn PhysicalExpr> = match &self.aggregate_type {
            ModelAggregateType::Count => Arc::new(ModelCountPhysicalExpr {
                model_table_metadata: self.model_table_metadata.clone(),
            }),
            ModelAggregateType::Min => Arc::new(ModelMinPhysicalExpr {
                model_table_metadata: self.model_table_metadata.clone(),
            }),
            ModelAggregateType::Max => Arc::new(ModelMaxPhysicalExpr {
                model_table_metadata: self.model_table_metadata.clone(),
            }),
            ModelAggregateType::Sum => Arc::new(ModelSumPhysicalExpr {
                model_table_metadata: self.model_table_metadata.clone(),
            }),
            ModelAggregateType::Avg => Arc::new(ModelAvgPhysicalExpr {
                model_table_metadata: self.model_table_metadata.clone(),
            }),
        };
        vec![expr]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accum: Box<dyn Accumulator> = match self.aggregate_type {
            ModelAggregateType::Count => Box::new(ModelCountAccumulator { count: 0 }),
            ModelAggregateType::Min => Box::new(ModelMinAccumulator { min: f32::MAX }),
            ModelAggregateType::Max => Box::new(ModelMaxAccumulator { max: f32::MIN }),
            ModelAggregateType::Sum => Box::new(ModelSumAccumulator { sum: 0.0 }),
            ModelAggregateType::Avg => Box::new(ModelAvgAccumulator { sum: 0.0, count: 0 }),
        };
        Ok(accum)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

//Count
#[derive(Debug)]
pub struct ModelCountPhysicalExpr {
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl Display for ModelCountPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelCountPhysicalExpr(UInt64)")
    }
}

impl PhysicalExpr for ModelCountPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let gids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let start_times = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let end_times = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let count = models::count(
            gids.len(),
            gids,
            start_times,
            end_times,
            &self.model_table_metadata.sampling_intervals,
        ) as u64;

        // Returning an AggregateState::Scalar fills an array with the value.
        let mut result = UInt64Array::builder(1);
        result.append_value(count);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

#[derive(Debug)]
struct ModelCountAccumulator {
    count: u64,
}

impl Accumulator for ModelCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.count += array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);
        }
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(ScalarValue::UInt64(Some(self.count)))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }
}

//Min
#[derive(Debug)]
pub struct ModelMinPhysicalExpr {
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl Display for ModelMinPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelMinPhysicalExpr(Float32)")
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
        crate::downcast_arrays!(gids, start_times, end_times, mtids, models, gaps, batch);

        let mut min = f32::MAX;
        let num_rows = gids.len();
        for row_index in 0..num_rows {
            let gid = gids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let mtid = mtids.value(row_index);
            let sampling_interval = self
                .model_table_metadata
                .sampling_intervals
                .value(gid as usize);
            let model = models.value(row_index);
            let gaps = gaps.value(row_index);
            min = f32::min(
                min,
                models::min(
                    gid,
                    start_time,
                    end_time,
                    mtid,
                    sampling_interval,
                    model,
                    gaps,
                ),
            );
        }

        // Returning an AggregateState::Scalar fills an array with the value.
        let mut result = Float32Array::builder(1);
        result.append_value(min);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
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

    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(ScalarValue::Float32(Some(self.min)))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }
}

//Max
#[derive(Debug)]
pub struct ModelMaxPhysicalExpr {
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl Display for ModelMaxPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelMaxPhysicalExpr(Float32)")
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
        crate::downcast_arrays!(gids, start_times, end_times, mtids, models, gaps, batch);

        let mut max = f32::MIN;
        let num_rows = gids.len();
        for row_index in 0..num_rows {
            let gid = gids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let mtid = mtids.value(row_index);
            let sampling_interval = self
                .model_table_metadata
                .sampling_intervals
                .value(gid as usize);
            let model = models.value(row_index);
            let gaps = gaps.value(row_index);
            max = f32::max(
                max,
                models::max(
                    gid,
                    start_time,
                    end_time,
                    mtid,
                    sampling_interval,
                    model,
                    gaps,
                ),
            );
        }

        // Returning an AggregateState::Scalar fills an array with the value.
        let mut result = Float32Array::builder(1);
        result.append_value(max);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
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

    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(ScalarValue::Float32(Some(self.max)))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }
}

//Sum
#[derive(Debug)]
pub struct ModelSumPhysicalExpr {
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl Display for ModelSumPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelSumPhysicalExpr(Float32)")
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
        crate::downcast_arrays!(gids, start_times, end_times, mtids, models, gaps, batch);

        let mut sum = 0.0;
        let num_rows = gids.len();
        for row_index in 0..num_rows {
            let gid = gids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let mtid = mtids.value(row_index);
            let sampling_interval = self
                .model_table_metadata
                .sampling_intervals
                .value(gid as usize);
            let model = models.value(row_index);
            let gaps = gaps.value(row_index);
            sum += models::sum(
                gid,
                start_time,
                end_time,
                mtid,
                sampling_interval,
                model,
                gaps,
            );
        }

        // Returning an AggregateState::Scalar fills an array with the value.
        let mut result = Float32Array::builder(1);
        result.append_value(sum as f32);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

#[derive(Debug)]
struct ModelSumAccumulator {
    sum: f32,
}

impl Accumulator for ModelSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.sum += array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
        }
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(ScalarValue::Float32(Some(self.sum)))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }
}

//Avg
#[derive(Debug)]
pub struct ModelAvgPhysicalExpr {
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl Display for ModelAvgPhysicalExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ModelAvgPhysicalExpr(Float32)")
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
        crate::downcast_arrays!(gids, start_times, end_times, mtids, models, gaps, batch);

        let mut sum = 0.0;
        let mut count: u64 = 0;
        let num_rows = gids.len();
        for row_index in 0..num_rows {
            let gid = gids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let mtid = mtids.value(row_index);
            let sampling_interval = self
                .model_table_metadata
                .sampling_intervals
                .value(gid as usize);
            let model = models.value(row_index);
            let gaps = gaps.value(row_index);
            sum += models::sum(
                gid,
                start_time,
                end_time,
                mtid,
                sampling_interval,
                model,
                gaps,
            );
            count += (((end_time - start_time) / sampling_interval as i64) + 1) as u64;
        }

        // Returning an AggregateState::Scalar fills an array with the value.
        let mut result = Float32Array::builder(2);
        result.append_value(sum as f32);
        result.append_value(count as f32);
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
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
            self.sum += array.value(0) as f32;
            self.count += array.value(1) as u64;
        }
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<AggregateState>> {
        //Must match datafusion::physical_plan::expressions::AvgAccumulator
        Ok(vec![
            AggregateState::Scalar(ScalarValue::UInt64(Some(self.count))),
            AggregateState::Scalar(ScalarValue::Float32(Some(self.sum))),
        ])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }
}
