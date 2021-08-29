/* Copyright 2021 The MiniModelarDB Contributors
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

use crate::catalog::ModelTable;
use crate::models;
use crate::tables::GridExec;

use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::error::Result;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::expressions::format_state_name;
use datafusion::physical_plan::expressions::Count;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::prelude::ExecutionConfig;
use datafusion::scalar::ScalarValue;

//Optimizer Rule
pub struct ModelSimpleAggregatesPhysicalOptimizerRule {}

impl ModelSimpleAggregatesPhysicalOptimizerRule {
    fn optimize(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        //Matches a simple aggregate performed without filtering out segments
        if let Some(hae) = plan.as_any().downcast_ref::<HashAggregateExec>() {
            let children = &hae.children();
            if children.len() == 1 {
                let aggr_expr = hae.aggr_expr(); //TODO: support the simple aggregate functions
                if aggr_expr.len() == 1 && aggr_expr[0].as_any().downcast_ref::<Count>().is_some() {
                    if let Some(ge) = children[0].as_any().downcast_ref::<GridExec>() {
                        let mce = Arc::new(ModelCountPhysicalExpr {
                            model_table: ge.model_table.clone(),
                        });
                        let mca = Arc::new(ModelCountAggregateExpr::new(mce));
                        let hae = Arc::new(
                            HashAggregateExec::try_new(
                                *hae.mode(),
                                hae.group_expr().to_vec(),
                                vec![mca],
                                ge.children()[0].clone(), //Removes the GridExec
                                hae.input_schema(),
                            )
                            .unwrap(),
                        );
                        return Some(hae);
                    }
                }
            }
        }

        //Visit the children
        //TODO: handle plans were multiple children must be updated
        for child in plan.children() {
            if let Some(new_child) = self.optimize(&child) {
                return Some(plan.with_new_children(vec![new_child]).unwrap());
            }
        }
        None
    }
}

impl PhysicalOptimizerRule for ModelSimpleAggregatesPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(optimized_plan) = self.optimize(&plan) {
            Ok(optimized_plan)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "model_count_physical_query_plan"
    }
}

//Aggregte Expression
#[derive(Debug)]
pub struct ModelCountAggregateExpr {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl ModelCountAggregateExpr {
    fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            name: "ModelCountAggregateExpr".to_string(),
            data_type: DataType::UInt64,
            nullable: false,
            expr,
        }
    }
}

impl AggregateExpr for ModelCountAggregateExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "count"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ModelCountAccumulator { count: 0 }))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub struct ModelCountPhysicalExpr {
    model_table: Arc<ModelTable>,
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
        let count = models::length(
            gids.len(),
            gids,
            start_times,
            end_times,
            &self.model_table.sampling_intervals,
        ) as u64;

        //If a ScalarValue is returned an array is filled with the value it contains
        let mut result = UInt64Array::builder(1);
        result.append_value(count).unwrap();
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

#[derive(Debug)]
struct ModelCountAccumulator {
    count: u64,
}

impl Accumulator for ModelCountAccumulator {
    fn update(&mut self, _values: &[ScalarValue]) -> Result<()> {
        unreachable!()
    }

    fn merge(&mut self, _states: &[ScalarValue]) -> Result<()> {
        unreachable!()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for values in values {
            self.count += values
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

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.count))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.count)))
    }
}
