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
pub mod model_simple_aggregates;

use std::sync::Arc;

use async_trait::async_trait;

use datafusion::error::Result;
use datafusion::execution::context::{ExecutionContextState, ExecutionProps, QueryPlanner};
use datafusion::logical_plan::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion::prelude::ExecutionConfig;

pub struct PrintOptimizerRule {}

impl OptimizerRule for PrintOptimizerRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        println!("LOGICAL PLAN[START]\n {:#?} \nLOGICAL PLAN[END]", &plan);
        Ok(plan.clone())
    }

    fn name(&self) -> &str {
        "print_logical_query_plan"
    }
}

pub struct PrintQueryPlanner {}

#[async_trait]
impl QueryPlanner for PrintQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!(
            "QUERY PLANNER[START]\n {:#?} \nQUERY PLANNER[END]",
            &logical_plan
        );
        let planner = DefaultPhysicalPlanner::default();
        planner.create_physical_plan(logical_plan, ctx_state).await
    }
}

pub struct PrintPhysicalOptimizerRule {}

impl PhysicalOptimizerRule for PrintPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("EXECUTION PLAN[START]\n {:#?} \nEXECUTION PLAN[END]", &plan);
        Ok(plan.clone())
    }

    fn name(&self) -> &str {
        "print_physical_query_plan"
    }
}
