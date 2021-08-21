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
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::context::QueryPlanner;
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
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        println!("LOGICAL PLAN[START]\n {:#?} \nLOGICAL PLAN[END]", &plan);
        Ok(plan.clone())
    }

    fn name(&self) -> &str {
        "print_logical_query_plan"
    }
}

pub struct PrintQueryPlanner {}

impl QueryPlanner for PrintQueryPlanner {
    fn create_physical_plan(
        &self,
        plan: &datafusion::logical_plan::LogicalPlan,
        ctx_state: &datafusion::execution::context::ExecutionContextState,
    ) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        println!("QUERY PLANNER[START]\n {:#?} \nQUERY PLANNER[END]", &plan);
        let planner = DefaultPhysicalPlanner::default();
        planner.create_physical_plan(plan, ctx_state)
    }
}

pub struct PrintPhysicalOptimizerRule {}

impl PhysicalOptimizerRule for PrintPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("EXECUTION PLAN[START]\n {:#?} \nEXECUTION PLAN[END]", &plan);
        Ok(plan.clone())
    }

    fn name(&self) -> &str {
        "print_physical_query_plan"
    }
}
