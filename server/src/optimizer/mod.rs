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
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_plan::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use tracing::debug;

pub struct LogOptimizerRule {}

impl OptimizerRule for LogOptimizerRule {
    fn optimize(
        &self,
        logical_plan: &LogicalPlan,
        _execution_props: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        debug!("Logical plan:\n{:#?}\n", &logical_plan);
        Ok(logical_plan.clone())
    }

    fn name(&self) -> &str {
        "log_optimizer_rule "
    }
}

pub struct LogQueryPlanner {}

#[async_trait]
impl QueryPlanner for LogQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Logical plan:\n{:#?}\n", &logical_plan);
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub struct LogPhysicalOptimizerRule {}

impl PhysicalOptimizerRule for LogPhysicalOptimizerRule {
    fn optimize(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Execution plan:\n{:#?}\n", &execution_plan);
        Ok(execution_plan.clone())
    }

    fn name(&self) -> &str {
        "log_physical_optimizer_rule"
    }
}
