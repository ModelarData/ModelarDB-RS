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

//! Implementation of physical optimizer rules that rewrites the physical plans produced by Apache
//! Arrow DataFusion to execute queries more efficiently directly on the segments with metadata and
//! models stored in model tables.

mod model_simple_aggregates;

use std::sync::Arc;

use model_simple_aggregates::ModelSimpleAggregatesPhysicalOptimizerRule;

/// Return all physical optimizer rules. Currently, only simple aggregates are executed on segments.
pub fn physical_optimizer_rules() -> Vec<Arc<ModelSimpleAggregatesPhysicalOptimizerRule>> {
    vec![Arc::new(ModelSimpleAggregatesPhysicalOptimizerRule {})]
}

#[cfg(test)]
mod test_util {
    use std::any::TypeId;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::physical_plan::ExecutionPlan;

    use crate::metadata::test_util;
    use crate::query::ModelTable;

    /// Parse, plan, and optimize the `query` for execution on data in `path`.
    pub async fn query_optimized_physical_query_plan(
        path: &Path,
        query: &str,
    ) -> Arc<dyn ExecutionPlan> {
        let context = test_util::test_context(path).await;
        let model_table_metadata = test_util::model_table_metadata();

        context
            .session
            .register_table(
                "model_table",
                ModelTable::new(context.clone(), Arc::new(model_table_metadata)),
            )
            .unwrap();

        context
            .session
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// Assert that `physical_plan` and `expected_plan` contain the same operators. `expected_plan`
    /// only contains the type ids so the tests do not have to construct the actual operators.
    pub fn assert_eq_physical_plan_expected(
        physical_plan: Arc<dyn ExecutionPlan>,
        expected_plan: Vec<Vec<TypeId>>,
    ) {
        let mut level = 0;
        let mut current_execs = vec![physical_plan];
        let mut next_execs = vec![];

        while !current_execs.is_empty() {
            let expected_execs = &expected_plan[level];
            assert_eq!(current_execs.len(), expected_execs.len());

            for (current, expected) in current_execs.iter().zip(expected_execs) {
                assert_eq!(current.as_any().type_id(), *expected);
                next_execs.extend(current.children());
            }

            level += 1;
            current_execs = next_execs;
            next_execs = vec![];
        }
    }
}
