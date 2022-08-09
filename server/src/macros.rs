/* Copyright 2022 The ModelarDB Contributors
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

#[macro_export]
macro_rules! downcast_arrays {
    ($gids:ident, $start_times:ident, $end_times:ident, $mtids:ident, $models:ident, $gaps:ident, $batch:ident) => {
        let $gids = $batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let $start_times = $batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let $end_times = $batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let $mtids = $batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let $models = $batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let $gaps = $batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
    };
}
