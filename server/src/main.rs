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
mod catalog;
mod engine;
mod macros;
mod models;
mod optimizer;
mod remote;
mod tables;

use datafusion::prelude::ExecutionContext;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/** Public Types **/
pub struct Context {
    catalog: catalog::Catalog,
    runtime: Runtime,
    execution: ExecutionContext,
}

/** Public Function **/
fn main() {
    let mut args = std::env::args();
    args.next(); //Skip executable
    if let Some(data_folder) = args.next() {
        //Build context
        let catalog = catalog::new(&data_folder);
        let runtime = Runtime::new().unwrap();
        let execution = runtime.block_on(engine::new(&catalog));
        let context = Arc::new(Context {
            catalog,
            runtime,
            execution,
        });

        //Start interface
        remote::start_arrow_flight_server(context, 9999);
    } else {
        //The errors are consciously ignored as the program is terminating
        let binary_path = std::env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap();
        println!("usage: {} data_folder", binary_name.to_str().unwrap());
    }
}
