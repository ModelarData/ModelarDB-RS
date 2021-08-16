mod catalog;
mod engine;
mod models;
mod optimizer;
mod remote;
mod tables;

use datafusion::prelude::ExecutionContext;
use std::sync::Arc;
use tokio::runtime::Runtime;

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
        let execution = engine::new(&catalog);
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
        println!(
            "usage: {} data_folder [query_file]",
            binary_name.to_str().unwrap()
        );
    }
}
