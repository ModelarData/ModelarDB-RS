mod catalog;
mod engine;
mod models;
mod optimizer;
mod tables;

/** Public Methods **/
//TODO: drop order by node on time if on timestamp as it is already sorted, reversing can be done without comparison
//TODO: create ingestor that writes receives data over arrows, writes it to
// parquet, and ModelarDB-JVM compress in a separate process asynchronously
//TODO: support CREATE TABLE and CREATE MODEL TABLE, or have TS-VALUE columns?
fn main() {
    let mut args = std::env::args();
    args.next();
    if let Some(data_folder) = args.next() {
	let catalog = catalog::build_catalog(&data_folder);
	let mut ctx = engine::new(catalog);
	//TODO: receive queries from client instead of file and REPL
	if let Some(query_file) = args.next() {
	    engine::file(&mut ctx, &query_file);
	} else {
	    engine::repl(&mut ctx);
	}
    } else {
	//The errors are consciously ignored as the program is terminating
	let binary_path = std::env::current_exe().unwrap();
	let binary_name = binary_path.file_name().unwrap();
	println!("usage: {} data_folder [query_file]",
		 binary_name.to_str().unwrap());
    }
}
