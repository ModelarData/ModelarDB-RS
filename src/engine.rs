use std::fs::File;
use std::io::{self, BufRead, Write};
use std::sync::Arc;

use datafusion::prelude::{ExecutionConfig, ExecutionContext};
use datafusion::arrow::util::pretty;

use crate::tables::DataPointView;
use crate::catalog::Catalog;

/** Public Functions **/
pub fn new(catalog: Catalog) -> ExecutionContext {

    let config = ExecutionConfig::new()
	.add_optimizer_rule(Arc::new(crate::optimizer::PrintOptimizerRule {}))
	.with_query_planner(Arc::new(crate::optimizer::PrintQueryPlanner {}))
	.add_physical_optimizer_rule(Arc::new(crate::optimizer::PrintPhysicalOptimizerRule {}));
    let mut ctx = ExecutionContext::with_config(config);

    //Initializes tables consisting of standard Apache Parquet files
    for table in catalog.tables {
	if ctx.register_parquet(&table.name, &table.path).is_err() {
	    eprintln!("ERROR: unable to initialize table {}", table.name);
	}
    }

    //Initializes tables storing time series as models in Apache Parquet files
    for table in catalog.model_tables {
	let name = table.name.clone();
	let table_provider = DataPointView::new(
	    catalog.cores, &Arc::new(table));
	if ctx.register_table(name.as_str(), table_provider).is_err() {
	    eprintln!("ERROR: unable to initialize model table {}", name);
	}
    }
    ctx
}

pub fn file(ctx: &mut ExecutionContext, queries_path: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let file = File::open(queries_path).unwrap();
    let lines = io::BufReader::new(file).lines();

    for query in lines {
	let df = ctx.sql(&query.unwrap());
	rt.block_on(async {
	    let results = df.unwrap().collect().await.unwrap();
	    pretty::print_batches(&results).unwrap();
	});
    }
}

pub fn repl(ctx: &mut ExecutionContext) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut input = String::new();
    while &input != "exit" {
	//Get user input
	input.clear();
	print!("ModelarDB> ");
	if let Err(error) = std::io::stdout().flush() {
	    eprintln!("ERROR: {} ", error);
	    return;
	}

	if let Err(error) = std::io::stdin().read_line(&mut input) {
	    eprintln!("ERROR: {} ", error);
	    return;
	}
	input.pop(); //Removes \n

	//Execute command or query
	if input.starts_with('\\') {
	    execute_command(ctx, &input);
	} else {
	    let df = ctx.sql(&input);
	    if let Err(error) = &df {
		eprintln!("ERROR: {} ", error);
	    } else {
		rt.block_on(async {
		    let results = df.unwrap().collect().await.unwrap();
		    pretty::print_batches(&results).unwrap();
		});
	    }
	}

    }
}

/** Private Functions **/
fn execute_command(ctx: &mut ExecutionContext, command_and_argument: &str) {
    let mut command_and_argument = command_and_argument.split(' ');
    match command_and_argument.next().unwrap() { //command_and_argument starts with \
	"\\dt" => {
	    //Lists the tables in the public schema
	    if let Some(catalog) = ctx.catalog("datafusion") { //default catalog
		if let Some(schema) = catalog.schema("public") {
		    println!("public");
		    for table_name in schema.table_names() {
			println!("  {}", table_name);
		    }
		} else {
		    eprintln!("ERROR: schema does not exist")
		}
	    } else {
		eprintln!("ERROR: catalog does not exist")
	    }
	},
	"\\d" => {
	    //Lists the schema of the table provided
	    if let Some(table_name) = command_and_argument.next() {
		if let Some(catalog) = ctx.catalog("datafusion") { //default catalog
		    if let Some(schema) = catalog.schema("public") {
			if let Some(table) = schema.table(table_name) {
			    println!("{}", table.schema());
			} else {
			    eprintln!("ERROR: schema does not exist")
			}
		    }
		} else {
		    eprintln!("ERROR: catalog does not exist")
		}
	    } else {
		eprintln!("a table name was not provided")
	    }
	},
	_ => eprintln!("unknown command")
    }
}
