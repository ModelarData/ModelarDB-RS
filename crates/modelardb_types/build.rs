use std::io::Result as StdIoResult;

fn main() -> StdIoResult<()> {
    prost_build::compile_protos(&["src/flight.proto"], &["src/"])?;
    Ok(())
}
