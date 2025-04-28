use std::io::Result as StdIoResult;

fn main() -> StdIoResult<()> {
    prost_build::compile_protos(&["src/flight/protocol.proto"], &["src/"])?;
    Ok(())
}
